"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。"""
    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 画像轮次与压实候选 -----------------------------

@dataclass
class ProfileEpoch:
    """一轮统计画像：按分区聚合行数，并冻结该轮可回查的文件范围。

    压实资格判定只引用某一轮画像冻结的范围与合计，
    避免压实反复扫描分区文件，也避免拿已经失效的画像做决定。
    """
    epoch_id: str
    lake_table: str
    total_rows: int
    partition_rows: dict[str, int]          # 分区 -> 行数（按分区聚合）
    partition_files: dict[str, list[str]]   # 分区 -> 该轮文件范围（可回查）
    created_at: float


@dataclass
class CompactCandidate:
    """压实候选：交付给压实作业的可核对输入范围。

    生命周期：待接单 -> 已接单 -> 已完成
                            -> 已撤回（接单后合计变化，重算不达门槛）
                   待接单 -> 已失效（画像更新，候选范围被覆盖）
    """
    candidate_id: str
    lake_table: str
    partition_key: str
    epoch_id: str                # 生成候选那一轮的画像
    file_ids: list[str]          # 生成那一轮的可回查文件范围
    partition_rows: int
    total_rows: int
    status: str                  # 待接单 | 已接单 | 已完成 | 已失效 | 已撤回
    created_at: float
    # 接单那一轮冻结的范围：值班员可按此复算执行资格
    claim_epoch_id: str | None = None
    claim_file_ids: list[str] = field(default_factory=list)
    claim_partition_rows: int = 0
    claim_total_rows: int = 0
    claimed_at: float | None = None
    # 失效/撤回原因与覆盖它的新画像
    invalid_reason: str | None = None
    superseded_by: str | None = None
    result_id: str | None = None


@dataclass
class CompactResult:
    """压实结果：同一候选重复提交只保留一份；boundary 供下游发布对齐。"""
    result_id: str
    candidate_id: str
    lake_table: str
    partition_key: str
    input_file_ids: list[str]
    output_file_id: str
    output_rows: int
    boundary_snapshot_id: str    # 已确认的压实边界（快照）
    confirmed_at: float


@dataclass
class CandidateOverwrite:
    """候选覆盖变化：画像更新或合计变化导致候选失效/撤回，值班员可见并按新画像派发。"""
    lake_table: str
    partition_key: str
    candidate_id: str
    old_epoch_id: str
    new_epoch_id: str
    old_share: str               # 旧占比，如 "6/10"
    new_share: str               # 新占比，如 "6/15"
    reason: str
    changed_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 画像轮次：lake_table -> list[ProfileEpoch]（按发布序）
        self._profile_epochs: dict[str, list[ProfileEpoch]] = {}

        # 压实候选：candidate_id -> CompactCandidate
        self._candidates: dict[str, CompactCandidate] = {}

        # 压实结果（幂等）：candidate_id -> CompactResult
        self._compact_results: dict[str, CompactResult] = {}

        # 候选覆盖变化流水：值班员可见
        self._candidate_overwrites: list[CandidateOverwrite] = []

    # ---- 文件操作 ----

    def register_file(
        self, lake_table: str, partition_key: str, rows: int, bytes_: int, manifest: str | None = None
    ) -> PartitionFile:
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
        )
        self._files.setdefault(lake_table, {})[file_id] = pf
        return pf

    def list_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = list(self._files.get(lake_table, {}).values())
        if partition_key is not None:
            files = [f for f in files if f.partition_key == partition_key]
        return files

    def get_file(self, lake_table: str, file_id: str) -> PartitionFile | None:
        return self._files.get(lake_table, {}).get(file_id)

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        table_files = self._files.get(lake_table, {})
        removed = 0
        for fid in file_ids:
            if fid in table_files:
                del table_files[fid]
                removed += 1
        return removed

    def replace_file_list(
        self, lake_table: str, old_file_ids: list[str], new_files: list[PartitionFile]
    ) -> None:
        """原子替换：先注册新文件，再删除旧文件。保证归并不留半份清单。"""
        table_files = self._files.setdefault(lake_table, {})
        for nf in new_files:
            table_files[nf.file_id] = nf
        for fid in old_file_ids:
            table_files.pop(fid, None)

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(self, merge_id: str, partial_file_ids: list[str]) -> None:
        self._merge_checkpoints[merge_id] = {
            "partial_file_ids": list(partial_file_ids),
            "saved_at": time.time(),
        }

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        return self._merge_checkpoints.get(merge_id)

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        self._merge_checkpoints.pop(merge_id, None)

    # ---- 画像轮次 ----

    def append_profile_epoch(
        self,
        lake_table: str,
        partition_rows: dict[str, int],
        partition_files: dict[str, list[str]],
    ) -> ProfileEpoch:
        """发布一轮画像：冻结分区行数与文件范围，后续判定引用同一轮。"""
        chain = self._profile_epochs.setdefault(lake_table, [])
        epoch = ProfileEpoch(
            epoch_id=f"epoch-{lake_table}-{len(chain) + 1}",
            lake_table=lake_table,
            total_rows=sum(partition_rows.values()),
            partition_rows=dict(partition_rows),
            partition_files={pk: list(ids) for pk, ids in partition_files.items()},
            created_at=time.time(),
        )
        chain.append(epoch)
        return epoch

    def latest_profile_epoch(self, lake_table: str) -> ProfileEpoch | None:
        chain = self._profile_epochs.get(lake_table, [])
        return chain[-1] if chain else None

    def get_profile_epoch(self, lake_table: str, epoch_id: str) -> ProfileEpoch | None:
        for epoch in self._profile_epochs.get(lake_table, []):
            if epoch.epoch_id == epoch_id:
                return epoch
        return None

    # ---- 压实候选 ----

    def add_compact_candidate(
        self,
        lake_table: str,
        partition_key: str,
        epoch_id: str,
        file_ids: list[str],
        partition_rows: int,
        total_rows: int,
    ) -> CompactCandidate:
        candidate_id = f"cand-{lake_table}-{partition_key}-{uuid.uuid4().hex[:6]}"
        cand = CompactCandidate(
            candidate_id=candidate_id,
            lake_table=lake_table,
            partition_key=partition_key,
            epoch_id=epoch_id,
            file_ids=list(file_ids),
            partition_rows=partition_rows,
            total_rows=total_rows,
            status="待接单",
            created_at=time.time(),
        )
        self._candidates[candidate_id] = cand
        return cand

    def get_candidate(self, candidate_id: str) -> CompactCandidate | None:
        return self._candidates.get(candidate_id)

    def list_candidates(
        self, lake_table: str | None = None, status: str | None = None
    ) -> list[CompactCandidate]:
        cands = list(self._candidates.values())
        if lake_table is not None:
            cands = [c for c in cands if c.lake_table == lake_table]
        if status is not None:
            cands = [c for c in cands if c.status == status]
        return sorted(cands, key=lambda c: c.created_at)

    def record_candidate_overwrite(self, overwrite: CandidateOverwrite) -> None:
        self._candidate_overwrites.append(overwrite)

    def list_candidate_overwrites(self, lake_table: str | None = None) -> list[CandidateOverwrite]:
        if lake_table is None:
            return list(self._candidate_overwrites)
        return [o for o in self._candidate_overwrites if o.lake_table == lake_table]

    # ---- 压实结果 ----

    def save_compact_result(self, result: CompactResult) -> None:
        """按候选幂等保存：同一候选重复提交只保留第一份结果。"""
        self._compact_results.setdefault(result.candidate_id, result)

    def get_compact_result(self, candidate_id: str) -> CompactResult | None:
        return self._compact_results.get(candidate_id)

    def latest_compact_boundary(self, lake_table: str) -> CompactResult | None:
        """最近已确认的压实边界：下游发布结果据此标明生效位置。"""
        results = [r for r in self._compact_results.values() if r.lake_table == lake_table]
        if not results:
            return None
        return max(results, key=lambda r: r.confirmed_at)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()
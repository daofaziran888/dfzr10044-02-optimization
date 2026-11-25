"""入湖批次领域服务：批次入库、去重、快照绑定与校验值对比。

分区批次发布（publish_partition_batch）与压实确认共用表级写锁：
同一批次重复发布只入湖一次；发布完成刷新画像轮次，
并在结果中标明本次发布在哪个已确认的压实边界之后生效。
"""
from __future__ import annotations

import hashlib
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.stats_profile import StatsProfileService
from app.store import store
from app.lake_store import lake_store


class IngestBatchService(BaseModuleService):
    MODULE = "ingest_batch"
    ENTITY = "入湖批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "所属湖表", "来源系统", "批次行数", "采集窗口", "落湖快照", "完整校验值", "入湖时间"]
    REQUIRED_FIELDS = ["批次编号", "所属湖表", "来源系统"]
    STATUS_ORDER = ["待入湖", "入湖中", "已入湖", "入湖失败"]
    ACTION_RULES = {"启动入湖": "入湖中", "确认入湖": "已入湖", "标记失败": "入湖失败"}
    NEGATIVE_ACTIONS = []

    def ingest_batch(self, lake_table: str, source_system: str,
                     rows: list[dict[str, Any]], window_start: str,
                     window_end: str) -> dict[str, Any]:
        """接收入湖批次数据：去重、存储、并与快照绑定。"""
        batch_id = f"batch-{lake_table}-{uuid.uuid4().hex[:6]}"
        # 先去重再存储
        seen = set()
        deduped: list[dict[str, Any]] = []
        for r in rows:
            key = hashlib.md5(str(sorted(r.items())).encode()).hexdigest()
            if key not in seen:
                seen.add(key)
                deduped.append(r)
        br = lake_store.store_batch(batch_id, lake_table, source_system,
                                    deduped, window_start, window_end)
        lake_store.update_batch_status(batch_id, "ingesting")
        return {
            "batch_id": batch_id,
            "lake_table": lake_table,
            "row_count": len(deduped),
            "duplicates_removed": len(rows) - len(deduped),
            "checksum": br.checksum,
            "window": f"{window_start} ~ {window_end}",
        }

    def verify_batch_integrity(self, batch_id: str) -> dict[str, Any]:
        """校验批次完整性：重新计算校验值并对比。"""
        br = lake_store.get_batch(batch_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        recalc = hashlib.md5(
            str(sorted([str(r) for r in br.rows])).encode()
        ).hexdigest()
        return {
            "batch_id": batch_id,
            "stored_checksum": br.checksum,
            "recalculated_checksum": recalc,
            "integrity_ok": recalc == br.checksum,
        }

    def promote_to_ingested(self, batch_id: str, snapshot_id: str) -> dict[str, Any]:
        """操作已入湖：绑定快照并标记已入湖。"""
        br = lake_store.update_batch_status(batch_id, "ingested", snapshot_id)
        if br is None:
            return {"success": False, "error": "batch not found"}
        return {
            "batch_id": batch_id,
            "status": "ingested",
            "snapshot_id": snapshot_id,
        }

    def publish_partition_batch(
        self,
        lake_table: str,
        partition_key: str,
        batch_id: str,
        rows: list[dict[str, Any]],
        source_system: str = "LATE_BATCH",
    ) -> dict[str, Any]:
        """发布一个分区批次：幂等入湖、落快照、刷新画像轮次。

        - 同一 batch_id 重复发布只入湖一次（迟到批次补批与压实提交冲突重试安全）；
        - 发布与压实确认共用表级写锁，冲突在锁内串行化；
        - 结果标明本次发布在哪个已确认的压实边界之后生效。
        """
        lake_store.acquire_write_lock(lake_table)
        try:
            br = lake_store.get_batch(batch_id)
            if br is not None and br.status == "ingested":
                # 幂等：批次已入湖，本次不再重复登记文件
                epoch = lake_store.latest_profile_epoch(lake_table)
                boundary = lake_store.latest_compact_boundary(lake_table)
                return {
                    "success": True,
                    "idempotent": True,
                    "batch_id": batch_id,
                    "applied_rows": 0,
                    "epoch_id": epoch.epoch_id if epoch else None,
                    "effective_after_boundary": boundary.result_id if boundary else None,
                    "boundary_snapshot_id": boundary.boundary_snapshot_id if boundary else None,
                }
            if br is None:
                lake_store.store_batch(
                    batch_id, lake_table, source_system, rows,
                    window_start="2026-11-01T00:00:00", window_end="2026-11-01T23:59:59",
                )
            lake_store.update_batch_status(batch_id, "ingesting")
            pf = lake_store.register_file(
                lake_table, partition_key, rows=len(rows), bytes_=max(1, len(rows)) * 256,
            )
            all_file_ids = [f.file_id for f in lake_store.list_files(lake_table)]
            snaps = lake_store.list_snapshots(lake_table)
            available = [s for s in snaps if s.status == "available"]
            snap = lake_store.create_snapshot(
                lake_table, all_file_ids,
                parent_snapshot=available[-1].snapshot_id if available else None,
                added_rows=len(rows), deleted_rows=0,
            )
            lake_store.mark_snapshot_available(lake_table, snap.snapshot_id)
            lake_store.update_batch_status(batch_id, "ingested", snap.snapshot_id)
            boundary = lake_store.latest_compact_boundary(lake_table)
            # 发布即刷新画像：候选范围变更在锁内生效，压实确认能立即看到
            profile = StatsProfileService().publish_profile_epoch(lake_table)
            return {
                "success": True,
                "idempotent": False,
                "batch_id": batch_id,
                "applied_rows": len(rows),
                "file_id": pf.file_id,
                "snapshot_id": snap.snapshot_id,
                "epoch_id": profile["epoch_id"],
                "table_total_rows": profile["total_rows"],
                "effective_after_boundary": boundary.result_id if boundary else None,
                "boundary_snapshot_id": boundary.boundary_snapshot_id if boundary else None,
            }
        finally:
            lake_store.release_write_lock(lake_table)
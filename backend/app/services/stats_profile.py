"""统计画像领域服务：采样汇总湖表行数、字节与分区倾斜度，供分区规划和压实触发共同引用。

画像发布（publish_profile_epoch）把"按分区聚合的文件行数"冻结成一轮可回查的画像，
并向压实作业交付能核对输入范围的压实候选；候选范围变更后旧候选即失效，
覆盖变化对值班员可见，避免压实反复扫描分区文件、拿失效画像做决定。
"""
from __future__ import annotations

import time
from typing import Any

from app.services.base import BaseModuleService
from app.store import store
from app.lake_store import lake_store, CandidateOverwrite, CompactCandidate


def is_partition_skewed(partition_rows: int, total_rows: int) -> bool:
    """倾斜判定：分区行数 / 全表行数 严格超过一半才算倾斜；空表不倾斜。"""
    return total_rows > 0 and partition_rows * 2 > total_rows


def candidate_to_dict(cand: CompactCandidate) -> dict[str, Any]:
    """候选的可回查视图：生成轮次与接单轮次的文件范围、占比都可见。"""
    return {
        "candidate_id": cand.candidate_id,
        "lake_table": cand.lake_table,
        "partition_key": cand.partition_key,
        "epoch_id": cand.epoch_id,
        "file_ids": list(cand.file_ids),
        "partition_rows": cand.partition_rows,
        "total_rows": cand.total_rows,
        "share": f"{cand.partition_rows}/{cand.total_rows}",
        "status": cand.status,
        "claim_epoch_id": cand.claim_epoch_id,
        "claim_file_ids": list(cand.claim_file_ids),
        "claim_share": (
            f"{cand.claim_partition_rows}/{cand.claim_total_rows}"
            if cand.claim_epoch_id else None
        ),
        "invalid_reason": cand.invalid_reason,
        "superseded_by": cand.superseded_by,
        "result_id": cand.result_id,
    }


class StatsProfileService(BaseModuleService):
    MODULE = "stats_profile"
    ENTITY = "统计画像"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["画像编号", "所属湖表", "统计粒度", "行数估算", "字节估算", "分区倾斜度", "采样比例", "统计时间"]
    REQUIRED_FIELDS = ["画像编号", "所属湖表", "统计粒度"]
    STATUS_ORDER = ["采样中", "已生成", "已过期", "生成失败"]
    ACTION_RULES = {"启动采样": "已生成", "确认生成": "已过期", "标记过期": "生成失败"}
    NEGATIVE_ACTIONS = []

    def sample_table_stats(self, lake_table: str, granularity: str = "TABLE") -> dict[str, Any]:
        """按粒度采样湖表统计信息。"""
        files = lake_store.list_files(lake_table)
        snaps = lake_store.list_snapshots(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)

        # 按分区计数找到最大分区占比
        partition_rows: dict[str, int] = {}
        for f in files:
            partition_rows[f.partition_key] = partition_rows.get(f.partition_key, 0) + f.file_rows
        max_partition_share = (max(partition_rows.values()) / total_rows) if total_rows else 0

        return {
            "lake_table": lake_table,
            "granularity": granularity,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": len(partition_rows),
            "max_partition_skew": round(max_partition_share, 4),
            "available_snapshots": sum(1 for s in snaps if s.status == "available"),
            "sample_time": time.time(),
        }

    def profile_skew_distribution(self, lake_table: str) -> list[dict[str, Any]]:
        """按分区输出倾斜度分布。"""
        files = lake_store.list_files(lake_table)
        partition_rows: dict[str, int] = {}
        partition_bytes: dict[str, int] = {}
        for f in files:
            partition_rows[f.partition_key] = partition_rows.get(f.partition_key, 0) + f.file_rows
            partition_bytes[f.partition_key] = partition_bytes.get(f.partition_key, 0) + f.file_bytes
        total_rows = sum(partition_rows.values()) or 1
        return [
            {
                "partition": pk,
                "rows": partition_rows[pk],
                "bytes": partition_bytes[pk],
                "skew_ratio": round(partition_rows[pk] / total_rows, 4),
            }
            for pk in sorted(partition_rows.keys())
        ]

    def publish_profile_epoch(self, lake_table: str) -> dict[str, Any]:
        """按分区聚合文件行数，发布一轮画像并交付压实候选。

        - 每轮画像冻结一份可回查的文件范围（分区 -> file_ids），后续判定不再重扫文件；
        - 严格超过一半（分区行数 * 2 > 全表行数）才交付候选，空表没有可执行候选；
        - 上一轮仍未接单的候选随画像更新失效，覆盖变化记录供值班员查看并按新画像派发；
        - 已接单的候选不在此处理：由压实确认时按最新画像重算资格。
        """
        files = lake_store.list_files(lake_table)
        partition_rows: dict[str, int] = {}
        partition_files: dict[str, list[str]] = {}
        for f in files:
            partition_rows[f.partition_key] = partition_rows.get(f.partition_key, 0) + f.file_rows
            partition_files.setdefault(f.partition_key, []).append(f.file_id)
        total_rows = sum(partition_rows.values())
        epoch = lake_store.append_profile_epoch(lake_table, partition_rows, partition_files)

        # 候选范围变更 → 重新判定：上一轮未接单候选失效，覆盖变化值班员可见
        invalidated: list[CompactCandidate] = []
        overwrites: list[CandidateOverwrite] = []
        for cand in lake_store.list_candidates(lake_table, status="待接单"):
            if cand.epoch_id == epoch.epoch_id:
                continue
            new_rows = partition_rows.get(cand.partition_key, 0)
            if total_rows <= 0:
                reason = "空表无可执行候选"
            elif is_partition_skewed(new_rows, total_rows):
                reason = "候选范围变更"
            else:
                reason = "合计变化不再倾斜"
            cand.status = "已失效"
            cand.invalid_reason = reason
            cand.superseded_by = epoch.epoch_id
            overwrite = CandidateOverwrite(
                lake_table=lake_table,
                partition_key=cand.partition_key,
                candidate_id=cand.candidate_id,
                old_epoch_id=cand.epoch_id,
                new_epoch_id=epoch.epoch_id,
                old_share=f"{cand.partition_rows}/{cand.total_rows}",
                new_share=f"{new_rows}/{total_rows}",
                reason=reason,
                changed_at=time.time(),
            )
            lake_store.record_candidate_overwrite(overwrite)
            invalidated.append(cand)
            overwrites.append(overwrite)

        # 交付本轮候选：严格超过一半才倾斜，空表没有可执行候选
        delivered: list[CompactCandidate] = []
        if total_rows > 0:
            for pk in sorted(partition_rows.keys()):
                rows = partition_rows[pk]
                if is_partition_skewed(rows, total_rows):
                    delivered.append(lake_store.add_compact_candidate(
                        lake_table, pk, epoch.epoch_id,
                        file_ids=partition_files.get(pk, []),
                        partition_rows=rows,
                        total_rows=total_rows,
                    ))

        return {
            "lake_table": lake_table,
            "epoch_id": epoch.epoch_id,
            "total_rows": total_rows,
            "empty_table": total_rows == 0,
            "partition_rows": dict(partition_rows),
            "partition_files": {pk: list(ids) for pk, ids in partition_files.items()},
            "delivered_candidates": [candidate_to_dict(c) for c in delivered],
            "invalidated_candidates": [candidate_to_dict(c) for c in invalidated],
            "overwrites": [
                {
                    "candidate_id": o.candidate_id,
                    "partition_key": o.partition_key,
                    "old_share": o.old_share,
                    "new_share": o.new_share,
                    "reason": o.reason,
                }
                for o in overwrites
            ],
        }
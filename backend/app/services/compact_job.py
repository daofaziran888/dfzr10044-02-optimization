"""压实作业领域服务：基于触发条件的湖表分区压实，保留快照可读性。

压实资格走"统计画像 -> 压实候选"通道：候选交付时冻结可回查的文件范围，
接单与确认都引用画像轮次的冻结范围做判定，不再反复扫描分区文件；
接单后若合计变化，确认时按最新画像重算，不达门槛即撤回并对值班员可见。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.services.stats_profile import is_partition_skewed, candidate_to_dict
from app.store import store
from app.lake_store import lake_store, CompactResult, CandidateOverwrite


class CompactJobService(BaseModuleService):
    MODULE = "compact_job"
    ENTITY = "压实作业"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["压实编号", "所属湖表", "触发条件", "目标分区", "读取文件数", "输出文件数", "占用资源额度", "压实耗时"]
    REQUIRED_FIELDS = ["压实编号", "所属湖表", "触发条件"]
    STATUS_ORDER = ["待触发", "压实中", "已完成", "压实中断"]
    ACTION_RULES = {"触发压实": "压实中", "确认完成": "已完成", "标记中断": "压实中断"}
    NEGATIVE_ACTIONS = []

    def evaluate_compact_trigger(self, lake_table: str) -> dict[str, Any]:
        """评估压实触发条件：按文件数量与总字节数判定。"""
        files = lake_store.list_files(lake_table)
        total_files = len(files)
        total_bytes = sum(f.file_bytes for f in files)
        small_files = [f for f in files if f.file_bytes < 3000]
        triggers = {
            "文件过多": total_files > 20,
            "碎片文件超过阈值": len(small_files) > 5,
            "单分区文件密度高": any(
                sum(1 for ff in files if ff.partition_key == f.partition_key) > 8
                for f in files
            ),
        }
        return {
            "total_files": total_files,
            "total_bytes": total_bytes,
            "small_file_count": len(small_files),
            "trigger_conditions": triggers,
            "should_trigger": any(triggers.values()),
        }

    def execute_compact(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        """执行压实：读取指定分区的全部文件，合并为一个输出文件。"""
        start_time = time.time()
        files = lake_store.list_files(lake_table, partition_key)
        read_count = len(files)
        total_rows = sum(f.file_rows for f in files)
        # 压缩后体积降低
        compact_pf = lake_store.register_file(
            lake_table, partition_key,
            rows=total_rows, bytes_=max(1, sum(f.file_bytes for f in files) // 2),
        )
        # 原子替换
        old_ids = [f.file_id for f in files]
        lake_store.replace_file_list(lake_table, old_ids, [compact_pf])
        elapsed = time.time() - start_time
        return {
            "lake_table": lake_table,
            "partition": partition_key,
            "read_files": read_count,
            "output_files": 1,
            "total_rows": total_rows,
            "elapsed": elapsed,
            "compact_success": True,
        }

    # ---- 候选接单 / 确认 / 复算 ----

    def list_candidates(self, lake_table: str, status: str | None = None) -> list[dict[str, Any]]:
        """列出画像交付的压实候选（含可核对输入范围）。"""
        return [
            candidate_to_dict(c)
            for c in lake_store.list_candidates(lake_table, status=status)
        ]

    def claim_candidate(self, candidate_id: str) -> dict[str, Any]:
        """接单：按最新一轮画像重新判定资格，合格则锁定接单这一轮的文件范围。

        判定只引用画像冻结的分区行数与文件范围，不重扫分区文件；
        候选范围已被新画像覆盖（已失效）或合计变化后不再倾斜的，不能接单。
        """
        cand = lake_store.get_candidate(candidate_id)
        if cand is None:
            return {"success": False, "reason": f"候选 {candidate_id} 不存在"}
        if cand.status != "待接单":
            return {
                "success": False,
                "candidate_id": candidate_id,
                "status": cand.status,
                "reason": f"候选状态为「{cand.status}」，不能接单",
                "invalid_reason": cand.invalid_reason,
            }
        epoch = lake_store.latest_profile_epoch(cand.lake_table)
        if epoch is None or epoch.total_rows <= 0:
            cand.status = "已失效"
            cand.invalid_reason = "空表无可执行候选"
            return {
                "success": False,
                "candidate_id": candidate_id,
                "status": cand.status,
                "reason": "空表无可执行候选",
            }
        part_rows = epoch.partition_rows.get(cand.partition_key, 0)
        share = f"{part_rows}/{epoch.total_rows}"
        if cand.epoch_id != epoch.epoch_id or not is_partition_skewed(part_rows, epoch.total_rows):
            # 画像已更新或重算不达门槛：旧候选失去执行资格
            cand.status = "已失效"
            cand.invalid_reason = "合计变化不再倾斜"
            cand.superseded_by = epoch.epoch_id
            return {
                "success": False,
                "candidate_id": candidate_id,
                "status": cand.status,
                "reason": "合计变化不再倾斜",
                "share": share,
            }
        # 合格：锁定接单这一轮的可回查文件范围
        cand.status = "已接单"
        cand.claim_epoch_id = epoch.epoch_id
        cand.claim_file_ids = list(epoch.partition_files.get(cand.partition_key, []))
        cand.claim_partition_rows = part_rows
        cand.claim_total_rows = epoch.total_rows
        cand.claimed_at = time.time()
        return {
            "success": True,
            "candidate_id": candidate_id,
            "status": cand.status,
            "claim_epoch_id": cand.claim_epoch_id,
            "claim_file_ids": list(cand.claim_file_ids),
            "claim_share": share,
        }

    def confirm_candidate(self, candidate_id: str) -> dict[str, Any]:
        """确认压实：乐观提交，与并发发布冲突时按最新画像重算资格。

        - 同一候选重复提交只保留一份压实结果（幂等）；
        - 接单后若合计变化（如迟到批次先提交），按最新画像重算：
          不再严格过半则撤回并告知值班员，仍达门槛则继续提交；
        - 提交成功产生已确认的压实边界，供下游发布对齐生效位置。
        """
        cand = lake_store.get_candidate(candidate_id)
        if cand is None:
            return {"success": False, "reason": f"候选 {candidate_id} 不存在"}
        # 幂等：已有结果直接返回，不重复压实
        existing = lake_store.get_compact_result(candidate_id)
        if existing is not None:
            return {
                "success": True,
                "outcome": "已完成",
                "idempotent": True,
                "candidate_id": candidate_id,
                "result_id": existing.result_id,
                "boundary_snapshot_id": existing.boundary_snapshot_id,
            }
        if cand.status != "已接单":
            return {
                "success": False,
                "candidate_id": candidate_id,
                "status": cand.status,
                "reason": f"候选状态为「{cand.status}」，不能确认",
            }

        lk = cand.lake_table
        lake_store.acquire_write_lock(lk)
        try:
            # 锁内再查幂等，防止并发双份结果
            existing = lake_store.get_compact_result(candidate_id)
            if existing is not None:
                return {
                    "success": True,
                    "outcome": "已完成",
                    "idempotent": True,
                    "candidate_id": candidate_id,
                    "result_id": existing.result_id,
                    "boundary_snapshot_id": existing.boundary_snapshot_id,
                }
            epoch = lake_store.latest_profile_epoch(lk)
            conflict_seen = epoch is not None and epoch.epoch_id != cand.claim_epoch_id
            recomputed_share: str | None = None
            if conflict_seen and epoch is not None:
                # 接单后画像被并发发布覆盖：用最新画像的冻结合计重算资格
                part_rows = epoch.partition_rows.get(cand.partition_key, 0)
                recomputed_share = f"{part_rows}/{epoch.total_rows}"
                if not is_partition_skewed(part_rows, epoch.total_rows):
                    cand.status = "已撤回"
                    cand.invalid_reason = "合计变化不再倾斜"
                    cand.superseded_by = epoch.epoch_id
                    lake_store.record_candidate_overwrite(CandidateOverwrite(
                        lake_table=lk,
                        partition_key=cand.partition_key,
                        candidate_id=cand.candidate_id,
                        old_epoch_id=cand.claim_epoch_id or cand.epoch_id,
                        new_epoch_id=epoch.epoch_id,
                        old_share=f"{cand.claim_partition_rows}/{cand.claim_total_rows}",
                        new_share=recomputed_share,
                        reason="合计变化不再倾斜",
                        changed_at=time.time(),
                    ))
                    return {
                        "success": False,
                        "outcome": "已撤回",
                        "candidate_id": candidate_id,
                        "reason": "合计变化不再倾斜",
                        "old_share": f"{cand.claim_partition_rows}/{cand.claim_total_rows}",
                        "new_share": recomputed_share,
                    }
            # 核对输入范围：接单那一轮冻结的文件必须仍全部在册
            missing = [
                fid for fid in cand.claim_file_ids
                if lake_store.get_file(lk, fid) is None
            ]
            if missing:
                cand.status = "已撤回"
                cand.invalid_reason = "输入范围已变化"
                return {
                    "success": False,
                    "outcome": "已撤回",
                    "candidate_id": candidate_id,
                    "reason": "输入范围已变化",
                    "missing_files": missing,
                }
            # 执行压实：接单范围合并为一个输出文件，原子替换
            input_files = [lake_store.get_file(lk, fid) for fid in cand.claim_file_ids]
            output_rows = sum(f.file_rows for f in input_files if f is not None)
            output_bytes = max(1, sum(f.file_bytes for f in input_files if f is not None) // 2)
            compact_pf = lake_store.register_file(lk, cand.partition_key, rows=output_rows, bytes_=output_bytes)
            lake_store.replace_file_list(lk, list(cand.claim_file_ids), [compact_pf])
            # 已确认的压实边界：快照落链并可用
            all_file_ids = [f.file_id for f in lake_store.list_files(lk)]
            snaps = lake_store.list_snapshots(lk)
            available = [s for s in snaps if s.status == "available"]
            snap = lake_store.create_snapshot(
                lk, all_file_ids,
                parent_snapshot=available[-1].snapshot_id if available else None,
                added_rows=0, deleted_rows=0,
            )
            lake_store.mark_snapshot_available(lk, snap.snapshot_id)
            result = CompactResult(
                result_id=f"cres-{lk}-{uuid.uuid4().hex[:8]}",
                candidate_id=candidate_id,
                lake_table=lk,
                partition_key=cand.partition_key,
                input_file_ids=list(cand.claim_file_ids),
                output_file_id=compact_pf.file_id,
                output_rows=output_rows,
                boundary_snapshot_id=snap.snapshot_id,
                confirmed_at=time.time(),
            )
            lake_store.save_compact_result(result)
            cand.status = "已完成"
            cand.result_id = result.result_id
            return {
                "success": True,
                "outcome": "已完成",
                "idempotent": False,
                "candidate_id": candidate_id,
                "result_id": result.result_id,
                "boundary_snapshot_id": snap.snapshot_id,
                "output_file_id": compact_pf.file_id,
                "output_rows": output_rows,
                "conflict_seen": conflict_seen,
                "recomputed_share": recomputed_share,
            }
        finally:
            lake_store.release_write_lock(lk)

    def recompute_eligibility(self, candidate_id: str) -> dict[str, Any]:
        """值班员复算：按接单那一轮冻结的文件范围与合计重算执行资格。

        只引用接单时冻结的范围，不重扫分区文件；同时回查范围内文件当前登记状态。
        """
        cand = lake_store.get_candidate(candidate_id)
        if cand is None:
            return {"success": False, "reason": f"候选 {candidate_id} 不存在"}
        if not cand.claim_epoch_id:
            return {
                "success": False,
                "candidate_id": candidate_id,
                "status": cand.status,
                "reason": "候选尚未接单，没有可复算的接单轮次",
            }
        eligible = is_partition_skewed(cand.claim_partition_rows, cand.claim_total_rows)
        file_audit = [
            {
                "file_id": fid,
                "still_registered": lake_store.get_file(cand.lake_table, fid) is not None,
            }
            for fid in cand.claim_file_ids
        ]
        return {
            "success": True,
            "candidate_id": candidate_id,
            "status": cand.status,
            "claim_epoch_id": cand.claim_epoch_id,
            "claim_share": f"{cand.claim_partition_rows}/{cand.claim_total_rows}",
            "eligible_at_claim": eligible,
            "result_id": cand.result_id,
            "file_audit": file_audit,
        }

    def list_candidate_overwrites(self, lake_table: str) -> list[dict[str, Any]]:
        """值班员查看候选覆盖变化：旧候选为何失效/撤回，再按新画像派发。"""
        return [
            {
                "candidate_id": o.candidate_id,
                "partition_key": o.partition_key,
                "old_epoch_id": o.old_epoch_id,
                "new_epoch_id": o.new_epoch_id,
                "old_share": o.old_share,
                "new_share": o.new_share,
                "reason": o.reason,
            }
            for o in lake_store.list_candidate_overwrites(lake_table)
        ]
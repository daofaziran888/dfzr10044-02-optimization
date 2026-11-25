"""测试统计画像 -> 压实候选 -> 压实确认的阈值判定与并发行为。

场景覆盖：
- 按分区聚合行数 / 全表行数，严格超过一半才算倾斜，空表没有可执行候选
- A=6/B=4 入湖后画像 6/10；B 迟到批次 +5 后画像 6/15，未接单候选失去执行资格
- 已接单未确认与 B 的发布作为同一次并发试验：
  - B 先提交：A 因合计变化被撤回，值班员可见覆盖变化
  - B 等待 A：B 的发布结果标明在 A 的已确认压实边界之后生效
  - 两边处理完全表合计 15 行，B 的 5 行只入湖一次
- 压实完成后值班员按接单那一轮的文件范围复算执行资格
- 仍有资格的候选重复提交只保留一份压实结果
"""
from __future__ import annotations

import threading

from app.lake_store import lake_store
from app.services.stats_profile import StatsProfileService, is_partition_skewed
from app.services.compact_job import CompactJobService
from app.services.ingest_batch import IngestBatchService


def _rows(n: int, tag: str) -> list[dict[str, str]]:
    return [{"v": f"{tag}-{i}"} for i in range(n)]


def _setup_ab(table: str):
    """A 入湖 6 行到 pA、B 入湖 4 行到 pB，返回 pA 最新待接单候选（画像 6/10）。"""
    ingest = IngestBatchService()
    ingest.publish_partition_batch(table, "pA", f"{table}-batch-a", _rows(6, "a"))
    ingest.publish_partition_batch(table, "pB", f"{table}-batch-b", _rows(4, "b"))
    epoch = lake_store.latest_profile_epoch(table)
    assert epoch is not None and epoch.total_rows == 10
    pending = [
        c for c in lake_store.list_candidates(table, status="待接单")
        if c.partition_key == "pA"
    ]
    assert len(pending) == 1
    cand = pending[0]
    assert cand.partition_rows == 6 and cand.total_rows == 10
    assert cand.file_ids, "候选必须交付可核对的输入范围"
    return cand


class TestSkewThreshold:
    """阈值判定：严格超过一半才算倾斜，空表没有可执行候选。"""

    def test_strictly_over_half(self):
        assert not is_partition_skewed(5, 10)   # 恰好一半不算倾斜
        assert is_partition_skewed(6, 10)       # 严格超过一半才算
        assert not is_partition_skewed(0, 0)    # 空表不倾斜

    def test_empty_table_has_no_candidate(self):
        profile = StatsProfileService().publish_profile_epoch("cand_empty")
        assert profile["empty_table"]
        assert profile["total_rows"] == 0
        assert profile["delivered_candidates"] == []

    def test_candidate_carries_auditable_file_range(self):
        table = "cand_range"
        cand = _setup_ab(table)
        view = CompactJobService().list_candidates(table, status="待接单")[0]
        assert view["candidate_id"] == cand.candidate_id
        assert view["share"] == "6/10"
        # 可回查：候选文件范围与画像轮次冻结的范围一致
        epoch = lake_store.latest_profile_epoch(table)
        assert view["file_ids"] == epoch.partition_files["pA"]
        assert view["epoch_id"] == epoch.epoch_id


class TestUnclaimedCandidateExpiry:
    """A 的候选尚未接单时 B 迟到批次入湖：画像 6/10 -> 6/15，A 失去执行资格。"""

    def test_unclaimed_candidate_loses_eligibility(self):
        table = "cand_t1"
        cand = _setup_ab(table)
        compact = CompactJobService()

        # B 的迟到批次 +5 行入湖
        pub = IngestBatchService().publish_partition_batch(
            table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
        assert pub["table_total_rows"] == 15

        # 旧候选已失效：画像从 6/10 变为 6/15
        assert cand.status == "已失效"
        assert cand.invalid_reason == "合计变化不再倾斜"

        # 值班员能看到这次覆盖变化
        overwrites = [
            o for o in compact.list_candidate_overwrites(table)
            if o["candidate_id"] == cand.candidate_id
        ]
        assert overwrites, "覆盖变化必须对值班员可见"
        assert overwrites[-1]["old_share"] == "6/10"
        assert overwrites[-1]["new_share"] == "6/15"

        # 未接单的 A 失去执行资格
        claim = compact.claim_candidate(cand.candidate_id)
        assert not claim["success"]

        # 值班员按新画像派发：pB 9/15 倾斜，新候选可接单
        pending = compact.list_candidates(table, status="待接单")
        assert len(pending) == 1
        assert pending[0]["partition_key"] == "pB"
        assert pending[0]["share"] == "9/15"
        claim2 = compact.claim_candidate(pending[0]["candidate_id"])
        assert claim2["success"]
        assert claim2["claim_share"] == "9/15"


class TestClaimedVsPublishConcurrency:
    """A 已接单但尚未确认 与 B 的发布：同一次并发试验的两种次序。"""

    def test_b_commits_first_a_withdrawn_by_total_change(self):
        """B 先提交：A 因合计变化被撤回，操作员能看到撤回原因。"""
        table = "cand_t2"
        cand = _setup_ab(table)
        compact = CompactJobService()
        claim = compact.claim_candidate(cand.candidate_id)
        assert claim["success"] and claim["claim_share"] == "6/10"

        # 压实已读 A 的文件（接单锁定的范围），尚未确认
        read_rows = sum(
            lake_store.get_file(table, fid).file_rows for fid in cand.claim_file_ids
        )
        assert read_rows == 6

        # B 的补批先提交
        pub = IngestBatchService().publish_partition_batch(
            table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
        assert pub["success"] and pub["table_total_rows"] == 15

        # A 确认时与 B 的发布冲突：重算 6/15 不达门槛，因合计变化被撤回
        confirm = compact.confirm_candidate(cand.candidate_id)
        assert not confirm["success"]
        assert confirm["outcome"] == "已撤回"
        assert confirm["reason"] == "合计变化不再倾斜"
        assert confirm["old_share"] == "6/10"
        assert confirm["new_share"] == "6/15"

        # 两边处理完：全表合计 15 行，B 的 5 行只入湖一次
        assert sum(f.file_rows for f in lake_store.list_files(table)) == 15
        assert sum(f.file_rows for f in lake_store.list_files(table, "pB")) == 9

        # 覆盖变化值班员可见
        overwrites = [
            o for o in compact.list_candidate_overwrites(table)
            if o["candidate_id"] == cand.candidate_id
        ]
        assert overwrites[-1]["reason"] == "合计变化不再倾斜"
        assert overwrites[-1]["new_share"] == "6/15"

    def test_b_waits_for_a_publish_effective_after_boundary(self):
        """B 等待 A：B 的发布结果标明在 A 的已确认压实边界之后生效。"""
        table = "cand_t3"
        cand = _setup_ab(table)
        compact = CompactJobService()
        assert compact.claim_candidate(cand.candidate_id)["success"]

        # A 先确认：产生已确认的压实边界
        confirm = compact.confirm_candidate(cand.candidate_id)
        assert confirm["success"] and confirm["outcome"] == "已完成"
        boundary = confirm["result_id"]

        # B 等待 A 后发布：结果标明生效位置在 A 的压实边界之后
        pub = IngestBatchService().publish_partition_batch(
            table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
        assert pub["success"]
        assert pub["effective_after_boundary"] == boundary
        assert pub["boundary_snapshot_id"] == confirm["boundary_snapshot_id"]

        # 两边处理完：全表合计 15 行
        assert sum(f.file_rows for f in lake_store.list_files(table)) == 15

    def test_late_batch_ingested_exactly_once(self):
        """B 的 5 行只入湖一次：冲突重试同一批次幂等。"""
        table = "cand_t4"
        cand = _setup_ab(table)
        compact = CompactJobService()
        compact.claim_candidate(cand.candidate_id)
        ingest = IngestBatchService()

        ingest.publish_partition_batch(table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
        # 冲突后重试：同一 batch_id 重复发布
        retry = ingest.publish_partition_batch(
            table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
        assert retry["idempotent"]
        assert retry["applied_rows"] == 0

        assert sum(f.file_rows for f in lake_store.list_files(table)) == 15
        assert sum(f.file_rows for f in lake_store.list_files(table, "pB")) == 9

    def test_confirm_vs_publish_race_invariants(self):
        """真并发：确认与发布同时起跑，无论哪种次序不变量都成立。"""
        table = "cand_t8"
        cand = _setup_ab(table)
        compact = CompactJobService()
        compact.claim_candidate(cand.candidate_id)
        ingest = IngestBatchService()

        barrier = threading.Barrier(2)
        outcomes: dict[str, dict] = {}
        errors: list[Exception] = []

        def do_confirm():
            try:
                barrier.wait(timeout=5)
                outcomes["confirm"] = compact.confirm_candidate(cand.candidate_id)
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def do_publish():
            try:
                barrier.wait(timeout=5)
                outcomes["publish"] = ingest.publish_partition_batch(
                    table, "pB", f"{table}-batch-b-late", _rows(5, "b2"))
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        t1 = threading.Thread(target=do_confirm)
        t2 = threading.Thread(target=do_publish)
        t1.start()
        t2.start()
        t1.join(10)
        t2.join(10)

        assert not errors
        # 不变量：全表合计 15 行，B 的 5 行只入湖一次
        assert sum(f.file_rows for f in lake_store.list_files(table)) == 15
        assert sum(f.file_rows for f in lake_store.list_files(table, "pB")) == 9

        confirm = outcomes["confirm"]
        publish = outcomes["publish"]
        if confirm["success"]:
            # A 先确认：B 的发布在 A 的已确认压实边界之后生效
            assert confirm["outcome"] == "已完成"
            assert publish["effective_after_boundary"] == confirm["result_id"]
        else:
            # B 先提交：A 因合计变化被撤回
            assert confirm["outcome"] == "已撤回"
            assert confirm["reason"] == "合计变化不再倾斜"

    def test_conflict_recompute_still_eligible_confirms(self):
        """冲突重算仍达门槛：B 只补 1 行（合计 11），A 6/11 仍严格过半，确认继续。"""
        table = "cand_t7"
        cand = _setup_ab(table)
        compact = CompactJobService()
        compact.claim_candidate(cand.candidate_id)

        IngestBatchService().publish_partition_batch(
            table, "pB", f"{table}-batch-b-late", _rows(1, "b2"))
        confirm = compact.confirm_candidate(cand.candidate_id)
        assert confirm["success"]
        assert confirm["conflict_seen"]
        assert confirm["recomputed_share"] == "6/11"
        assert confirm["outcome"] == "已完成"


class TestAuditAndIdempotency:
    """值班员复算与压实结果幂等。"""

    def test_recompute_eligibility_from_claim_round(self):
        """压实完成后，值班员按接单那一轮的文件范围复算执行资格。"""
        table = "cand_t5"
        cand = _setup_ab(table)
        compact = CompactJobService()
        compact.claim_candidate(cand.candidate_id)
        confirm = compact.confirm_candidate(cand.candidate_id)
        assert confirm["success"]

        rec = compact.recompute_eligibility(cand.candidate_id)
        assert rec["success"]
        assert rec["claim_epoch_id"] == cand.claim_epoch_id
        assert rec["claim_share"] == "6/10"
        assert rec["eligible_at_claim"] is True
        assert rec["result_id"] == confirm["result_id"]
        # 可回查：接单范围文件已被压实输出替换
        assert len(rec["file_audit"]) == len(cand.claim_file_ids)
        assert all(not f["still_registered"] for f in rec["file_audit"])

    def test_recompute_requires_claim_round(self):
        table = "cand_t9"
        cand = _setup_ab(table)
        rec = CompactJobService().recompute_eligibility(cand.candidate_id)
        assert not rec["success"]
        assert "尚未接单" in rec["reason"]

    def test_duplicate_confirm_keeps_single_result(self):
        """仍有资格的分区重复提交同一候选，只保留一份压实结果。"""
        table = "cand_t6"
        cand = _setup_ab(table)
        compact = CompactJobService()
        compact.claim_candidate(cand.candidate_id)

        first = compact.confirm_candidate(cand.candidate_id)
        second = compact.confirm_candidate(cand.candidate_id)
        assert first["success"] and second["success"]
        assert second["idempotent"]
        assert first["result_id"] == second["result_id"]

        # 分区只有一份压实输出，没有重复压实
        pA_files = lake_store.list_files(table, "pA")
        assert len(pA_files) == 1
        assert pA_files[0].file_rows == 6

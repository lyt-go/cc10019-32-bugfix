"""清洗 → 聚合链路回归测试：批次、可用记录、版本一致性与逐桶回查。"""
from __future__ import annotations

import copy
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.store import store
from app.services.event_aggregate import (
    aggregate_events,
    compare_aggregate_results,
    get_aggregate_result,
    verify_aggregate_result,
)
from app.services.event_cleanse import (
    cleanse_events,
    get_cleanse_batch,
    get_latest_cleanse_batch,
)


class TestCleanseAggregateChain(unittest.TestCase):
    def setUp(self) -> None:
        store._artifacts.clear()
        store._compute_cache.clear()
        store._snapshots.clear()
        # 恢复规则被动作修改的字段，避免彼此影响。
        for row in store.rows("event_cleanse"):
            if row["rule_id"] == "EC-002":
                row["anomaly_action"] = "quarantine"
                row["dedup_window"] = 5
                row["version"] = "v1.0"

    def _quarantine_one(self, batch: dict) -> dict:
        """模拟新清洗把一条原本可用的记录调整为隔离。"""
        adjusted = copy.deepcopy(batch)
        target = next(r for r in adjusted["records"] if r.get("status") == "valid")
        target.update({
            "status": "quarantined",
            "quarantined": True,
            "usable": False,
            "anomaly_reason": "invalid",
        })
        adjusted["valid_records"] -= 1
        adjusted["invalid_records"] += 1
        adjusted["quarantined_records"] += 1
        target_id = target["record_id"]
        adjusted["valid_record_ids"] = [
            rid for rid in adjusted["valid_record_ids"] if rid != target_id
        ]
        adjusted["usable_record_ids"] = [
            rid for rid in adjusted["usable_record_ids"] if rid != target_id
        ]
        adjusted["quarantined_record_ids"].append(target_id)
        adjusted["stats"].update({
            "valid_records": adjusted["valid_records"],
            "invalid_records": adjusted["invalid_records"],
            "quarantined_records": adjusted["quarantined_records"],
            "usable_records": adjusted["valid_records"],
        })
        adjusted["cleanse_version"] = "v1.1#adjusted"
        adjusted["batch_id"] = "EC-002-adjusted-b999999"
        return adjusted

    def test_cleanse_statistics_are_self_consistent(self) -> None:
        for rule_id in ("EC-001", "EC-002", "EC-003"):
            result = cleanse_events(rule_id).model_dump()
            self.assertEqual(
                result["valid_records"]
                + result["invalid_records"]
                + result["deduped_records"],
                result["total_records"],
            )
            self.assertEqual(
                result["quarantined_records"] + result["discarded_records"],
                result["invalid_records"],
            )
            self.assertEqual(result["valid_records"], len(result["cleansed_records"]))
            self.assertEqual(
                len(result["valid_record_ids"])
                + len(result["quarantined_record_ids"])
                + len(result["discarded_record_ids"])
                + len(result["deduped_record_ids"]),
                result["total_records"],
            )

    def test_quarantined_and_discarded_records_never_aggregate(self) -> None:
        result = cleanse_events("EC-002").model_dump()
        self.assertGreater(result["quarantined_records"], 0)

        agg = aggregate_events("EA-002")
        aggregated_ids = set(agg.source_record_ids)
        all_ids = set(result["source_record_ids"])
        self.assertFalse(aggregated_ids & set(result["quarantined_record_ids"]))
        self.assertFalse(aggregated_ids & set(result["discarded_record_ids"]))
        self.assertFalse(aggregated_ids & set(result["deduped_record_ids"]))
        self.assertTrue(aggregated_ids <= all_ids)
        self.assertEqual(len(aggregated_ids), result["valid_records"])
        self.assertEqual(agg.source_batch_id, result["batch_id"])
        self.assertEqual(agg.source_cleanse_version, result["cleanse_version"])

    def test_aggregation_sums_only_usable_records_per_bucket(self) -> None:
        batch = cleanse_events("EC-002").model_dump()
        agg = aggregate_events("EA-002")
        usable = [r for r in batch["records"] if r.get("usable")]
        self.assertEqual(agg.total_count, len(usable))
        self.assertEqual(agg.result_row_count, len(agg.result_rows))
        self.assertAlmostEqual(
            agg.total_amount,
            round(sum(float(r.get("value", 0)) for r in usable), 6),
            places=6,
        )
        # 每个桶的 record_ids 只指向可用记录
        for row in agg.result_rows:
            for record_id in row["record_ids"]:
                record = next(r for r in batch["records"] if r["record_id"] == record_id)
                self.assertTrue(record["usable"])
                self.assertNotIn(record["status"], ("quarantined", "discarded", "deduped"))
            self.assertEqual(row["count"], row["record_count"])

    def test_batch_retry_does_not_double_count(self) -> None:
        batch = cleanse_events("EC-001")
        first = aggregate_events("EA-001", batch.batch_id)
        for _ in range(3):
            retried = aggregate_events("EA-001", batch.batch_id)
            self.assertEqual(retried.total_count, first.total_count)
            self.assertEqual(retried.total_amount, first.total_amount)
            self.assertEqual(retried.result_row_count, first.result_row_count)

    def test_requery_follows_latest_cleanse_version(self) -> None:
        old = cleanse_events("EC-001")
        aggregate_events("EA-001", old.batch_id)

        new = cleanse_events("EC-001")
        self.assertNotEqual(new.batch_id, old.batch_id)

        stored = store.get_artifact("event_aggregate", "EA-001")
        self.assertEqual(stored["source_batch_id"], old.batch_id)

        refreshed = get_aggregate_result("EA-001")
        self.assertEqual(refreshed["source_batch_id"], new.batch_id)
        self.assertEqual(refreshed["source_cleanse_version"], new.cleanse_version)
        # 新批次的有效记录与结果逐条对应
        self.assertEqual(refreshed["source_record_count"], new.valid_records)

    def test_competing_snapshots_do_not_cross(self) -> None:
        snapshot_a = get_latest_cleanse_batch("EV-002")
        if snapshot_a is None:
            snapshot_a = cleanse_events("EC-002").model_dump()
        result_a = aggregate_events("EA-002", snapshot_a)

        snapshot_b = self._quarantine_one(snapshot_a)
        result_b = aggregate_events("EA-002", snapshot_b)

        # 统计与来源记录不能交叉：B 的版本、批次、计数和来源编号来自同一快照
        self.assertEqual(result_a.source_batch_id, snapshot_a["batch_id"])
        self.assertEqual(result_b.source_batch_id, snapshot_b["batch_id"])
        self.assertEqual(result_b.source_cleanse_version, snapshot_b["cleanse_version"])
        self.assertEqual(result_b.total_count, snapshot_b["valid_records"])
        self.assertEqual(
            result_a.source_record_count - result_b.source_record_count, 1
        )
        removed = set(snapshot_a["valid_record_ids"]) - set(snapshot_b["valid_record_ids"])
        self.assertEqual(len(removed), 1)
        self.assertFalse(removed & set(result_b.source_record_ids))
        self.assertTrue(removed <= set(result_a.source_record_ids))

    def test_verify_each_bucket_and_compare_two_runs(self) -> None:
        snapshot_a = get_latest_cleanse_batch("EV-002")
        if snapshot_a is None:
            snapshot_a = cleanse_events("EC-002").model_dump()
        result_a = aggregate_events("EA-002", snapshot_a)
        snapshot_b = self._quarantine_one(snapshot_a)
        result_b = aggregate_events("EA-002", snapshot_b)

        verify_a = verify_aggregate_result("EA-002", result_a.source_batch_id)
        verify_b = verify_aggregate_result("EA-002", result_b.source_batch_id)
        self.assertTrue(verify_a["consistent"])
        self.assertTrue(verify_b["consistent"])
        for bucket in verify_a["buckets"] + verify_b["buckets"]:
            self.assertTrue(bucket["record_ids"])
            self.assertTrue(all(r["usable"] for r in bucket["records"]))
            self.assertEqual(len(bucket["record_ids"]), bucket["record_count"])
            self.assertTrue(all(r["rule_version"] for r in bucket["records"]))
            self.assertEqual(bucket["rule_versions"], ["v1.0"])

        comparison = compare_aggregate_results(
            "EA-002", result_a.source_batch_id, result_b.source_batch_id
        )
        self.assertEqual(comparison["total_count_diff"], -1)
        self.assertEqual(comparison["changed_bucket_count"], 1)
        changed = next(b for b in comparison["buckets"] if b["count_diff"])
        removed_id = set(changed["record_ids_a"]) - set(changed["record_ids_b"])
        self.assertEqual(len(removed_id), 1)
        removed = next(
            r for r in snapshot_a["records"] if r["record_id"] in removed_id
        )
        self.assertAlmostEqual(changed["amount_diff"], -float(removed["value"]), places=6)

    def test_cleanse_batch_lookup_by_id(self) -> None:
        result = cleanse_events("EC-003").model_dump()
        batch = get_cleanse_batch(result["batch_id"])
        self.assertIsNotNone(batch)
        self.assertEqual(batch["stats"]["total_records"], batch["total_records"])
        self.assertEqual(
            batch["valid_records"] + batch["invalid_records"] + batch["deduped_records"],
            batch["total_records"],
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)

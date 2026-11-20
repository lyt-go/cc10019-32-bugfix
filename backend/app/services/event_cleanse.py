"""事件清洗引擎：校验事件数据，执行去重和异常处理。

一次清洗执行会产出一个不可变的批次快照（``CleanseResult``）：批内统计、
逐条记录状态、规则版本和批次号都来自同一次判定，聚合只能整体消费其中一个
批次，避免把两次清洗的统计和记录混在一起。
"""
from __future__ import annotations

import copy
import threading
import time
from typing import Any

from app.store import store
from app.schemas import CleanseResult

# 清洗批次提交与聚合读取快照共用一把可重入锁：读取时要么看到旧批次，
# 要么看到新批次，不会读到“统计已更新、记录还是旧的”的中间状态。
cleanse_lock = threading.RLock()

# 批次序号全局递增，用于在同一时间戳内稳定判定“谁更新”。
_batch_seq = 0


def _next_batch_seq() -> int:
    global _batch_seq
    _batch_seq += 1
    return _batch_seq


def _generate_raw_event_data(event_id: str, count: int) -> list[dict[str, Any]]:
    """生成原始事件数据用于清洗。

    数据完全由入参决定，重复运行同一规则得到同一批用户、时间和金额，
    这样才能按“一条记录的隔离状态变化”核对两次聚合的差异。
    """
    events = []
    base_time = 1700000000
    channels = ["app", "web", "miniapp", "h5"]
    devices = ["ios", "android", "pc", "other"]
    levels = ["普通", "银卡", "金卡", "钻石"]

    for i in range(1, count + 1):
        user_id = f"U{i:05d}"
        # 模拟部分异常数据
        is_bot = i % 20 == 0
        is_dup = i % 15 == 0
        is_invalid = i % 25 == 0

        base = {
            "user_id": user_id,
            "event_id": event_id,
            "timestamp": base_time + i * 60,
            "page_url": "" if is_invalid else f"/page{i % 10}",
            "item_id": -1 if is_invalid else i,
            "amount": -10.0 if is_invalid else float(i * 5.5),
            "value": -10.0 if is_invalid else float(i * 5.5),
            "ATTR-001": channels[i % 4],
            "ATTR-003": -10.0 if is_invalid else float(i * 5.5),
            "ATTR-004": devices[i % 4],
            "ATTR-005": levels[i % 4],
            "is_bot": is_bot,
            "is_dup": is_dup,
        }
        events.append(base)
        if is_dup:
            # 插入一条同用户、同事件、同时间的重复数据
            duplicate = dict(base)
            duplicate["is_bot"] = False
            duplicate["is_dup"] = True
            events.append(duplicate)
    return events


def _field_is_invalid(evt: dict[str, Any], check_fields: list[str],
                      valid_range: dict[str, Any]) -> bool:
    """检查记录的校验字段与合法范围。"""
    for field in check_fields:
        val = evt.get(field)
        if val is None or val == "":
            return True
        if field not in valid_range:
            continue
        range_cfg = valid_range[field]
        try:
            numeric = float(val)
        except (ValueError, TypeError):
            # 时间字段在示例里是日期字符串，无法转数值时跳过范围比较。
            continue
        if "min" in range_cfg:
            try:
                if numeric < float(range_cfg["min"]):
                    return True
            except (ValueError, TypeError):
                pass
        if "max" in range_cfg:
            try:
                if numeric > float(range_cfg["max"]):
                    return True
            except (ValueError, TypeError):
                pass
    return False


def _cleanse_snapshot(rule: dict[str, Any]) -> dict[str, Any]:
    """在锁内执行一次清洗并返回完整的批次快照。"""
    rule_id = rule["rule_id"]
    event_id = rule["event_id"]
    check_fields = rule.get("check_fields", [])
    valid_range = rule.get("valid_range", {})
    anomaly_action = rule.get("anomaly_action", "discard")
    bot_detect = rule.get("bot_detect", False)
    dedup_window = int(rule.get("dedup_window", 0) or 0)
    rule_version = str(rule.get("version", ""))

    events = _generate_raw_event_data(event_id, 500)
    total_records = len(events)

    records: list[dict[str, Any]] = []
    cleansed_records: list[dict[str, Any]] = []
    status_ids: dict[str, list[str]] = {
        "valid": [], "flagged": [], "quarantined": [], "discarded": [], "deduped": [],
    }
    seen: set[tuple[str, ...]] = set()

    for index, raw in enumerate(events, start=1):
        record_id = f"{event_id}-{index:06d}"
        record = copy.deepcopy(raw)
        record["record_id"] = record_id
        record["batch_rule_version"] = rule_version

        # 每条记录只会落在一个最终状态里：异常与重复不会重复计数。
        anomaly_reason = ""
        if bot_detect and raw.get("is_bot"):
            anomaly_reason = "bot"
        elif _field_is_invalid(raw, check_fields, valid_range):
            anomaly_reason = "invalid"

        status = "valid"
        if anomaly_reason:
            if anomaly_action == "discard":
                status = "discarded"
            elif anomaly_action == "quarantine":
                status = "quarantined"
            else:
                status = "flagged"

        # 去重只对仍可用的记录生效；被隔离/丢弃的记录不占用去重窗口。
        if status in ("valid", "flagged") and dedup_window > 0:
            dup_key = tuple(
                str(record.get(f, "")) for f in ("user_id", "event_id", "timestamp")
            )
            if dup_key in seen:
                status = "deduped"
                anomaly_reason = "duplicate"
            else:
                seen.add(dup_key)

        record["status"] = status
        record["anomaly_reason"] = anomaly_reason
        record["quarantined"] = status == "quarantined"
        record["discarded"] = status == "discarded"
        record["flagged"] = status == "flagged"
        record["deduped"] = status == "deduped"
        record["usable"] = status in ("valid", "flagged")
        record["rule_version"] = rule_version

        records.append(record)
        status_ids[status].append(record_id)
        if record["usable"]:
            cleansed_records.append(record)

    valid_records = len(cleansed_records)
    quarantined_records = len(status_ids["quarantined"])
    discarded_records = len(status_ids["discarded"])
    deduped_records = len(status_ids["deduped"])
    flagged_records = len(status_ids["flagged"])
    invalid_records = quarantined_records + discarded_records

    # 恒等式：total = valid(可用) + invalid(隔离/丢弃) + deduped。
    assert total_records == valid_records + invalid_records + deduped_records

    batch_seq = _next_batch_seq()
    batch_id = f"{rule_id}-v{rule_version or '0'}-b{batch_seq:06d}"
    cleanse_version = f"{rule_version or '0'}#{batch_seq:06d}"

    snapshot = CleanseResult(
        rule_id=rule_id,
        event_id=event_id,
        total_records=total_records,
        valid_records=valid_records,
        invalid_records=invalid_records,
        deduped_records=deduped_records,
        output_table=f"cleansed_{event_id}",
        snapshot_path=f"/data/cleansed/{batch_id}.parquet",
        batch_id=batch_id,
        batch_seq=batch_seq,
        rule_version=rule_version,
        cleanse_version=cleanse_version,
        generated_at=time.time(),
        quarantined_records=quarantined_records,
        discarded_records=discarded_records,
        flagged_records=flagged_records,
        usable_records=valid_records,
        available_records=valid_records,
        excluded_records=invalid_records + deduped_records,
        valid_record_ids=[r["record_id"] for r in cleansed_records],
        usable_record_ids=[r["record_id"] for r in cleansed_records],
        quarantined_record_ids=status_ids["quarantined"],
        discarded_record_ids=status_ids["discarded"],
        deduped_record_ids=status_ids["deduped"],
        records=records,
        cleansed_records=cleansed_records,
        # 整套统计与来源记录来自同一次判定，作为一份快照整体提交。
        stats={
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": invalid_records,
            "deduped_records": deduped_records,
            "quarantined_records": quarantined_records,
            "discarded_records": discarded_records,
            "flagged_records": flagged_records,
            "usable_records": valid_records,
            "excluded_records": invalid_records + deduped_records,
        },
        source_record_ids=[r["record_id"] for r in records],
        available_record_ids=[r["record_id"] for r in cleansed_records],
    ).model_dump()
    snapshot["valid_record_count"] = valid_records
    snapshot["source_records"] = records

    # 批次表保存该批次本身；event_cleanse 只是指向“该规则最近提交的批次”的
    # 引用，因此对当前批次的任何调整都会同时体现在统计与来源记录上。
    store.set_artifacts([
        ("event_cleanse_batch", batch_id, copy.deepcopy(snapshot)),
        ("event_cleanse", rule_id, copy.deepcopy(snapshot)),
    ])
    store.add_snapshot("event_cleanse", copy.deepcopy(snapshot))
    return copy.deepcopy(snapshot)


def cleanse_events(rule_id: str) -> CleanseResult:
    """执行事件清洗，并将其作为一个新批次整体提交。"""
    rule = store.find_by_field("event_cleanse", "rule_id", rule_id)
    if rule is None:
        raise ValueError(f"Cleanse rule not found: {rule_id}")

    with cleanse_lock:
        snapshot = _cleanse_snapshot(dict(rule))
        store.update("event_cleanse", rule["id"], {
            "status": "已生效",
            "pending": False,
        })

    return CleanseResult(**snapshot)


def get_cleanse_result(rule_id: str) -> dict[str, Any] | None:
    """获取清洗结果（该规则最近一次提交的批次快照）。"""
    artifact = store.get_artifact("event_cleanse", rule_id)
    if artifact:
        return copy.deepcopy(artifact)
    try:
        result = cleanse_events(rule_id)
        return result.model_dump()
    except ValueError:
        return None


def get_cleanse_batch(batch_id: str) -> dict[str, Any] | None:
    """按批次号读取清洗批次快照（含逐条记录状态与规则版本）。"""
    artifact = store.get_artifact("event_cleanse_batch", batch_id)
    return copy.deepcopy(artifact) if artifact else None


def list_cleanse_batches(event_id: str | None = None) -> list[dict[str, Any]]:
    """列出清洗批次快照，可按事件过滤；按提交先后排序。"""
    batches = [
        copy.deepcopy(item)
        for item in store.list_artifacts("event_cleanse_batch")
        if event_id is None or item.get("event_id") == event_id
    ]
    return sorted(batches, key=lambda item: (item.get("batch_seq", 0), item.get("batch_id", "")))


def get_latest_cleanse_batch(event_id: str) -> dict[str, Any] | None:
    """获取某事件最近提交的清洗批次快照（返回深拷贝，可安全用于一次性聚合）。"""
    with cleanse_lock:
        candidates = [
            item
            for item in store.list_artifacts("event_cleanse_batch")
            if item.get("event_id") == event_id
        ]
        # 兼容只写入了 event_cleanse 产物的旧调用方；按批次号去重。
        seen = {item.get("batch_id") for item in candidates}
        candidates.extend(
            item
            for item in store.list_artifacts("event_cleanse")
            if item.get("event_id") == event_id and item.get("batch_id") not in seen
        )
        if not candidates:
            return None
        latest = max(
            candidates,
            key=lambda item: (
                float(item.get("generated_at", 0) or 0),
                int(item.get("batch_seq", 0) or 0),
                str(item.get("batch_id", "")),
            ),
        )
        return copy.deepcopy(latest)


def ensure_cleanse_batch(event_id: str) -> dict[str, Any]:
    """确保事件已有清洗批次；没有时先执行该事件已登记的清洗规则。"""
    with cleanse_lock:
        batch = get_latest_cleanse_batch(event_id)
        if batch is not None and batch.get("cleansed_records") is not None:
            return batch

        rules = store.find_all_by_field("event_cleanse", "event_id", event_id)
        if not rules:
            raise ValueError(f"No cleanse rule for event: {event_id}")

        # 选择最近登记/更新的规则执行，作为该事件的第一个批次。
        rule = max(rules, key=lambda item: int(item.get("id", 0) or 0))
        cleanse_events(rule["rule_id"])
        batch = get_latest_cleanse_batch(event_id)
        if batch is None:
            raise ValueError(f"Cleanse batch not produced for event: {event_id}")
        return batch

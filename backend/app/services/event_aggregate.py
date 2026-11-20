"""事件聚合引擎：消费清洗批次快照，按聚合维度与时间桶执行聚合。

关键约束：

* 只累计清洗批次里“确认可用”的记录；被隔离、丢弃或去重的记录不进入聚合。
* 一次聚合只读取一个清洗批次快照（同一批次号、同一规则版本、同一套逐条记录），
  读取与发布都在清洗锁内完成，新批次不会和旧统计交叉。
* 每次提交都从批次重新计算而不是在旧结果上累加，批次重试不会重复计数。
* 结果逐桶记录来源批次、规则版本和有效记录编号，可回查核对计数与金额。
"""
from __future__ import annotations

import copy
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from app.store import store
from app.schemas import AggResult
from app.services.event_cleanse import cleanse_lock, get_latest_cleanse_batch

# 聚合提交与结果发布同样使用清洗锁：保证“读取批次 + 发布结果”是一个原子段。
aggregate_lock = cleanse_lock


def _as_timestamp(value: Any) -> float:
    """把时间字段统一成 epoch 秒；无法解析时返回 0。"""
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            pass
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.timestamp()
        except ValueError:
            return 0.0
    return 0.0


def _time_bucket(timestamp: Any, grain: str) -> str:
    """按时间粒度生成可排序的时间桶。

    hour 级别的桶必须带日期，否则跨天同一小时会被错误地合并。
    """
    moment = datetime.fromtimestamp(_as_timestamp(timestamp), tz=timezone.utc)
    if grain == "minute":
        return moment.strftime("%Y-%m-%d %H:%M")
    if grain == "hour":
        return moment.strftime("%Y-%m-%d %H:00")
    if grain == "day":
        return moment.strftime("%Y-%m-%d")
    if grain == "week":
        year, week, _ = moment.isocalendar()
        return f"{year}-W{week:02d}"
    if grain == "month":
        return moment.strftime("%Y-%m")
    return "all"


def time_bucket(timestamp: Any, grain: str) -> str:
    """公开的时间桶计算，供下游复用同一口径。"""
    return _time_bucket(timestamp, grain)


def _matches_filter(record: dict[str, Any], filter_expr: str) -> bool:
    """执行聚合过滤条件；空条件表示全部通过。"""
    expr = (filter_expr or "").strip()
    if not expr:
        return True
    try:
        from app.services.tag_compute import _eval_condition

        return bool(_eval_condition(record, expr, {}))
    except Exception:
        return False


def _metric_value(record: dict[str, Any]) -> float:
    """取记录用于 sum/avg/max/min 的金额。"""
    for field in ("value", "amount"):
        if field in record:
            try:
                return float(record[field])
            except (TypeError, ValueError):
                continue
    return 0.0


def _agg_value(function: str, records: list[dict[str, Any]]) -> float:
    if function == "count":
        return len(records)
    if function == "uv":
        return len({r.get("user_id") for r in records})
    values = [_metric_value(r) for r in records]
    if function == "sum":
        return round(sum(values), 6)
    if function == "avg":
        return round(sum(values) / len(values), 6) if values else 0.0
    if function == "max":
        return max(values, default=0.0)
    if function == "min":
        return min(values, default=0.0)
    return float(len(records))


def _usable_records(batch: dict[str, Any]) -> list[dict[str, Any]]:
    """从批次快照中取出可参与聚合的记录。

    以批次内显式标记的 ``usable`` 为准；同时排除任何隔离/丢弃/去重标记，
    这样即使批次来自只支持字段标记的旧写入方，也不会把异常记录算进去。
    """
    records = batch.get("records")
    if records is None:
        records = batch.get("cleansed_records") or []

    usable: list[dict[str, Any]] = []
    for record in records:
        if not isinstance(record, dict):
            continue
        if record.get("quarantined") or record.get("discarded") or record.get("deduped"):
            continue
        status = record.get("status")
        if status is not None and status not in ("valid", "flagged"):
            continue
        if record.get("usable") is False:
            continue
        usable.append(record)
    return usable


def _build_result(
    config: dict[str, Any],
    batch: dict[str, Any],
) -> dict[str, Any]:
    """基于单个清洗批次构建聚合结果（纯计算，不写 store）。"""
    config_id = config["config_id"]
    event_id = config["event_id"]
    agg_dimensions = list(config.get("agg_dimensions", []))
    agg_function = config.get("agg_function", "count")
    time_grain = config.get("time_grain", "day")
    filter_expr = config.get("filter_expr", "")

    usable = _usable_records(batch)
    filtered = [record for record in usable if _matches_filter(record, filter_expr)]

    grouped: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for record in filtered:
        key_parts = [str(record.get(dim, "unknown")) for dim in agg_dimensions]
        key_parts.append(_time_bucket(record.get("timestamp"), time_grain))
        grouped[tuple(key_parts)].append(record)

    # rule_version 是清洗规则版本；cleanse_version 还带批次序号，用于精确
    # 指向“哪一次清洗判定”，结果同时记录两者。
    rule_version = str(batch.get("rule_version") or "")
    cleanse_version = str(
        batch.get("cleanse_version")
        or (f"{rule_version}#{batch.get('batch_seq', '')}" if rule_version else "")
    )
    batch_id = str(batch.get("batch_id") or "")
    rule_id = str(batch.get("rule_id") or "")

    result_rows: list[dict[str, Any]] = []
    total_amount = 0.0
    for key, records in sorted(grouped.items()):
        row: dict[str, Any] = {}
        for index, dim in enumerate(agg_dimensions):
            row[dim] = key[index]
        bucket = key[-1]
        row["time_bucket"] = bucket
        row["bucket"] = bucket
        row["time"] = bucket
        # 兼容旧字段：原实现把时间桶写进 time_grain，这里继续保留。
        row["time_grain"] = bucket

        metric = _agg_value(agg_function, records)
        row["agg_value"] = metric
        row["value"] = metric
        if agg_function == "count":
            row["count"] = int(metric)
        else:
            row["count"] = len(records)
        # 每个桶都带上计数与金额，便于按一次清洗判定变化核对两次聚合。
        row["record_count"] = len(records)
        amount = round(sum(_metric_value(r) for r in records), 6)
        row["amount"] = amount
        row["total_amount"] = amount
        total_amount += amount

        record_ids = [str(r.get("record_id")) for r in records if r.get("record_id")]
        row["record_ids"] = record_ids
        row["valid_record_ids"] = record_ids
        row["source_record_ids"] = record_ids
        row["rule_id"] = rule_id
        row["rule_version"] = rule_version
        row["cleanse_rule_version"] = rule_version
        row["cleanse_version"] = cleanse_version
        row["source_cleanse_version"] = cleanse_version
        row["batch_id"] = batch_id
        row["source_batch_id"] = batch_id
        row["cleanse_batch_id"] = batch_id
        result_rows.append(row)

    row_count = len(result_rows)
    source_ids = [
        str(r.get("record_id"))
        for r in filtered
        if r.get("record_id")
    ]

    payload = AggResult(
        config_id=config_id,
        event_id=event_id,
        agg_dimensions=agg_dimensions,
        time_grain=time_grain,
        result_rows=result_rows,
        total_count=len(filtered),
        output_table=config.get("output_table", ""),
        source_batch_id=batch_id,
        batch_id=batch_id,
        cleanse_batch_id=batch_id,
        source_cleanse_batch=batch_id,
        source_cleanse_rule_id=rule_id,
        cleanse_rule_id=rule_id,
        source_cleanse_version=cleanse_version,
        cleanse_rule_version=rule_version,
        generated_at=time.time(),
        source_record_count=len(filtered),
        source_record_ids=source_ids,
        result_row_count=row_count,
        row_count=row_count,
        total_amount=round(total_amount, 6),
    ).model_dump()

    # 聚合自身也带版本号，便于区分同一批次上的重复提交。
    payload["source_cleanse_stats"] = copy.deepcopy(
        batch.get("stats")
        or {
            "total_records": batch.get("total_records", 0),
            "valid_records": batch.get("valid_records", 0),
            "invalid_records": batch.get("invalid_records", 0),
            "deduped_records": batch.get("deduped_records", 0),
        }
    )
    payload["source_cleanse_batch_id"] = batch_id
    payload["source_cleanse_version_id"] = cleanse_version
    payload["source_cleanse_rule_version"] = rule_version
    payload["filtered_out_count"] = len(usable) - len(filtered)
    payload["usable_record_count"] = len(usable)
    payload["agg_function"] = agg_function
    return payload


def aggregate_events(
    config_id: str,
    batch_id: str | dict[str, Any] | None = None,
    *,
    cleanse_batch_id: str | dict[str, Any] | None = None,
) -> AggResult:
    """执行事件聚合，消费一个完整的清洗批次快照并发布结果。

    ``batch_id`` / ``cleanse_batch_id`` 可传入批次号或批次快照本身；不传时使用
    该事件最近提交的清洗批次。一次调用只消费一个批次，且整体重新计算。
    """
    selected = cleanse_batch_id if cleanse_batch_id is not None else batch_id
    with aggregate_lock:
        config = store.find_by_field("event_aggregate", "config_id", config_id)
        if config is None:
            raise ValueError(f"Aggregate config not found: {config_id}")

        event_id = config["event_id"]
        batch: dict[str, Any] | None
        if isinstance(selected, dict):
            batch = copy.deepcopy(selected)
        elif selected:
            batch = store.get_artifact("event_cleanse_batch", selected)
            if batch is None:
                batch = store.get_artifact("event_cleanse", selected)
            if batch is None or batch.get("event_id") != event_id:
                raise ValueError(f"Cleanse batch not found for event {event_id}: {selected}")
            batch = copy.deepcopy(batch)
        else:
            batch = get_latest_cleanse_batch(event_id)
            if batch is None:
                from app.services.event_cleanse import ensure_cleanse_batch

                batch = ensure_cleanse_batch(event_id)
            if batch is None:
                raise ValueError(f"No cleanse batch for event: {event_id}")

        payload = _build_result(dict(config), batch)

        # 结果与来源批次一起整体发布；重复提交覆盖同一键，不会累加。
        # 同时保存本次消费的批次快照，使历史结果可以脱离清洗侧状态逐桶回查。
        store.set_artifacts([
            ("event_aggregate", config_id, copy.deepcopy(payload)),
            (
                "event_aggregate_batch",
                f"{config_id}:{payload['source_batch_id']}",
                copy.deepcopy(payload),
            ),
            (
                "event_aggregate_source",
                f"{config_id}:{payload['source_batch_id']}",
                copy.deepcopy(batch),
            ),
        ])
        store.update("event_aggregate", config["id"], {
            "status": "已生效",
            "pending": False,
        })

    return AggResult(**payload)


def get_aggregate_result(
    config_id: str,
    batch_id: str | None = None,
) -> dict[str, Any] | None:
    """获取聚合结果。

    * 指定 ``batch_id`` 时返回该批次已发布的结果（用于历史版本对照）。
    * 未指定时，已发布结果如果仍对应当前最新清洗批次就直接返回；若清洗已产生
      新批次，则基于最新批次重算并重新发布，保证查询结果与清洗版本一致。
    """
    config = store.find_by_field("event_aggregate", "config_id", config_id)
    if config is None:
        return None

    if batch_id:
        return get_aggregate_batch_result(config_id, batch_id)

    artifact = store.get_artifact("event_aggregate", config_id)
    latest = get_latest_cleanse_batch(config["event_id"])
    latest_batch_id = latest.get("batch_id") if latest else None
    if artifact is not None and (
        latest_batch_id is None or artifact.get("source_batch_id") == latest_batch_id
    ):
        return copy.deepcopy(artifact)

    try:
        return aggregate_events(config_id).model_dump()
    except ValueError:
        return copy.deepcopy(artifact) if artifact is not None else None


def get_aggregate_batch_result(config_id: str, batch_id: str) -> dict[str, Any] | None:
    """按批次读取某次已发布的聚合结果（用于版本对照）。"""
    return store.get_artifact("event_aggregate_batch", f"{config_id}:{batch_id}")


def list_aggregate_batches(config_id: str) -> list[dict[str, Any]]:
    """列出某聚合配置发布过的所有批次结果，按生成时间排序。"""
    prefix = f"{config_id}:"
    results = [
        copy.deepcopy(item)
        for key, item in store._artifacts.get("event_aggregate_batch", {}).items()
        if key.startswith(prefix)
    ]
    return sorted(results, key=lambda item: float(item.get("generated_at", 0) or 0))


def compare_aggregate_results(
    config_id: str,
    batch_id_a: str,
    batch_id_b: str,
) -> dict[str, Any]:
    """对照两个清洗批次上的聚合结果，逐桶给出计数与金额差异。

    用于按“某条记录的一次清洗判定变化”核对两次聚合：差异应只出现在该记录
    所属的桶上，且计数差为 1、金额差等于该记录金额。
    """
    result_a = get_aggregate_batch_result(config_id, batch_id_a)
    result_b = get_aggregate_batch_result(config_id, batch_id_b)
    if result_a is None or result_b is None:
        raise ValueError("Aggregate result not found for one of the batches")

    def bucket_map(result: dict[str, Any]) -> dict[tuple[Any, ...], dict[str, Any]]:
        mapping: dict[tuple[Any, ...], dict[str, Any]] = {}
        dims = result.get("agg_dimensions", [])
        for row in result.get("result_rows", []):
            key = tuple(row.get(dim) for dim in dims) + (row.get("time_bucket"),)
            mapping[key] = row
        return mapping

    map_a = bucket_map(result_a)
    map_b = bucket_map(result_b)
    buckets: list[dict[str, Any]] = []
    for key in sorted(set(map_a) | set(map_b), key=str):
        row_a = map_a.get(key, {})
        row_b = map_b.get(key, {})
        count_a = int(row_a.get("record_count", 0) or 0)
        count_b = int(row_b.get("record_count", 0) or 0)
        amount_a = float(row_a.get("amount", 0.0) or 0.0)
        amount_b = float(row_b.get("amount", 0.0) or 0.0)
        dims = result_a.get("agg_dimensions", [])
        buckets.append({
            "dimensions": {dim: key[i] for i, dim in enumerate(dims)},
            "time_bucket": key[-1],
            "count_a": count_a,
            "count_b": count_b,
            "count_diff": count_b - count_a,
            "amount_a": round(amount_a, 6),
            "amount_b": round(amount_b, 6),
            "amount_diff": round(amount_b - amount_a, 6),
            "agg_value_a": row_a.get("agg_value"),
            "agg_value_b": row_b.get("agg_value"),
            "record_ids_a": row_a.get("record_ids", []),
            "record_ids_b": row_b.get("record_ids", []),
        })
    changed = [b for b in buckets if b["count_diff"] or abs(b["amount_diff"]) > 1e-6]
    return {
        "config_id": config_id,
        "batch_a": batch_id_a,
        "batch_b": batch_id_b,
        "version_a": result_a.get("source_cleanse_version"),
        "version_b": result_b.get("source_cleanse_version"),
        "total_count_a": result_a.get("source_record_count"),
        "total_count_b": result_b.get("source_record_count"),
        "total_count_diff": (result_b.get("source_record_count") or 0)
        - (result_a.get("source_record_count") or 0),
        "total_amount_a": result_a.get("total_amount"),
        "total_amount_b": result_b.get("total_amount"),
        "total_amount_diff": round(
            float(result_b.get("total_amount", 0.0) or 0.0)
            - float(result_a.get("total_amount", 0.0) or 0.0),
            6,
        ),
        "changed_bucket_count": len(changed),
        "buckets": buckets,
    }


def verify_aggregate_result(
    config_id: str,
    batch_id: str | None = None,
) -> dict[str, Any] | None:
    """逐桶回查聚合结果：核对记录编号、规则版本、计数与金额。

    ``batch_id`` 为空时校验当前发布的聚合结果；指定批次时校验该批次已发布
    的结果快照（不再触发重算），便于对历史版本逐桶回查。
    """
    if batch_id:
        result = get_aggregate_batch_result(config_id, batch_id)
    else:
        result = get_aggregate_result(config_id)
    if result is None:
        return None
    batch = store.get_artifact(
        "event_aggregate_source", f"{config_id}:{result.get('source_batch_id', '')}"
    )
    if batch is None:
        batch = store.get_artifact("event_cleanse_batch", result.get("source_batch_id", ""))
    if batch is None:
        latest = get_latest_cleanse_batch(result.get("event_id", ""))
        batch = latest
    record_index: dict[str, dict[str, Any]] = {}
    if batch:
        for record in batch.get("records", []):
            record_index[str(record.get("record_id"))] = record

    buckets: list[dict[str, Any]] = []
    all_consistent = True
    agg_function = result.get("agg_function", "")
    for row in result.get("result_rows", []):
        ids = row.get("record_ids") or row.get("valid_record_ids") or []
        records = [record_index[rid] for rid in ids if rid in record_index]
        amount = round(sum(_metric_value(r) for r in records), 6)
        count_ok = len(records) == int(row.get("record_count", len(records)))
        # 回查时用桶内记录重算指标，避免只信结果行里的数字。
        value_ok = True
        if agg_function and batch is not None:
            value_ok = abs(_agg_value(agg_function, records) - float(row.get("agg_value", 0.0))) < 1e-6
        amount_ok = abs(amount - float(row.get("amount", 0.0))) < 1e-6
        missing = [rid for rid in ids if rid not in record_index]
        unusable = [
            r.get("record_id") for r in records
            if r.get("quarantined") or r.get("discarded") or r.get("deduped")
        ]
        consistent = count_ok and value_ok and amount_ok and not missing and not unusable
        all_consistent = all_consistent and consistent
        buckets.append({
            "time_bucket": row.get("time_bucket"),
            "dimensions": {dim: row.get(dim) for dim in result.get("agg_dimensions", [])},
            "agg_value": row.get("agg_value"),
            "record_count": row.get("record_count"),
            "amount": row.get("amount"),
            "rule_version": row.get("rule_version"),
            "cleanse_version": row.get("cleanse_version"),
            "batch_id": row.get("batch_id"),
            "record_ids": ids,
            "records": records,
            "missing_record_ids": missing,
            "unusable_record_ids": unusable,
            "rule_versions": sorted({str(r.get("rule_version", "")) for r in records}),
            "consistent": consistent,
        })

    return {
        "config_id": config_id,
        "event_id": result.get("event_id"),
        "source_batch_id": result.get("source_batch_id"),
        "source_cleanse_version": result.get("source_cleanse_version"),
        "source_record_count": result.get("source_record_count"),
        "result_row_count": result.get("result_row_count", len(result.get("result_rows", []))),
        "total_amount": result.get("total_amount"),
        "consistent": all_consistent,
        "buckets": buckets,
    }

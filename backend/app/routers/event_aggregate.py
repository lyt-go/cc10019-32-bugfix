"""事件聚合路由：执行事件聚合并获取结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionPayload, ActionResult
from app.services.event_aggregate import (
    _usable_records,
    aggregate_events,
    compare_aggregate_results,
    get_aggregate_batch_result,
    get_aggregate_result,
    list_aggregate_batches,
    verify_aggregate_result,
)
from app.services.event_cleanse import get_latest_cleanse_batch, list_cleanse_batches
from app.store import store, query_filters

router = APIRouter(prefix="/api/event-aggregate", tags=["event_aggregate"])


def _summary() -> dict[str, Any]:
    """聚合入口指标：与来源清洗批次对账，不再返回自造的示例行数。"""
    rows = store.rows("event_aggregate")
    published = store.list_artifacts("event_aggregate")
    row_count = 0
    source_count = 0
    cleansed_valid = 0
    filtered_out = 0
    consistent = True
    for item in published:
        result_rows = item.get("result_rows", [])
        source_records = int(item.get("source_record_count", 0) or 0)
        row_count += len(result_rows)
        source_count += source_records
        filtered_out += int(item.get("filtered_out_count", 0) or 0)
        batch = store.get_artifact(
            "event_cleanse_batch", item.get("source_batch_id", "")
        )
        if batch is None:
            consistent = False
            continue
        usable = len(_usable_records(batch))
        cleansed_valid += int(batch.get("valid_records", 0) or 0)
        # 结果行数、参与记录数与来源批次必须能对上，否则统计不可信。
        if len(result_rows) != int(item.get("result_row_count", 0) or 0):
            consistent = False
        if source_records + int(item.get("filtered_out_count", 0) or 0) > usable:
            consistent = False
        if int(batch.get("excluded_records", 0) or 0) != (
            int(batch.get("invalid_records", 0) or 0)
            + int(batch.get("deduped_records", 0) or 0)
        ):
            consistent = False
    return {
        "metrics": [
            {"label": "聚合配置数", "value": len(rows), "trend": 0},
            {"label": "已发布结果行数", "value": row_count, "trend": 0},
            {"label": "参与聚合有效记录", "value": source_count, "trend": 0},
            {"label": "清洗有效记录", "value": cleansed_valid, "trend": 0},
        ],
        "row_count": row_count,
        "source_record_count": source_count,
        "filtered_out_records": filtered_out,
        "cleansed_valid_records": cleansed_valid,
        "consistent": consistent,
    }


@router.get("")
def index(page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("event_aggregate")
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}


@router.get("/summary")
def summary() -> dict[str, Any]:
    return _summary()


@router.get("/results")
def published_results() -> dict[str, Any]:
    """已发布的聚合结果（含来源清洗批次），用于页面刷新后直接回显。

    读取时会对齐当前清洗批次：清洗已产生新批次时基于新批次重算，
    保证刷新后看到的结果与其清洗版本一致。
    """
    items: list[dict[str, Any]] = []
    for row in store.rows("event_aggregate"):
        try:
            refreshed = get_aggregate_result(row["config_id"])
        except ValueError:
            refreshed = None
        if refreshed is not None:
            items.append(refreshed)
    return {"items": items, "total": len(items)}


@router.get("/export")
def export_entries() -> dict[str, Any]:
    rows = store.rows("event_aggregate")
    return {"items": rows, "total": len(rows)}


@router.post("/run")
def run_from_payload(payload: dict[str, Any]) -> ActionResult:
    """分析看板入口：按配置编号执行聚合，并返回结果行。"""
    values = payload.get("values") or {}
    config_id = payload.get("config_id") or values.get("config_id") or values.get("配置编号")
    if not config_id:
        # 未指定配置时，对所有配置逐个执行，返回汇总后的结果行。
        items: list[dict[str, Any]] = []
        for row in store.rows("event_aggregate"):
            try:
                items.extend(aggregate_events(row["config_id"]).result_rows)
            except ValueError:
                continue
        entry = {
            "items": items,
            "result_rows": items,
            "total": len(items),
            "result_row_count": len(items),
            "source_batch_id": "",
        }
        return ActionResult(ok=True, message="Aggregated", entry=entry)
    try:
        result = aggregate_events(str(config_id), payload.get("batch_id"))
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    return ActionResult(ok=True, message="Aggregated", entry=result.model_dump())


@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("event_aggregate", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{config_id}/run")
def run(
    config_id: str,
    batch_id: str | None = Query(default=None, description="指定要消费的清洗批次"),
) -> ActionResult:
    try:
        result = aggregate_events(config_id, batch_id)
        return ActionResult(ok=True, message="Aggregated", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{config_id}/result")
def result(
    config_id: str,
    batch_id: str | None = Query(default=None, description="按批次读取已发布结果"),
) -> dict[str, Any]:
    res = get_aggregate_result(config_id, batch_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{config_id}/verify")
def verify(
    config_id: str,
    batch_id: str | None = Query(default=None, description="按批次回查已发布结果"),
) -> dict[str, Any]:
    """逐桶回查聚合结果与清洗批次的对应关系。"""
    res = verify_aggregate_result(config_id, batch_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{config_id}/compare")
def compare(
    config_id: str,
    batch_a: str = Query(..., description="对照批次 A"),
    batch_b: str = Query(..., description="对照批次 B"),
) -> dict[str, Any]:
    """按两次清洗批次对照聚合结果的计数与金额。"""
    try:
        return compare_aggregate_results(config_id, batch_a, batch_b)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{config_id}/batches")
def batches(config_id: str) -> dict[str, Any]:
    """聚合配置消费过的清洗批次及已发布结果。"""
    config = store.find_by_field("event_aggregate", "config_id", config_id)
    if config is None:
        raise HTTPException(status_code=404, detail="Not found")
    published = list_aggregate_batches(config_id)
    available = list_cleanse_batches(config["event_id"])
    latest = get_latest_cleanse_batch(config["event_id"])
    return {
        "config_id": config_id,
        "event_id": config["event_id"],
        "latest_cleanse_batch": latest.get("batch_id") if latest else None,
        "latest_cleanse_version": latest.get("cleanse_version") if latest else None,
        "published": published,
        "available_cleanse_batches": [
            {
                "batch_id": item.get("batch_id"),
                "batch_seq": item.get("batch_seq"),
                "rule_id": item.get("rule_id"),
                "rule_version": item.get("rule_version"),
                "cleanse_version": item.get("cleanse_version"),
                "valid_records": item.get("valid_records"),
                "quarantined_records": item.get("quarantined_records"),
                "discarded_records": item.get("discarded_records"),
                "deduped_records": item.get("deduped_records"),
            }
            for item in available
        ],
    }


@router.get("/{config_id}/batches/{batch_id}")
def batch_result(config_id: str, batch_id: str) -> dict[str, Any]:
    res = get_aggregate_batch_result(config_id, batch_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("event_aggregate", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)

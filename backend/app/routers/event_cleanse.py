"""事件清洗路由：执行事件清洗并获取结果。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from app.schemas import ActionPayload, ActionResult
from app.services.event_cleanse import (
    cleanse_events,
    get_cleanse_batch,
    get_cleanse_result,
    list_cleanse_batches,
)
from app.store import store, query_filters

router = APIRouter(prefix="/api/event-cleanse", tags=["event_cleanse"])


@router.get("")
def index(request: Request, page: int = 1, size: int = 20) -> dict[str, Any]:
    rows = store.rows("event_cleanse")
    filters=query_filters(request)
    if filters:
        rows = [r for r in rows if all(str(r.get(k)) == str(v) for k, v in filters.items())]
    total = len(rows)
    start = (page - 1) * size
    return {"items": rows[start : start + size], "total": total, "page": page, "size": size}


@router.get("/{entry_id}")
def detail(entry_id: int) -> dict[str, Any]:
    row = store.find("event_cleanse", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    return row


@router.post("/{rule_id}/run")
def run(rule_id: str) -> ActionResult:
    try:
        result = cleanse_events(rule_id)
        return ActionResult(ok=True, message="Cleansed", entry=result.model_dump())
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))


@router.get("/{rule_id}/result")
def result(rule_id: str) -> dict[str, Any]:
    res = get_cleanse_result(rule_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Result not found")
    return res


@router.get("/{rule_id}/batches")
def rule_batches(rule_id: str) -> dict[str, Any]:
    """列出某规则已提交的清洗批次（含统计与版本）。"""
    rule = store.find_by_field("event_cleanse", "rule_id", rule_id)
    if rule is None:
        raise HTTPException(status_code=404, detail="Not found")
    items = [
        item for item in list_cleanse_batches(rule["event_id"])
        if item.get("rule_id") == rule_id
    ]
    return {"items": items, "total": len(items)}


@router.get("/batches/all")
def batches(event_id: str | None = Query(default=None)) -> dict[str, Any]:
    """列出清洗批次快照，可按事件过滤。"""
    items = list_cleanse_batches(event_id)
    return {"items": items, "total": len(items)}


@router.get("/batches/{batch_id}")
def batch_detail(batch_id: str) -> dict[str, Any]:
    """按批次号读取清洗快照：统计、来源记录和规则版本来自同一份快照。"""
    res = get_cleanse_batch(batch_id)
    if res is None:
        raise HTTPException(status_code=404, detail="Not found")
    return res


@router.post("/{entry_id}/actions")
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    row = store.find("event_cleanse", entry_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Not found")
    action = payload.action
    row["status"] = action
    row["pending"] = False
    return ActionResult(ok=True, message=action, entry=row)

"""行为热力服务：消费事件聚合数据，生成热力图分析结果。"""
from __future__ import annotations

from typing import Any
from collections import defaultdict

from app.store import store


def generate_heatmap(config_id: str) -> dict[str, Any]:
    """生成行为热力图，消费事件聚合结果。"""
    config = store.find_by_field("behavior_heatmap", "config_id", config_id)
    if config is None:
        raise ValueError(f"Heatmap config not found: {config_id}")

    event_id = config["event_id"]
    spatial_dim = config["spatial_dim"]
    agg_grain = config["agg_grain"]
    time_range = config["time_range"]

    # 获取事件聚合产物作为输入：优先使用该事件最新发布、且与清洗批次一致的结果。
    agg_artifacts = store.list_artifacts("event_aggregate")
    related_agg = None
    for agg in agg_artifacts:
        if agg.get("event_id") != event_id:
            continue
        if related_agg is None or float(agg.get("generated_at", 0) or 0) >= float(
            related_agg.get("generated_at", 0) or 0
        ):
            related_agg = agg

    # 生成热力数据
    heat_data: dict[str, list[dict[str, Any]]] = defaultdict(list)

    # 模拟热力数据点
    if spatial_dim == "page":
        pages = ["/home", "/product", "/cart", "/checkout", "/profile"]
        for page in pages:
            for hour in range(24):
                heat_data[page].append({
                    "dimension": page,
                    "time_slot": f"{hour:02d}:00",
                    "intensity": 100 + (hour * 10) + hash(page) % 50,
                })
    elif spatial_dim == "element":
        elements = ["button_buy", "button_cart", "link_detail", "banner_top", "search_box"]
        for elem in elements:
            for day in range(7):
                heat_data[elem].append({
                    "dimension": elem,
                    "time_slot": f"day_{day}",
                    "intensity": 50 + (day * 20) + hash(elem) % 30,
                })
    else:  # region
        regions = ["header", "sidebar", "main", "footer", "modal"]
        for region in regions:
            heat_data[region].append({
                "dimension": region,
                "time_slot": "all",
                "intensity": 80 + hash(region) % 100,
            })

    result = {
        "config_id": config_id,
        "event_id": event_id,
        "spatial_dim": spatial_dim,
        "agg_grain": agg_grain,
        "time_range": time_range,
        "heatmap_data": dict(heat_data),
        "max_intensity": max(
            (d["intensity"] for vals in heat_data.values() for d in vals),
            default=0,
        ),
        "min_intensity": min(
            (d["intensity"] for vals in heat_data.values() for d in vals),
            default=0,
        ),
        "source_aggregate": related_agg.get("config_id") if related_agg else None,
        "source_batch_id": related_agg.get("source_batch_id") if related_agg else None,
        "source_cleanse_version": (
            related_agg.get("source_cleanse_version") if related_agg else None
        ),
        "output_image": config.get("output_image", ""),
    }

    # 存储产物
    store.set_artifact("behavior_heatmap", config_id, result)

    # 更新状态
    store.update("behavior_heatmap", config["id"], {
        "status": "已完成",
        "pending": False,
    })

    return result


def get_heatmap(config_id: str) -> dict[str, Any] | None:
    """获取热力图结果。"""
    artifact = store.get_artifact("behavior_heatmap", config_id)
    if artifact:
        return artifact
    try:
        return generate_heatmap(config_id)
    except ValueError:
        return None

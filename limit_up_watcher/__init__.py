"""limit_up_watcher — D201 涨停板排队监控库

Usage:
    from limit_up_watcher import LimitUpWatcher, WebSocketSource

    watcher = LimitUpWatcher("SZ002177", price_li=7950)  # 7.95元涨停价, 买盘
    ws = WebSocketSource("ws://127.0.0.1:8080/d201")
    ws.add_watcher(watcher)

    @watcher.on_tick
    def on_tick(w):
        print(f"封单={w.current.amount}万, 流入={w.inflow_streak}")

    ws.connect(block=True)  # 阻塞运行
"""

from .watcher import LimitUpWatcher
from .ws_source import WebSocketSource
from .types import (
    QueueRecord,
    TickAggregate,
    MyQueueSide,
    MyQueueEntry,
    PricePacket,
    PriceRecord,
)
from .status_codes import (
    is_new_order,
    is_partial_fill,
    is_cancel,
    status_name,
)

__all__ = [
    "LimitUpWatcher",
    "WebSocketSource",
    "QueueRecord",
    "TickAggregate",
    "MyQueueSide",
    "MyQueueEntry",
    "PricePacket",
    "PriceRecord",
    "is_new_order",
    "is_partial_fill",
    "is_cancel",
    "status_name",
]

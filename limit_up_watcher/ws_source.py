"""WebSocket 实时数据源 — 连接 D201，解析 JSON → PricePacket"""
import json
import threading
import logging
from typing import Callable, Optional

from .types import PricePacket, PriceRecord
from .watcher import LimitUpWatcher

logger = logging.getLogger(__name__)


class WebSocketSource:
    """连接 ws://127.0.0.1:8080/d201，订阅 price 数据，喂入 LimitUpWatcher"""

    def __init__(self, url: str = "ws://127.0.0.1:8080/d201"):
        self._url = url
        self._ws = None
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._watchers: list[LimitUpWatcher] = []
        self._lock = threading.Lock()

    def add_watcher(self, w: LimitUpWatcher):
        """注册一个观察器，收到数据时自动 feed"""
        with self._lock:
            if w not in self._watchers:
                self._watchers.append(w)

    def remove_watcher(self, w: LimitUpWatcher):
        with self._lock:
            if w in self._watchers:
                self._watchers.remove(w)

    def connect(self, block: bool = True):
        """连接并订阅。block=True 时阻塞当前线程（forever），否则后台线程运行"""
        import websocket  # type: ignore

        self._running = True
        self._ws = websocket.WebSocketApp(
            self._url,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )

        if block:
            self._ws.run_forever(ping_interval=30, ping_timeout=10)
        else:
            self._thread = threading.Thread(
                target=lambda: self._ws.run_forever(ping_interval=30, ping_timeout=10),
                daemon=True,
            )
            self._thread.start()

    def disconnect(self):
        self._running = False
        if self._ws:
            self._ws.close()

    def _on_open(self, ws):
        logger.info("D201 connected")
        # 为每个 watcher 发送订阅
        with self._lock:
            for w in self._watchers:
                cmd = {
                    "type": "price",
                    "code": w.code,
                    "enable": 1,
                    "direction": w.direction,
                    "priceCent": w.price_li,
                }
                ws.send(json.dumps([cmd]))
                logger.info("subscribed: %s price=%d", w.code, w.price_li)

    def _on_message(self, ws, text: str):
        try:
            msg = json.loads(text)
        except json.JSONDecodeError:
            return

        ts = msg.get("ts", 0)
        items = msg.get("list", [])
        if not items:
            return

        # 按 watcher 分组 batch
        batches: dict[str, list[PricePacket]] = {}
        for item in items:
            if item.get("type") != "price":
                continue
            data = item.get("data", {})
            pkt = self._parse_price_data(data, ts)
            if pkt:
                batches.setdefault(pkt.code, []).append(pkt)

        with self._lock:
            for w in self._watchers:
                pkts = batches.get(w.code, [])
                if pkts:
                    w.feed_batch(pkts, ts)

    @staticmethod
    def _parse_price_data(data: dict, ts: int) -> Optional[PricePacket]:
        code = data.get("code", "")
        if not code:
            return None
        is_first = data.get("isFirst", 0)
        records_data = data.get("records", [])

        pkt = PricePacket(
            code=code,
            direction=data.get("direction", 0),
            price_li=data.get("price", 0),
            is_first=is_first,
            cur_count=data.get("curCount", len(records_data)),
        )

        if is_first == 1:
            pkt.total_volume = data.get("totalVolume", 0)
            pkt.total_amount = data.get("totalAmount", 0)
            pkt.total_count = data.get("totalCount", 0)
            pkt.seq = data.get("seq", 0)

        pkt.records = [
            PriceRecord(
                volume=r.get("volume", 0),
                id=r.get("id", 0),
                big_order=r.get("bigOrder", 0),
                status=r.get("status", 0),
            )
            for r in records_data
        ]
        pkt.timestamp = ts
        return pkt

    def _on_error(self, ws, error):
        logger.error("D201 WS error: %s", error)

    def _on_close(self, ws, code, msg):
        logger.info("D201 disconnected: %s %s", code, msg)
        self._running = False

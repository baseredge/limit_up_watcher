"""涨停观察器核心算法 — 移植自 limit_up_sim/observer.cpp"""
import threading
import logging
from typing import Callable, Optional

from .types import (
    QueueRecord, TickAggregate, MyQueueEntry, MyQueueSide, PricePacket
)
from .status_codes import (
    is_new_order, is_partial_fill, is_cancel
)

logger = logging.getLogger(__name__)


class LimitUpWatcher:
    """单股票单价位排队观察器，不关心数据来源（WS/回放）"""

    def __init__(self, code: str = "", price_li: int = 0, direction: int = 0):
        self._code = code
        self._price_li = price_li      # 厘
        self._direction = direction    # 0=买盘, 1=卖盘
        self._lock = threading.RLock()  # 可重入: 回调中访问属性也需锁

        # ── 队列 ──
        self._records: list[QueueRecord] = []
        self._used = 0

        # ── 聚合 ──
        self._first = TickAggregate()
        self._current = TickAggregate()
        self._new_add = TickAggregate()
        self._cancelled = TickAggregate()
        self._executed = TickAggregate()
        self._net_change_amt = 0

        # ── 我的排队 ──
        self._my_orders: list[MyQueueEntry] = []

        # ── 衍生信号 ──
        self._timestamp = 0
        self._first_timestamp = 0
        self._packet_seq = 0
        self._is_first = 0
        self._inflow_streak = 0
        self._outflow_streak = 0
        self._post_auction = False
        self._already_vented = False
        self._limit_up_gone = False
        self._limit_up_may_gone = False
        self._single_max_exec_amt = 0
        self._total_exec_amt = 0

        # ── 回调 ──
        self._callback_snapshot: list[Callable] = []
        self._callback_tick: list[Callable] = []
        self._callback_position: list[Callable] = []
        self._callback_fill: list[Callable] = []
        self._callback_limit_gone: list[Callable] = []

    # ═══════════════════════════════════════════
    # 公共属性 (线程安全)
    # ═══════════════════════════════════════════
    @property
    def code(self) -> str:
        return self._code

    @property
    def price_li(self) -> int:
        return self._price_li

    @property
    def price_fen(self) -> int:
        return self._price_li // 10

    @property
    def price_yuan(self) -> float:
        return self._price_li / 1000.0

    @property
    def direction(self) -> int:
        return self._direction

    @property
    def records(self) -> list:
        with self._lock:
            return self._records[:self._used].copy()

    @property
    def current(self) -> TickAggregate:
        with self._lock:
            return self._replace_agg(self._current)

    @property
    def new_add(self) -> TickAggregate:
        with self._lock:
            return self._replace_agg(self._new_add)

    @property
    def cancelled(self) -> TickAggregate:
        with self._lock:
            return self._replace_agg(self._cancelled)

    @property
    def executed(self) -> TickAggregate:
        with self._lock:
            return self._replace_agg(self._executed)

    @property
    def first(self) -> TickAggregate:
        with self._lock:
            return self._replace_agg(self._first)

    @property
    def my_orders(self) -> list:
        with self._lock:
            return self._my_orders.copy()

    @property
    def limit_up_gone(self) -> bool:
        return self._limit_up_gone

    @property
    def limit_up_may_gone(self) -> bool:
        return self._limit_up_may_gone

    @property
    def inflow_streak(self) -> int:
        return self._inflow_streak

    @property
    def outflow_streak(self) -> int:
        return self._outflow_streak

    @property
    def timestamp(self) -> int:
        return self._timestamp

    @property
    def post_auction(self) -> bool:
        return self._post_auction

    @property
    def net_change_amt(self) -> int:
        return self._net_change_amt

    # ═══════════════════════════════════════════
    # 回调注册
    # ═══════════════════════════════════════════
    def on_snapshot(self, cb: Callable):
        self._callback_snapshot.append(cb)
        return cb

    def on_tick(self, cb: Callable):
        self._callback_tick.append(cb)
        return cb

    def on_position_update(self, cb: Callable):
        self._callback_position.append(cb)
        return cb

    def on_fill(self, cb: Callable):
        self._callback_fill.append(cb)
        return cb

    def on_limit_gone(self, cb: Callable):
        self._callback_limit_gone.append(cb)
        return cb

    def _emit(self, cbs: list, *args):
        for cb in cbs:
            try:
                cb(self, *args)
            except Exception:
                logger.exception("callback error")

    # ═══════════════════════════════════════════
    # 我的操作
    # ═══════════════════════════════════════════
    def queue(self, hand_count: int, order_id: int = 0) -> int:
        """已排板（用户调用）。返回 entry_index"""
        with self._lock:
            entry = MyQueueEntry(
                timestamp=self._timestamp,
                order_id=order_id,
                hand_count=hand_count,
                status=1,  # 已排队未匹配
            )
            idx = len(self._my_orders)
            self._my_orders.append(entry)
            return idx

    def cancel(self, entry_index: int):
        """已撤单（用户调用）"""
        with self._lock:
            if 0 <= entry_index < len(self._my_orders):
                self._my_orders[entry_index].status = 3  # 已撤

    # ═══════════════════════════════════════════
    # 喂入数据 (由 DataSource 调用)
    # ═══════════════════════════════════════════
    def feed(self, pkt: PricePacket):
        """处理一个 PricePacket"""
        with self._lock:
            self._process_packet(pkt)

    def feed_batch(self, packets: list, timestamp: int = 0):
        """处理同一 timestamp 的多个 PricePacket 并 finish_tick"""
        with self._lock:
            if timestamp:
                self._timestamp = timestamp
            had_data = bool(packets)

            for pkt in packets:
                self._timestamp = timestamp or pkt.timestamp or self._timestamp
                self._process_packet(pkt)

            if had_data:
                self._finish_tick()

    # ═══════════════════════════════════════════
    # 内部: 包处理 (移植自 observer.cpp:process_packet)
    # ═══════════════════════════════════════════
    def _process_packet(self, pkt: PricePacket):
        self._code = pkt.code or self._code
        self._price_li = pkt.price_li or self._price_li
        self._is_first = pkt.is_first
        self._direction = pkt.direction

        # 竞价判断 (E语言: 已过竞价 = timestamp >= 93000)
        ts = self._timestamp
        if not self._post_auction and ts > 0:
            secs = (ts // 1000 + 8 * 3600) % 86400
            if secs >= 34200:  # 9*3600 + 30*60 = 34200 → 9:30:00
                self._post_auction = True

        if pkt.is_first == 1:
            self._process_snapshot(pkt)
            self._emit(self._callback_snapshot)
        else:
            self._process_incremental(pkt)

    def _process_snapshot(self, pkt: PricePacket):
        """isFirst==1 全量快照"""
        price_fen = self.price_fen
        self._packet_seq += 1

        self._current = TickAggregate(
            count=pkt.total_count,
            volume=pkt.total_volume,
            amount=self._calc_amount(price_fen, pkt.total_volume),
        )
        self._first = TickAggregate(
            count=self._current.count,
            volume=self._current.volume,
            amount=self._current.amount,
        )
        self._new_add = TickAggregate(
            count=self._current.count,
            volume=self._current.volume,
            amount=self._current.amount,
        )

        total = pkt.total_count
        if total > len(self._records):
            self._records = [QueueRecord() for _ in range(total)]
        self._used = total

        seq = pkt.seq
        for i, rec in enumerate(pkt.records):
            pos = seq + i
            if 0 <= pos < total:
                self._records[pos] = QueueRecord(
                    volume=rec.volume,
                    id=rec.id,
                    big_order=rec.big_order,
                )

    def _process_incremental(self, pkt: PricePacket):
        """isFirst==0 增量更新"""
        if pkt.cur_count == 0:
            return

        price_fen = self.price_fen
        self._packet_seq += 1

        for rec in pkt.records:
            status = rec.status

            if is_new_order(status):
                self._handle_new_record(rec)

                # 尝试匹配我的未匹配条目
                for entry in self._my_orders:
                    if entry.found_id == 0:
                        if entry.hand_count == -1 or rec.volume == entry.hand_count:
                            entry.found_id = rec.id
                            entry.status = 2  # 已匹配
                            entry.front.volume = self._current.volume - rec.volume
                            entry.front.amount_when_queued = self._calc_amount(
                                price_fen, entry.front.volume
                            )
                    elif entry.found_id != 0:
                        # 已匹配的: 新委托在我后面
                        entry.back.volume += rec.volume
                        entry.back.count += 1
            else:
                # 变动: 二分查找 by id
                idx = self._binary_search_id(rec.id)
                if idx < 0:
                    continue

                old_vol = self._records[idx].volume
                reduce_vol = old_vol - rec.volume
                if reduce_vol == 0:
                    continue

                self._records[idx].volume = rec.volume
                self._current.volume -= reduce_vol
                if rec.volume == 0:
                    self._current.count -= 1

                if is_partial_fill(status):
                    self._executed.count += 1
                    self._executed.volume += reduce_vol
                elif is_cancel(status):
                    self._cancelled.count += 1
                    self._cancelled.volume += reduce_vol

                # 更新我的位置
                for entry in self._my_orders:
                    if entry.found_id == 0:
                        continue
                    if rec.id == entry.found_id:
                        entry.status = 100  # 成交
                    elif rec.id > entry.found_id:
                        entry.back.volume -= reduce_vol
                    else:
                        entry.front.volume -= reduce_vol

    def _handle_new_record(self, rec):
        """新增委托 (移植自 handle_new_record)"""
        price_fen = self.price_fen
        if self._used >= len(self._records):
            self._records.extend([QueueRecord() for _ in range(1000)])

        self._records[self._used] = QueueRecord(
            volume=rec.volume,
            id=rec.id,
            big_order=rec.big_order,
        )
        self._used += 1
        self._current.count += 1
        self._current.volume += rec.volume
        self._new_add.count += 1
        self._new_add.volume += rec.volume

    # ═══════════════════════════════════════════
    # 内部: finish_tick (移植自 observer.cpp:finish_tick)
    # ═══════════════════════════════════════════
    def _finish_tick(self):
        price_fen = self.price_fen

        # 计算金额
        self._current.amount = self._calc_amount(price_fen, self._current.volume)
        self._new_add.amount = self._calc_amount(price_fen, self._new_add.volume)
        self._cancelled.amount = self._calc_amount(price_fen, self._cancelled.volume)
        self._executed.amount = self._calc_amount(price_fen, self._executed.volume)
        self._net_change_amt = self._new_add.amount - self._cancelled.amount - self._executed.amount

        # 连续流入/流出
        if self._new_add.count == 0:
            self._outflow_streak += 1
            self._inflow_streak = 0
        else:
            self._inflow_streak += 1
            self._outflow_streak = 0

        had_records = self._used > 0

        # 竞价后累计成交额
        if self._post_auction:
            self._total_exec_amt += self._executed.amount
            if self._executed.amount > self._single_max_exec_amt:
                self._single_max_exec_amt = self._executed.amount

        # 已发泄过 (E语言: 成交额单次最大>=500 && 成交额>=3000)
        if not self._already_vented:
            self._already_vented = (
                self._single_max_exec_amt >= 500 and self._total_exec_amt >= 3000
            )

        # 价位将消失 / 价位消失
        self._limit_up_may_gone = (
            had_records
            and (self._cancelled.amount + self._executed.amount) * 5 > self._current.amount
        )
        self._limit_up_gone = had_records and self._current.count == 0

        # 价位消失时重置
        if self._limit_up_gone or (self._is_first == 0 and not had_records):
            self._packet_seq = 0
            self._used = 0
            self._current = TickAggregate()
            if self._limit_up_gone:
                self._emit(self._callback_limit_gone)

        # 更新我的位置金额
        self._update_my_position_amounts(price_fen)

        # 成交检测
        for entry in self._my_orders:
            if entry.status == 100:
                self._emit(self._callback_fill, entry)

        # tick 回调
        self._emit(self._callback_tick)

        # 重置增量
        self._new_add = TickAggregate()
        self._cancelled = TickAggregate()
        self._executed = TickAggregate()

    def _update_my_position_amounts(self, price_fen: int):
        for entry in self._my_orders:
            if entry.found_id == 0:
                continue
            prev_front_amt = entry.front.amount
            entry.front.amount = self._calc_amount(price_fen, entry.front.volume)
            entry.back.amount = self._calc_amount(price_fen, entry.back.volume)
            entry.front.last_reduction = prev_front_amt - entry.front.amount
            entry.front.prev_reduction = entry.front.last_reduction  # retain history
            entry.queue_elapsed_ms = self._timestamp - entry.timestamp

    # ═══════════════════════════════════════════
    # 工具
    # ═══════════════════════════════════════════
    @staticmethod
    def _calc_amount(price_cents: int, volume_hands: int) -> int:
        """price(分) × volume(手) / 10000 = 万元"""
        return (price_cents * volume_hands) // 10000

    def _binary_search_id(self, target_id: int) -> int:
        """二分查找 id 在 records[0..used-1] 中的位置"""
        lo, hi = 0, self._used - 1
        while lo <= hi:
            mid = (lo + hi) // 2
            mid_id = self._records[mid].id
            if mid_id == target_id:
                return mid
            if mid_id < target_id:
                lo = mid + 1
            else:
                hi = mid - 1
        return -1

    @staticmethod
    def _replace_agg(src: TickAggregate) -> TickAggregate:
        return TickAggregate(count=src.count, volume=src.volume, amount=src.amount)

    # ═══════════════════════════════════════════
    # 重置
    # ═══════════════════════════════════════════
    def reset(self):
        """重置所有状态"""
        with self._lock:
            self._records.clear()
            self._used = 0
            self._first = self._current = self._new_add = TickAggregate()
            self._cancelled = self._executed = TickAggregate()
            self._net_change_amt = 0
            self._my_orders.clear()
            self._timestamp = 0
            self._first_timestamp = 0
            self._packet_seq = 0
            self._is_first = 0
            self._inflow_streak = 0
            self._outflow_streak = 0
            self._post_auction = False
            self._already_vented = False
            self._limit_up_gone = False
            self._limit_up_may_gone = False
            self._single_max_exec_amt = 0
            self._total_exec_amt = 0

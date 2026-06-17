"""涨停观察器核心算法 — 移植自 limit_up_sim/observer.cpp"""
import copy
import threading
import logging
from typing import Callable, List, Tuple

from .types import (
    QueueRecord, TickAggregate, MyQueueEntry, MyQueueSide, PricePacket
)
from .status_codes import (
    is_new_order, is_partial_fill, is_cancel
)

logger = logging.getLogger(__name__)

# 北京时区相对 UTC 的偏移秒数 (ts 为 epoch ms, 需换算到北京当天秒数判断竞价)
_UTC_OFFSET_CN = 8 * 3600
# 集合竞价结束时刻 9:30:00 对应当天秒数
_AUCTION_END_SECS = 9 * 3600 + 30 * 60
# 涨停"将消失"预警阈值: (撤+成交) * 该值 > 当前封单
_MAY_GONE_RATIO = 5


class LimitUpWatcher:
    """单股票单价位排队观察器，不关心数据来源（WS/回放）"""

    def __init__(self, code: str = "", price_li: int = 0, direction: int = 0):
        self._code = code
        self._price_li = price_li      # 厘
        self._direction = direction    # 0=买盘, 1=卖盘
        self._lock = threading.RLock()  # 可重入: 回调中访问属性也需锁

        # ── 队列 ──
        self._records: List[QueueRecord] = []

        # ── 聚合 ──
        self._first = TickAggregate()
        self._current = TickAggregate()
        self._new_add = TickAggregate()
        self._cancelled = TickAggregate()
        self._executed = TickAggregate()
        self._net_change_amt = 0

        # ── 我的排队 ──
        self._my_orders: List[MyQueueEntry] = []

        # ── 衍生信号 ──
        self._timestamp = 0
        self._packet_seq = 0
        self._is_first = 0
        self._inflow_streak = 0
        self._outflow_streak = 0
        self._post_auction = False
        self._limit_up_gone = False
        self._limit_up_may_gone = False

        # ── 回调 ──
        self._callback_snapshot: List[Callable] = []
        self._callback_tick: List[Callable] = []
        self._callback_position: List[Callable] = []
        self._callback_fill: List[Callable] = []
        self._callback_limit_gone: List[Callable] = []

    # ═══════════════════════════════════════════
    # 公共属性 (线程安全)
    # ═══════════════════════════════════════════
    @property
    def code(self) -> str:
        with self._lock:
            return self._code

    @property
    def price_li(self) -> int:
        with self._lock:
            return self._price_li

    @property
    def price_fen(self) -> int:
        with self._lock:
            return self._price_li // 10

    @property
    def price_yuan(self) -> float:
        with self._lock:
            return self._price_li / 1000.0

    @property
    def direction(self) -> int:
        with self._lock:
            return self._direction

    @property
    def records(self) -> list:
        with self._lock:
            return copy.deepcopy(self._records)

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
            return copy.deepcopy(self._my_orders)

    @property
    def limit_up_gone(self) -> bool:
        with self._lock:
            return self._limit_up_gone

    @property
    def limit_up_may_gone(self) -> bool:
        with self._lock:
            return self._limit_up_may_gone

    @property
    def inflow_streak(self) -> int:
        with self._lock:
            return self._inflow_streak

    @property
    def outflow_streak(self) -> int:
        with self._lock:
            return self._outflow_streak

    @property
    def timestamp(self) -> int:
        with self._lock:
            return self._timestamp

    @property
    def packet_seq(self) -> int:
        with self._lock:
            return self._packet_seq

    @property
    def post_auction(self) -> bool:
        with self._lock:
            return self._post_auction

    @property
    def net_change_amt(self) -> int:
        with self._lock:
            return self._net_change_amt

    # ═══════════════════════════════════════════
    # 回调注册 (加锁, 避免与 _emit 迭代竞态)
    # ═══════════════════════════════════════════
    def on_snapshot(self, cb: Callable):
        with self._lock:
            self._callback_snapshot.append(cb)
        return cb

    def on_tick(self, cb: Callable):
        with self._lock:
            self._callback_tick.append(cb)
        return cb

    def on_position_update(self, cb: Callable):
        with self._lock:
            self._callback_position.append(cb)
        return cb

    def on_fill(self, cb: Callable):
        with self._lock:
            self._callback_fill.append(cb)
        return cb

    def on_limit_gone(self, cb: Callable):
        with self._lock:
            self._callback_limit_gone.append(cb)
        return cb

    def _emit(self, cbs: list, *args):
        # 取快照迭代, 避免注册新回调时并发修改
        for cb in list(cbs):
            try:
                cb(self, *args)
            except Exception:
                logger.exception("callback error")

    def _emit_events(self, events: List[Tuple[list, tuple]]):
        for cb_list, args in events:
            self._emit(cb_list, *args)

    # ═══════════════════════════════════════════
    # 我的操作
    # ═══════════════════════════════════════════
    def queue(self, hand_count: int, order_id: int = 0) -> int:
        """已排板（用户调用）。返回 entry_index

        hand_count=-1 表示匹配任意手数。
        """
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
        """已撤单（用户调用）。status 置为 3=已撤"""
        with self._lock:
            if 0 <= entry_index < len(self._my_orders):
                self._my_orders[entry_index].status = 3  # 已撤

    # ═══════════════════════════════════════════
    # 喂入数据 (由 DataSource 调用)
    # ═══════════════════════════════════════════
    def feed(self, pkt: PricePacket):
        """处理一个 PricePacket (不触发 finish_tick, 用 feed_batch 触发 tick)"""
        events: List[Tuple[list, tuple]] = []
        with self._lock:
            self._process_packet(pkt, events)
        self._emit_events(events)

    def feed_batch(self, packets: list, timestamp: int = 0):
        """处理同一 timestamp 的多个 PricePacket 并 finish_tick"""
        events: List[Tuple[list, tuple]] = []
        with self._lock:
            for pkt in packets:
                self._timestamp = timestamp or pkt.timestamp or self._timestamp
                self._process_packet(pkt, events)
            if packets:
                self._finish_tick(events)
        # 回调在锁外执行, 避免用户回调阻塞喂数据线程
        self._emit_events(events)
        # 回调执行完毕后再重置增量统计 (保证 on_tick 回调内仍可读取本次增量)
        with self._lock:
            self._new_add = TickAggregate()
            self._cancelled = TickAggregate()
            self._executed = TickAggregate()

    # ═══════════════════════════════════════════
    # 内部: 包处理 (移植自 observer.cpp:process_packet)
    # ═══════════════════════════════════════════
    def _process_packet(self, pkt: PricePacket, events: list):
        self._code = pkt.code or self._code

        # 价位/方向过滤: 只处理本 watcher 订阅的价位与方向
        # (防止 ws_source 按 code 分组时把同 code 不同价位/方向的包喂错)
        if pkt.price_li and self._price_li and pkt.price_li != self._price_li:
            return
        if not self._price_li:
            self._price_li = pkt.price_li
        if pkt.direction != self._direction:
            return

        self._is_first = pkt.is_first

        # 竞价判断 (已过竞价 = 北京时间 >= 9:30:00)
        ts = self._timestamp
        if not self._post_auction and ts > 0:
            secs = (ts // 1000 + _UTC_OFFSET_CN) % 86400
            if secs >= _AUCTION_END_SECS:
                self._post_auction = True

        if pkt.is_first == 1:
            self._process_snapshot(pkt)
            events.append((self._callback_snapshot, ()))
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
        # 快照时重置增量计数器 (原实现遗漏 cancelled/executed)
        self._cancelled = TickAggregate()
        self._executed = TickAggregate()

        total = pkt.total_count
        seq = pkt.seq
        # 直接构建定长列表, 不再维护 _used 与占位空对象
        self._records = [QueueRecord() for _ in range(total)]
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
                    else:
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
        # 校验 id 单调递增 (二分查找的前提), 乱序则告警
        if self._records and rec.id <= self._records[-1].id:
            logger.warning(
                "委托 id 非递增: new=%d last=%d, 二分查找可能失效",
                rec.id, self._records[-1].id,
            )
        self._records.append(QueueRecord(
            volume=rec.volume,
            id=rec.id,
            big_order=rec.big_order,
        ))
        self._current.count += 1
        self._current.volume += rec.volume
        self._new_add.count += 1
        self._new_add.volume += rec.volume

    # ═══════════════════════════════════════════
    # 内部: finish_tick (移植自 observer.cpp:finish_tick)
    # ═══════════════════════════════════════════
    def _finish_tick(self, events: list):
        price_fen = self.price_fen

        # 计算金额
        self._current.amount = self._calc_amount(price_fen, self._current.volume)
        self._new_add.amount = self._calc_amount(price_fen, self._new_add.volume)
        self._cancelled.amount = self._calc_amount(price_fen, self._cancelled.volume)
        self._executed.amount = self._calc_amount(price_fen, self._executed.volume)
        self._net_change_amt = (
            self._new_add.amount - self._cancelled.amount - self._executed.amount
        )

        # 连续流入/流出
        if self._new_add.count == 0:
            self._outflow_streak += 1
            self._inflow_streak = 0
        else:
            self._inflow_streak += 1
            self._outflow_streak = 0

        had_records = len(self._records) > 0

        # 价位将消失 / 价位消失
        self._limit_up_may_gone = (
            had_records
            and (self._cancelled.amount + self._executed.amount) * _MAY_GONE_RATIO
            > self._current.amount
        )
        self._limit_up_gone = had_records and self._current.count == 0

        # 价位消失时重置
        if self._limit_up_gone or (self._is_first == 0 and not had_records):
            self._packet_seq = 0
            self._records.clear()
            self._current = TickAggregate()
            if self._limit_up_gone:
                events.append((self._callback_limit_gone, ()))

        # 更新我的位置金额
        self._update_my_position_amounts(price_fen)

        # 触发 position 回调 (传深拷贝, 避免锁外回调时内部状态被改)
        for entry in self._my_orders:
            if entry.found_id != 0:
                events.append((self._callback_position, (copy.deepcopy(entry),)))

        # 成交检测
        for entry in self._my_orders:
            if entry.status == 100:
                events.append((self._callback_fill, (copy.deepcopy(entry),)))

        # tick 回调
        events.append((self._callback_tick, ()))

        # 增量重置移到 feed_batch 的回调执行之后, 保证 on_tick 内可读取本次增量

    def _update_my_position_amounts(self, price_fen: int):
        for entry in self._my_orders:
            if entry.found_id == 0:
                continue
            prev_front_amt = entry.front.amount
            entry.front.amount = self._calc_amount(price_fen, entry.front.volume)
            entry.back.amount = self._calc_amount(price_fen, entry.back.volume)
            # prev_reduction 保留上一次的 last_reduction (原实现直接覆盖, 历史丢失)
            entry.front.prev_reduction = entry.front.last_reduction
            entry.front.last_reduction = prev_front_amt - entry.front.amount
            entry.queue_elapsed_ms = self._timestamp - entry.timestamp

    # ═══════════════════════════════════════════
    # 工具
    # ═══════════════════════════════════════════
    @staticmethod
    def _calc_amount(price_cents: int, volume_hands: int) -> int:
        """price(分) × volume(手) / 10000 = 万元"""
        return (price_cents * volume_hands) // 10000

    def _binary_search_id(self, target_id: int) -> int:
        """二分查找 id 在 records 中的位置 (前提: records 按 id 升序)"""
        lo, hi = 0, len(self._records) - 1
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
            self._first = self._current = self._new_add = TickAggregate()
            self._cancelled = self._executed = TickAggregate()
            self._net_change_amt = 0
            self._my_orders.clear()
            self._timestamp = 0
            self._packet_seq = 0
            self._is_first = 0
            self._inflow_streak = 0
            self._outflow_streak = 0
            self._post_auction = False
            self._limit_up_gone = False
            self._limit_up_may_gone = False

"""LimitUpWatcher 核心逻辑单元测试"""
import unittest

from limit_up_watcher import LimitUpWatcher, PricePacket, PriceRecord

PRICE_LI = 7950   # 7.95 元 = 7950 厘
PRICE_FEN = 795   # 795 分
DIRECTION = 0     # 买盘


def rec(volume: int, id: int, status: int = 0, big_order: int = 0) -> PriceRecord:
    return PriceRecord(volume=volume, id=id, status=status, big_order=big_order)


def make_snapshot(records, code="SZ002177", price_li=PRICE_LI, direction=DIRECTION,
                  seq=0, total_count=None, total_volume=None):
    records = list(records)
    return PricePacket(
        code=code, direction=direction, price_li=price_li, is_first=1,
        total_count=total_count if total_count is not None else len(records),
        total_volume=total_volume if total_volume is not None else sum(r.volume for r in records),
        total_amount=0, seq=seq, cur_count=len(records), records=records,
    )


def make_incremental(records, code="SZ002177", price_li=PRICE_LI, direction=DIRECTION):
    records = list(records)
    return PricePacket(
        code=code, direction=direction, price_li=price_li, is_first=0,
        cur_count=len(records), records=records,
    )


class StatusCodesTest(unittest.TestCase):
    """状态码分类 (#status_codes)"""

    def test_new_order(self):
        from limit_up_watcher import is_new_order
        for s in (4, 12, 64, 192):
            self.assertTrue(is_new_order(s), f"{s} 应为新增委托")
        self.assertFalse(is_new_order(16))
        self.assertFalse(is_new_order(32))

    def test_partial_fill(self):
        from limit_up_watcher import is_partial_fill
        for s in (1, 9, 16, 128, 144):
            self.assertTrue(is_partial_fill(s), f"{s} 应为部分成交")
        self.assertFalse(is_partial_fill(64))

    def test_cancel(self):
        from limit_up_watcher import is_cancel
        for s in (2, 32):
            self.assertTrue(is_cancel(s), f"{s} 应为撤单")
        self.assertFalse(is_cancel(64))


class SnapshotTest(unittest.TestCase):
    """快照处理"""

    def test_snapshot_sets_current_and_first(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1), rec(1000, 2)])], timestamp=0)

        self.assertEqual(w.current.count, 2)
        self.assertEqual(w.current.volume, 2000)
        # 795 分 * 2000 手 // 10000 = 159 万
        self.assertEqual(w.current.amount, 159)
        self.assertEqual(w.first.amount, 159)
        self.assertEqual(w.packet_seq, 1)

    def test_snapshot_resets_incremental(self):
        """#7: 快照应重置 cancelled/executed 增量计数器

        用 feed() (不触发 finish_tick) 喂入, 使增量累积, 再喂快照验证重置。
        """
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed(make_snapshot([rec(1000, 1), rec(1000, 2)]))
        w.feed(make_incremental([rec(0, 1, status=32)]))
        self.assertEqual(w.cancelled.count, 1)
        # 再次快照应清零 cancelled/executed
        w.feed(make_snapshot([rec(1000, 1), rec(1000, 2)]))
        self.assertEqual(w.cancelled.count, 0)
        self.assertEqual(w.executed.count, 0)


class IncrementalTest(unittest.TestCase):
    """增量更新"""

    def setUp(self):
        self.w = LimitUpWatcher("SZ002177", PRICE_LI)
        self.w.feed_batch([make_snapshot([rec(1000, 1), rec(1000, 2)])], timestamp=0)

    def test_new_order_appends(self):
        # 增量统计在 on_tick 回调内有效 (finish_tick 末尾会清零)
        seen = []

        @self.w.on_tick
        def _cb(w):
            seen.append((w.current.count, w.current.volume, w.new_add.volume))

        self.w.feed_batch([make_incremental([rec(500, 3, status=64)])], timestamp=0)
        self.assertEqual(seen[-1], (3, 2500, 500))

    def test_cancel_reduces(self):
        seen = []

        @self.w.on_tick
        def _cb(w):
            seen.append((w.current.count, w.current.volume, w.cancelled.volume))

        self.w.feed_batch([make_incremental([rec(0, 1, status=32)])], timestamp=0)
        self.assertEqual(seen[-1], (1, 1000, 1000))

    def test_partial_fill_reduces(self):
        seen = []

        @self.w.on_tick
        def _cb(w):
            seen.append((w.current.volume, w.executed.volume, w.cancelled.volume))

        # id=1 成交 400 手后剩 600
        self.w.feed_batch([make_incremental([rec(600, 1, status=16)])], timestamp=0)
        self.assertEqual(seen[-1], (1600, 400, 0))

    def test_inflow_outflow_streak(self):
        # setUp 快照已让 inflow_streak=1
        self.w.feed_batch([make_incremental([rec(500, 3, status=64)])], timestamp=0)
        self.assertEqual(self.w.inflow_streak, 2)
        self.assertEqual(self.w.outflow_streak, 0)
        # 无新增 → outflow
        self.w.feed_batch([make_incremental([rec(0, 1, status=32)])], timestamp=0)
        self.assertEqual(self.w.inflow_streak, 0)
        self.assertEqual(self.w.outflow_streak, 1)


class BinarySearchTest(unittest.TestCase):
    """二分查找边界"""

    def test_find_existing(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(100, 10), rec(200, 20), rec(300, 30)])], timestamp=0)
        self.assertEqual(w._binary_search_id(10), 0)
        self.assertEqual(w._binary_search_id(20), 1)
        self.assertEqual(w._binary_search_id(30), 2)

    def test_find_missing(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(100, 10), rec(200, 20)])], timestamp=0)
        self.assertEqual(w._binary_search_id(15), -1)
        self.assertEqual(w._binary_search_id(5), -1)
        self.assertEqual(w._binary_search_id(99), -1)


class MyOrdersTest(unittest.TestCase):
    """我的排队匹配与位置追踪"""

    def test_match_and_track_position(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1), rec(1000, 2)])], timestamp=1000)
        idx = w.queue(hand_count=500)
        # 新增 500 手委托 → 匹配
        w.feed_batch([make_incremental([rec(500, 3, status=64)])], timestamp=2000)
        orders = w.my_orders
        self.assertEqual(orders[idx].found_id, 3)
        self.assertEqual(orders[idx].status, 2)
        # front = 当前总(2500) - 我的(500) = 2000
        self.assertEqual(orders[idx].front.volume, 2000)

        # 后续新增 → 在我后面
        w.feed_batch([make_incremental([rec(200, 4, status=64)])], timestamp=3000)
        orders = w.my_orders
        self.assertEqual(orders[idx].back.volume, 200)

        # 前面有人撤单 (id=1, 1000→0)
        w.feed_batch([make_incremental([rec(0, 1, status=32)])], timestamp=4000)
        orders = w.my_orders
        self.assertEqual(orders[idx].front.volume, 1000)
        # last_reduction = 之前 front 金额 - 现在 front 金额
        # 之前 front=2000 手 → 795*2000//10000=159 万
        # 现在 front=1000 手 → 795*1000//10000=79 万
        self.assertEqual(orders[idx].front.last_reduction, 159 - 79)

    def test_fill_detection(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        w.queue(hand_count=500)
        w.feed_batch([make_incremental([rec(500, 2, status=64)])], timestamp=0)
        # 我的委托 id=2 被吃
        w.feed_batch([make_incremental([rec(0, 2, status=16)])], timestamp=0)
        orders = w.my_orders
        self.assertEqual(orders[0].status, 100)

    def test_cancel_marks_status_3(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        idx = w.queue(hand_count=100)
        w.cancel(idx)
        self.assertEqual(w.my_orders[idx].status, 3)


class PrevReductionTest(unittest.TestCase):
    """#9: prev_reduction 应保留上一次的 last_reduction"""

    def test_prev_reduction_retains_history(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1), rec(1000, 2)])], timestamp=0)
        w.queue(hand_count=500)

        # tick1: 匹配, front 从 0 跳到 159 万, last_reduction=-159, prev_reduction=0
        w.feed_batch([make_incremental([rec(500, 3, status=64)])], timestamp=0)
        self.assertEqual(w.my_orders[0].front.prev_reduction, 0)
        tick1_last = w.my_orders[0].front.last_reduction

        # tick2: 前面撤 1000 手 (id=1), front 159→79, last_reduction=80, prev_reduction=tick1_last
        w.feed_batch([make_incremental([rec(0, 1, status=32)])], timestamp=0)
        self.assertEqual(w.my_orders[0].front.prev_reduction, tick1_last)
        self.assertEqual(w.my_orders[0].front.last_reduction, 159 - 79)


class FilterTest(unittest.TestCase):
    """#4: 价位/方向过滤"""

    def test_ignore_wrong_direction(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI, direction=0)
        # 卖盘包应被忽略
        pkt = make_snapshot([rec(1000, 1)], direction=1)
        w.feed_batch([pkt], timestamp=0)
        self.assertEqual(w.current.count, 0)

    def test_ignore_wrong_price(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI, direction=0)
        # 不同价位包应被忽略
        pkt = make_snapshot([rec(1000, 1)], price_li=8000)
        w.feed_batch([pkt], timestamp=0)
        self.assertEqual(w.current.count, 0)

    def test_accept_correct_packet(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI, direction=0)
        pkt = make_snapshot([rec(1000, 1)], price_li=PRICE_LI, direction=0)
        w.feed_batch([pkt], timestamp=0)
        self.assertEqual(w.current.count, 1)


class DeepCopyTest(unittest.TestCase):
    """#8: my_orders/records 返回深拷贝, 修改不影响内部"""

    def test_my_orders_isolated(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        w.queue(hand_count=100)
        orders = w.my_orders
        orders[0].status = 999
        # 内部不受影响
        self.assertEqual(w.my_orders[0].status, 1)

    def test_records_isolated(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        records = w.records
        records[0].volume = 999999
        self.assertEqual(w.records[0].volume, 1000)


class CallbackTest(unittest.TestCase):
    """#1/#2: packet_seq 可用, on_position_update 被触发"""

    def test_position_callback_fired(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        fired = []

        @w.on_position_update
        def _cb(watcher, entry):
            fired.append(entry.found_id)

        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        w.queue(hand_count=500)
        w.feed_batch([make_incremental([rec(500, 2, status=64)])], timestamp=0)
        self.assertEqual(fired, [2])

    def test_tick_callback_fired(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        count = []

        @w.on_tick
        def _cb(watcher):
            count.append(watcher.packet_seq)

        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        self.assertEqual(len(count), 1)


class LimitUpGoneTest(unittest.TestCase):
    """涨停消失重置"""

    def test_limit_up_gone_when_all_cancelled(self):
        w = LimitUpWatcher("SZ002177", PRICE_LI)
        gone = []

        @w.on_limit_gone
        def _cb(watcher):
            gone.append(True)

        w.feed_batch([make_snapshot([rec(1000, 1)])], timestamp=0)
        w.feed_batch([make_incremental([rec(0, 1, status=32)])], timestamp=0)
        self.assertTrue(w.limit_up_gone)
        self.assertEqual(gone, [True])
        # 消失后状态重置
        self.assertEqual(w.current.count, 0)


class CalcAmountTest(unittest.TestCase):
    """金额计算"""

    def test_calc_amount(self):
        # 795 分 * 10000 手 // 10000 = 795 万
        self.assertEqual(LimitUpWatcher._calc_amount(795, 10000), 795)
        # 795 分 * 2000 手 // 10000 = 159 万
        self.assertEqual(LimitUpWatcher._calc_amount(795, 2000), 159)
        # 795 分 * 100 手 // 10000 = 7 万 (整除)
        self.assertEqual(LimitUpWatcher._calc_amount(795, 100), 7)


if __name__ == "__main__":
    unittest.main()

"""涨停观察器数据类型 — 移植自 limit_up_sim/types.hpp + observer.hpp"""

from dataclasses import dataclass, field
from typing import List


@dataclass
class QueueRecord:
    """单笔队列记录 (对应 C++ QueueRecord / 易语言 手数/id/大单 并行数组)"""
    volume: int = 0       # 手
    id: int = 0           # 委托ID (单调递增, 有序)
    big_order: int = 0    # 大单标记


@dataclass
class TickAggregate:
    """聚合统计 (对应 C++ TickAggregate, 金额单位: 万元)"""
    count: int = 0        # 笔数
    volume: int = 0       # 手
    amount: int = 0       # 万元


@dataclass
class MyQueueSide:
    """"我的位置"一侧的排队快照 (对应 C++ MyQueueSide)"""
    volume: int = 0               # 手
    count: int = 0                # 笔数
    amount: int = 0               # 当前金额(万元)
    amount_when_queued: int = 0   # 刚排队时这侧的金额(万元)
    last_reduction: int = 0       # 本次减少金额(万元)
    prev_reduction: int = 0       # 上次减少金额(万元)


@dataclass
class MyQueueEntry:
    """我的排队条目 (对应 C++ MyQueueEntry)"""
    timestamp: int = 0           # 排队时刻 (epoch ms)
    order_id: int = 0            # 我的委托号 (0=未知)
    hand_count: int = 0          # 我的手数 (-1=匹配任意)
    found_id: int = 0            # 在队列中匹配到的 id (0=未匹配)
    status: int = 0              # 0=空, 1=已排队未匹配, 2=排队已匹配, 3=已撤, 100=已成交
    front: MyQueueSide = field(default_factory=MyQueueSide)  # 前面的排队
    back: MyQueueSide = field(default_factory=MyQueueSide)   # 后面的排队
    queue_elapsed_ms: int = 0    # 已排队毫秒


@dataclass
class PricePacket:
    """D201 price 数据包 (从 WebSocket JSON 解析)"""
    code: str = ""
    direction: int = 0     # 0=买, 1=卖
    price_li: int = 0      # 价位 (厘, ÷10=分)
    is_first: int = 0      # 1=首次全量快照, 0=增量更新
    timestamp: int = 0     # 数据时间戳 (epoch ms)
    # isFirst=1 字段
    total_volume: int = 0
    total_amount: int = 0
    total_count: int = 0
    seq: int = 0
    cur_count: int = 0
    # 记录列表
    records: List[PriceRecord] = field(default_factory=list)

    @property
    def price_fen(self) -> int:
        """价位 (分)"""
        return self.price_li // 10

    @property
    def price_yuan(self) -> float:
        """价位 (元)"""
        return self.price_li / 1000.0


@dataclass
class PriceRecord:
    """price 包中的单条记录"""
    volume: int = 0
    id: int = 0
    big_order: int = 0
    status: int = 0

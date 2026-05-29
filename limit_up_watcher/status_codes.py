"""5215 包的 status 状态码 (移植自 types.hpp / 易语言 涨停观察器_状态解释)"""

# ── 新增委托 ──
NEW_ORDER_STATUS = {4, 12, 64, 192}
#  4=委卖, 12=大单委卖, 64=委买, 192=大单委买

# ── 部分成交后剩余 ──
PARTIAL_FILL_STATUS = {1, 9, 16, 128, 144}
#  1=成交后剩(卖), 9=大单成交后剩(卖), 16=成交后剩(买),
#  128=大单成交0手后剩(买), 144=大单成交后剩(买)

# ── 撤单后剩余 ──
CANCEL_STATUS = {2, 32}
#  2=撤卖, 32=撤买


def is_new_order(status: int) -> bool:
    return status in NEW_ORDER_STATUS


def is_partial_fill(status: int) -> bool:
    return status in PARTIAL_FILL_STATUS


def is_cancel(status: int) -> bool:
    return status in CANCEL_STATUS


def status_name(status: int) -> str:
    """返回状态的中文描述 (移植自 涨停观察器_状态解释)"""
    names = {
        1: "[卖]成交后剩",
        2: "[卖]撤卖",
        4: "[卖]委卖",
        9: "[卖]大单成交后剩",
        12: "[卖]大单委卖",
        16: "[买]成交后剩",
        32: "[买]撤买",
        64: "[买]委买",
        128: "[买]大单成交0手后剩",
        144: "[买]大单成交后剩",
        192: "[买]大单委买",
    }
    return names.get(status, f"[状态{status}]")

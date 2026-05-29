# limit_up_watcher

涨停板排队监控 Python 库 — 基于达塔接口 D201 WebSocket 的 `price`（单个价位）数据类型。

## 功能

- 连接本地 `ws://127.0.0.1:8080/d201` 订阅 `price` 数据
- 自动维护涨停价位排队队列（id 有序数组、二分查找）
- 通过手数自动匹配你的委托 ID
- 实时追踪 **我前面/后面** 的排队量（手数、金额、笔数变化）
- 回调机制：快照、tick、位置更新、成交、涨停消失
- 线程安全，支持用户从任意线程调用 `queue()` / `cancel()`

## 安装

```bash
pip install limit_up_watcher
# 或
pip install git+https://github.com/yourname/limit_up_watcher.git
```

依赖: `websocket-client`

## 快速开始

```python
from limit_up_watcher import LimitUpWatcher, WebSocketSource

# 监控 SZ002177 涨停板买1档（7.95元 = 7950厘）
watcher = LimitUpWatcher("SZ002177", price_li=7950)

ws = WebSocketSource("ws://127.0.0.1:8080/d201")
ws.add_watcher(watcher)

@watcher.on_snapshot
def on_snap(w):
    print(f"封单量: {w.first.amount}万 / {w.first.volume}手 / {w.first.count}笔")

@watcher.on_tick
def on_tick(w):
    print(f"+{w.new_add.amount}万 -{w.cancelled.amount}万(撤) -{w.executed.amount}万(成交)")

# 连接（阻塞）
ws.connect()
```

## API

### LimitUpWatcher(code, price_li, direction=0)

| 属性 | 类型 | 说明 |
|------|------|------|
| `current` | TickAggregate | 当前总封单 (count/volume/amount) |
| `new_add` | TickAggregate | 本次新增委托 |
| `cancelled` | TickAggregate | 本次撤销委托 |
| `executed` | TickAggregate | 本次成交委托 |
| `first` | TickAggregate | 首次快照时的封单 |
| `my_orders` | list[MyQueueEntry] | 我的排队列表 |
| `records` | list[QueueRecord] | 当前队列（按 id 有序） |
| `limit_up_gone` | bool | 涨停价位是否已消失 |
| `limit_up_may_gone` | bool | 价位是否即将消失 |
| `inflow_streak` | int | 连续流入 tick 数 |
| `outflow_streak` | int | 连续流出 tick 数 |
| `net_change_amt` | int | 本次净增金额(万元) |

| 方法 | 说明 |
|------|------|
| `queue(hand_count, order_id=0)` | 我已排板，返回 entry_index |
| `cancel(entry_index)` | 我已撤单 |
| `on_snapshot(cb)` | 注册快照回调 |
| `on_tick(cb)` | 注册 tick 回调 |
| `on_position_update(cb)` | 注册位置更新回调 |
| `on_fill(cb)` | 注册成交回调 |
| `on_limit_gone(cb)` | 注册涨停消失回调 |

### MyQueueEntry

| 字段 | 说明 |
|------|------|
| `status` | 0=空, 1=已排队未匹配, 2=排队已匹配, 3=已撤单, 100=已成交 |
| `found_id` | 匹配到的委托 ID（0=未匹配） |
| `front` | MyQueueSide: 我前面的排队（volume/amount/last_reduction） |
| `back` | MyQueueSide: 我后面的排队 |
| `queue_elapsed_ms` | 已排队毫秒 |

## 价格单位

| 字段 | 单位 | 转元 |
|------|------|------|
| `price_li` (构造参数) | 厘 | ÷1000 |
| `price_fen` | 分 | ÷100 |
| `amount` (聚合) | 万元 | 即万元 |
| `volume` (手数) | 手 | ×100=股 |

## 策略示例

```python
@watcher.on_tick
def my_strategy(w):
    for i, entry in enumerate(w.my_orders):
        if entry.status == 2:  # 已匹配
            # 前面减少了很多 → 考虑撤单
            if entry.front.last_reduction > 50:
                w.cancel(i)
                print("前面大额撤单，我也撤")
            
            # 快排到了
            if entry.front.amount < 100:
                print("快排到了！")
```

## License

MIT

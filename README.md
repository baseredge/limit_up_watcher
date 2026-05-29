# limit_up_watcher

**大A打板神器 — 涨停板排队实时监控库**

打板最怕什么？排进去了却不知道前面还有多少单子，撤单了也不知道是自己撤的还是被别人挤掉的。
这个库让你在排涨停的每一秒都清清楚楚：**前面还有多少手、后面追了多少手、谁在撤、谁在吃。**

基于[达塔接口](https://lty.gt.tc/dapi/) D201 的 Level-2 逐笔数据。数据源到本机是 TCP 直连（不经云中转），本机到你的策略代码走 localhost 通信（<1ms）。实际延迟仅取决于你到数据源的网络质量，无额外中转开销。

## 能做什么

- **排板前** — 看封单量变化趋势，判断要不要排
- **排队中** — 实时知道"我前面大约多少人/多少手/多少钱"和"我后面大约多少人"
- **关键时刻** — 前面有大单撤了、封单快速萎缩，立刻收到通知
- **撤单了** — 撤完回调里拿到状态，不用切回券商确认
- **炸板预警** — 涨停价位即将消失/已消失，第一时间撤退

核心原理：库自动订阅涨停买一档的逐笔排队数据，根据你报的手数推测你的委托 ID，然后持续追踪你前后每一笔委托的增减变化。**注意：ID 匹配基于手数推测，当同一价位出现相同手数时可能误判，不构成精确的成交回报。**

## 安装

```bash
pip install git+https://github.com/baseredge/limit_up_watcher.git
```

依赖: `websocket-client`

## 5 分钟上手

```python
from limit_up_watcher import LimitUpWatcher, WebSocketSource

# 盯住 SZ002177 涨停板买1档（7.95元 = 7950厘）
watcher = LimitUpWatcher("SZ002177", price_li=7950)

ws = WebSocketSource("ws://127.0.0.1:8080/d201")
ws.add_watcher(watcher)

@watcher.on_snapshot
def on_snap(w):
    """涨停价首次出现"""
    print(f"封板！封单量: {w.first.amount}万 / {w.first.volume}手 / {w.first.count}笔")

@watcher.on_tick
def on_tick(w):
    """每次数据更新"""
    print(f"+{w.new_add.amount}万(新排) -{w.cancelled.amount}万(撤) -{w.executed.amount}万(吃) "
          f"= 净{w.net_change_amt}万 | 封单{w.current.amount}万")

# 启动（阻塞）
ws.connect()
```

## 实战：排板 + 智能撤单

```python
# 你的券商下单后调用，告诉库"我排了 100 手"
idx = watcher.queue(hand_count=100)

@watcher.on_tick
def my_strategy(w):
    for i, entry in enumerate(w.my_orders):
        if entry.status == 2:  # 已匹配到我的单子（根据手数推测）

            # 前面不多了，可能快排到了
            if entry.front.amount < 50:
                print(f"前面只剩约 {entry.front.amount} 万")

            # 前面有大户跑了，我也撤
            if entry.front.last_reduction > 100:
                w.cancel(i)
                print("前面撤了 100 万，跟着撤！")

            # 封单要撑不住了
            if w.limit_up_may_gone:
                w.cancel(i)
                print("封单快没了，先撤！")

@watcher.on_fill
def on_fill(w, entry):
    print(f"推测已成交！等了约 {entry.queue_elapsed_ms/1000:.0f} 秒")
```

## API 速查

### LimitUpWatcher(code, price_li, direction=0)

| 属性 | 类型 | 说明 |
|------|------|------|
| `current` | TickAggregate | 当前总封单（万元/手/笔） |
| `new_add` | TickAggregate | 本次新增排单 |
| `cancelled` | TickAggregate | 本次撤单 |
| `executed` | TickAggregate | 本次被吃掉的单 |
| `first` | TickAggregate | 涨停时刻的初始封单 |
| `my_orders` | list[MyQueueEntry] | 我的所有排单记录 |
| `limit_up_gone` | bool | 涨停板是不是炸了 |
| `limit_up_may_gone` | bool | 封单快速萎缩预警 |
| `inflow_streak` | int | 连续多少 tick 有资金在排 |
| `outflow_streak` | int | 连续多少 tick 在流出 |
| `net_change_amt` | int | 本次净增金额(万元) |

| 方法 | 作用 |
|------|------|
| `queue(hand_count)` | 告诉库"我排板了 N 手" |
| `cancel(entry_index)` | 告诉库"我撤单了" |
| `on_snapshot(cb)` | 封板出现时回调 |
| `on_tick(cb)` | 每次数据更新回调（主战场） |
| `on_fill(cb)` | 推测成交回调（基于手数匹配，非精确回报） |
| `on_limit_gone(cb)` | 炸板回调 |

### MyQueueEntry — 你的排单位置

| 字段 | 含义 |
|------|------|
| `status` | 1=排队中(未匹配) 2=排队中(已匹配) 100=推测已成交 |
| `found_id` | 根据手数推测的委托 ID（0=未匹配） |
| `front.amount` | 你前面还有多少万元 |
| `front.volume` | 你前面还有多少手 |
| `front.last_reduction` | 刚才你前面减少了多少万 |
| `back.amount` | 你后面追了多少万元 |
| `queue_elapsed_ms` | 排了多久（毫秒） |

## 注意事项

- 需要先在本地安装 [达塔接口](https://lty.gt.tc/dapi/)（免费），启动后登录即可
- 价格单位 `price_li` 用厘：`元 × 1000`，如 7.95 元 = `7950`
- 金额单位是**万元**，手数单位是**手**（1手=100股）
- 如果你撤单了，记得调 `cancel(entry_index)`，否则状态不会更新

## License

MIT

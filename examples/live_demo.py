"""实时演示 — 连接 D201 监控涨停板排队

用法:
    # 先确保 data_interface.exe 已启动并登录
    python examples/live_demo.py <股票代码> <涨停价_元>

示例:
    python examples/live_demo.py SZ002177 7.95
"""
import sys
import time
import threading
from limit_up_watcher import LimitUpWatcher, WebSocketSource


def main():
    if len(sys.argv) < 3:
        print("用法: python live_demo.py <CODE> <PRICE_YUAN>")
        print("示例: python live_demo.py SZ002177 7.95")
        sys.exit(1)

    code = sys.argv[1]
    price_yuan = float(sys.argv[2])
    price_li = int(price_yuan * 1000)  # 元→厘

    print(f"监控 {code} 涨停价 {price_yuan}元 (price_li={price_li})")

    watcher = LimitUpWatcher(code, price_li)
    ws = WebSocketSource("ws://127.0.0.1:8080/d201")
    ws.add_watcher(watcher)

    # 注册回调
    @watcher.on_snapshot
    def on_snap(w):
        print(f"\n[快照] {w.code} 封单价={w.price_yuan:.2f}元, "
              f"封单={w.first.amount}万/{w.first.volume}手/{w.first.count}笔")

    @watcher.on_tick
    def on_tick(w):
        if w.current.volume == 0 and w.limit_up_gone:
            print(f"[涨停消失]")
            return
        if w.packet_seq % 20 == 1:  # 每20包打印一次
            print(f"[tick#{w.packet_seq}] "
                  f"封单={w.current.amount}万/{w.current.count}笔, "
                  f"+{w.new_add.amount}万, "
                  f"-{w.cancelled.amount}万(撤)/{w.executed.amount}万(成交), "
                  f"净增={w.net_change_amt}万, "
                  f"流入={w.inflow_streak} 流出={w.outflow_streak}")

    @watcher.on_position_update
    def on_pos(w, entry):
        print(f"[位置] 前面={entry.front.amount}万/{entry.front.volume}手, "
              f"后面={entry.back.amount}万/{entry.back.volume}手, "
              f"排队 {entry.queue_elapsed_ms/1000:.0f}秒")

    @watcher.on_fill
    def on_fill(w, entry):
        print(f"[成交!] 排队 {entry.queue_elapsed_ms/1000:.0f}秒后成交")

    @watcher.on_limit_gone
    def on_gone(w):
        print(f"[涨停消失] {w.code} 封板已打开")

    # 启动输入循环（模拟用户排板/撤单）
    print("\n输入命令: q=排板100手, c=撤单, s=查封单, 回车退出\n")

    # 主线程跑 WebSocket (后台), 主线程读输入
    print("连接 D201...")
    threading.Thread(target=ws.connect, kwargs={"block": True}, daemon=True).start()
    time.sleep(1)

    while True:
        try:
            cmd = input().strip()
        except EOFError:
            break
        if not cmd:
            break
        if cmd == "q":
            idx = watcher.queue(hand_count=100)
            print(f">>> 排板100手, entry={idx}")
        elif cmd == "c":
            for i, e in enumerate(watcher.my_orders):
                if e.status != 3:
                    watcher.cancel(i)
                    print(f">>> 撤单 entry={i}")
        elif cmd == "s":
            print(f"封单={watcher.current.amount}万/{watcher.current.count}笔")

    ws.disconnect()
    print("已断开")


if __name__ == "__main__":
    main()

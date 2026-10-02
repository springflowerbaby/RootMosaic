"""CPU stress 注入器(单根因故障之一)。

spawn N 个纯忙循环子进程,持续 --seconds 秒后自行退出。用来给整机制造 CPU 争用,
观察 sasrec 推理耗时 / http 延迟在故障窗内的上升。

用法:
    python cpu_stress.py --seconds 90 [--workers N]

Windows 注意:multiprocessing 在 Windows 是 spawn 模式,必须有 __main__ 守卫 +
freeze_support(),否则子进程递归 import 报错。子进程用 daemon=True,父进程被杀也不留孤儿。
该脚本按 deadline 自行结束(到点即退),编排脚本据此 wait 即可,无需强杀。
"""
import argparse
import multiprocessing as mp
import os
import time


def _burn(deadline: float):
    # 纯 CPU 忙循环,不 sleep、不申请大内存,只占 CPU 时间片
    x = 1.0001
    while time.time() < deadline:
        for _ in range(50000):
            x = x * 1.0000001 + 1e-9
        if x > 1e9:
            x = 1.0001


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--workers", type=int, default=0, help="0 = 逻辑核数")
    a = ap.parse_args()
    n = a.workers or (os.cpu_count() or 4)
    deadline = time.time() + a.seconds
    procs = []
    for _ in range(n):
        p = mp.Process(target=_burn, args=(deadline,), daemon=True)
        p.start()
        procs.append(p)
    print(f"[cpu_stress] {n} workers x {a.seconds}s (pid={os.getpid()})", flush=True)
    for p in procs:
        p.join()
    print("[cpu_stress] done", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()

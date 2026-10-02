"""Memory stress 注入器(host 作用域 resource 故障之一)。

镜像 cpu_stress.py 结构, 但制造的是【整机内存压力】而非 CPU 争用: 起【一个】daemon 子进程,
分块申请并实页提交一定量内存(默认 512MB), 持续 --seconds 秒后 deadline 到点自行释放退出。
用来抬升 host_mem_pct, 观察持久栈在内存吃紧下的表现(probe 服务只读探测)。

== 绝不无界增长(铁律: 有界 + 自动释放, 三重防 OOM) ==
  ① 硬上限闸 HARD_CAP_MB=2048: 即便误传 --mb 99999 也字面封顶 2048MB(main 第一步 clamp)。
  ② 分块申请 try/except MemoryError: 申请失败即停止增长(打印已分配量, 不崩整机/runner)。
  ③ daemon 子进程独立地址空间 + deadline 到点 return → bytearray 出作用域被 GC → 内存释放 →
     子进程退出; daemon=True 父被杀不留孤儿(与 cpu_stress 同护栏)。编排只需 wait, 无需强杀。

用法(可独立 smoke):
    python ops/chaos/mem_stress.py --mb 512 --seconds 30
    # 观察期间 psutil.virtual_memory().percent 抬升, 退出后回落到基线±2%。

Windows 注意: multiprocessing 在 Windows 是 spawn 模式, 必须有 __main__ 守卫 +
freeze_support(), 否则子进程递归 import 报错(照 cpu_stress.py 同款守卫)。
"""
import argparse
import multiprocessing as mp
import os
import time

# ---- 硬上限闸(字面常量, 绝不无界增长): 任何 --mb 值都被 clamp 到此上限以内 ----
HARD_CAP_MB = 2048
# 分块粒度: 每次申请 64MB, 边申请边实页提交, 分摊申请失败的风险(单块失败即停)。
CHUNK_MB = 64


def _hold(total_mb: int, deadline: float, touch_interval: float):
    """子进程体: 分块申请 total_mb(MB)内存并实页提交, 持有至 deadline 后释放退出。

    防惰性零页(Windows commit charge / Linux lazy zero-page 不计 RSS): 每块申请后立即
    按页步长(4096B)回写一字节, 强制实页提交, 使 RSS 真实抬升(否则 host_mem_pct 不动)。
    防换出: 持有期内每 touch_interval 秒回写每块首字节, 避免被换到 pagefile 后水位假性回落。
    """
    blocks = []
    allocated = 0
    chunk_bytes = CHUNK_MB * 1024 * 1024
    # 分块申请到 total_mb; 任一块申请失败(MemoryError)即停止增长, 不崩。
    while allocated < total_mb:
        want = min(CHUNK_MB, total_mb - allocated)
        nbytes = want * 1024 * 1024 if want != CHUNK_MB else chunk_bytes
        try:
            block = bytearray(nbytes)
            # 实页提交: 按 4096B 页步长写一字节, 强制 OS 真实分配物理页(防惰性提交)
            block[::4096] = b"\x01" * len(block[::4096])
            blocks.append(block)
            allocated += want
        except MemoryError:
            print(f"[mem_stress] MemoryError at {allocated}MB; stop growing (hold what we have)",
                  flush=True)
            break
    print(f"[mem_stress] child holding {allocated}MB until deadline (pid={os.getpid()})", flush=True)

    # 持有循环: 到 deadline 前周期 touch 防换出; 到点 return → blocks GC → 释放。
    last_touch = time.time()
    while time.time() < deadline:
        now = time.time()
        if now - last_touch >= touch_interval:
            for b in blocks:
                if len(b) > 0:
                    b[0] = 1  # 回写首字节防换出
            last_touch = now
        time.sleep(0.5)
    # 函数 return: blocks 出作用域 → 内存释放 → 子进程退出。


def main():
    ap = argparse.ArgumentParser(description="host 级内存压力注入器(有界 + deadline 自释放)")
    ap.add_argument("--mb", type=int, default=512,
                    help=f"单进程目标分配总量 MB(硬上限 {HARD_CAP_MB}MB; smoke 用 512)")
    ap.add_argument("--seconds", type=int, default=60, help="deadline 自退秒数")
    ap.add_argument("--touch-interval", type=float, default=5.0,
                    help="持有期周期 touch 间隔秒(防换出/惰性提交)")
    a = ap.parse_args()

    # ---- 硬上限闸: main 第一步 clamp, 超额时打印警示 ----
    total_mb = max(1, min(a.mb, HARD_CAP_MB))
    if a.mb > HARD_CAP_MB:
        print(f"[mem_stress] requested {a.mb}MB clamped to {HARD_CAP_MB}MB hard cap", flush=True)

    deadline = time.time() + a.seconds
    # 单 daemon 子进程持有内存(内存单进程持有即可, 不需多核); daemon=True 父被杀不留孤儿。
    p = mp.Process(target=_hold, args=(total_mb, deadline, a.touch_interval), daemon=True)
    p.start()
    procs = [p]
    print(f"[mem_stress] 1 holder x {total_mb}MB x {a.seconds}s (pid={os.getpid()})", flush=True)
    for proc in procs:
        proc.join()
    print("[mem_stress] done (memory released)", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()

#!/usr/bin/env python3
"""多租户批任务接纳器端到端演示。

场景:
  1. 登记不同权重的租户与异构机器 (CPU 节点 / GPU 加速器节点);
  2. 大 gang 任务资源不足时不留部分占用, 也不挡住其他租户的小任务;
  3. 持续送任务时按权重获得机会, 久候任务经老化在有限时间内启动;
  4. 业务键幂等 (同键同内容返回原结果, 同键异内容冲突);
  5. 撤回/终止/归还幂等, 容量恰好一次归还。

用法:
    python3 demo_batch.py                      # 内存演示
    python3 demo_batch.py --data-dir ./bm-data # 事件落盘, 重启恢复排队/占用
    python3 demo_batch.py --serve --port 8080  # 同时提供浏览器页面
"""

from __future__ import annotations

import argparse

from hpc_cr.api import build_server
from hpc_cr.batch import BatchManager, BatchConflict


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def run_demo(data_dir: str | None = None) -> BatchManager:
    bm = BatchManager(data_dir, auto_tick=False,
                      aging_after=30.0, aging_rate=2.0)

    banner("1) 登记租户 (权重) 与异构机器 (CPU / GPU 加速器)")
    for tid, w in (("physics", 3.0), ("chem", 1.0), ("bio", 2.0)):
        try:
            print("租户:", bm.register_tenant(tid, w))
        except BatchConflict:
            print(f"租户 {tid} 已存在 (重跑恢复)")
    machines = [
        ("gpu-1", {"cpu": 16, "mem": 64, "gpu": 8}, {"accel": "h100"}),
        ("gpu-2", {"cpu": 16, "mem": 64, "gpu": 8}, {"accel": "h100"}),
        ("cpu-1", {"cpu": 32, "mem": 128, "gpu": 0}, {}),
        ("cpu-2", {"cpu": 32, "mem": 128, "gpu": 0}, {}),
    ]
    for mid, cap, labels in machines:
        try:
            m = bm.add_machine(mid, cap, labels=labels)
            print(f"机器 {mid}: 总量 {m['total']} 标签 {m['labels']}")
        except BatchConflict:
            print(f"机器 {mid} 已存在 (重跑恢复)")

    banner("2) 成组需求: 3 台 GPU 机器协同 —— 集群只有 2 台, 接纳期拒绝")
    try:
        bm.submit_task(
            "physics", "mega-ensemble", 3,
            {"cpu": 4, "mem": 16, "gpu": 2},
            selector={"accel": "h100"})
    except BatchConflict as exc:
        print(f"拒绝 [{exc.reason}]: {exc}")

    banner("3) 2 台 GPU gang: 一台被占后, 任务排队且不留下部分占用;")
    banner("   同一时刻 chem 的 CPU 小任务不被阻挡, 照常启动")
    blocker = bm.submit_task(
        "physics", "blocker-gpu", 1,
        {"cpu": 14, "mem": 48, "gpu": 8}, selector={"accel": "h100"})
    print("blocker:", blocker["state"],
          [a["machine_id"] for a in blocker["allocations"]])
    gang = bm.submit_task(
        "physics", "dual-gpu-train", 2,
        {"cpu": 4, "mem": 16, "gpu": 2}, selector={"accel": "h100"})
    print("gang:", gang["state"], "→ 缘由:",
          gang["last_defer"] and gang["last_defer"]["code"])
    small = bm.submit_task("chem", "preprocess", 1, {"cpu": 2, "mem": 8})
    print("chem 小任务未被阻挡:", small["state"],
          [a["machine_id"] for a in small["allocations"]])

    banner("4) 业务键幂等: 同键同内容返回原任务, 同键异内容冲突")
    same = bm.submit_task("bio", "align-2026-10-04", 1,
                          {"cpu": 8, "mem": 32})
    again = bm.submit_task("bio", "align-2026-10-04", 1,
                           {"cpu": 8, "mem": 32})
    print("同键同内容 idempotent:", again["idempotent"],
          "同一任务:", again["task_id"] == same["task_id"])
    try:
        bm.submit_task("bio", "align-2026-10-04", 1, {"cpu": 16, "mem": 64})
    except BatchConflict as exc:
        print(f"同键异内容冲突 [{exc.reason}]")

    banner("5) 释放 blocker 后, 排队的 gang 两台容量一次性取得后启动")
    bm.terminate_task(blocker["task_id"])
    bm.run_scheduler_pass()
    gang_after = bm.get_task(gang["task_id"])
    print("gang:", gang_after["state"], "分配去向:",
          sorted(a["machine_id"] for a in gang_after["allocations"]))

    banner("6) 幂等终止与归还: 重复调用不二次扣减容量")
    bm.terminate_task(gang_after["task_id"])
    bm.terminate_task(gang_after["task_id"])
    bm.release_resources(gang_after["task_id"])
    for m in bm.list_machines():
        if m["machine_id"].startswith("gpu"):
            print(f"{m['machine_id']} 已用 {m['used']} 占用条目 "
                  f"{len(m['allocations'])}")

    banner("当前队列/运行/终态与最近调度次序见浏览器或 JSON")
    print("GET /api/batch/dashboard")
    for t in bm.list_tasks():
        print(f"  {t['state']:<10} {t['tenant_id']:<8} {t['business_key']}")
    return bm


def main() -> None:
    ap = argparse.ArgumentParser(description="多租户批任务接纳器演示")
    ap.add_argument("--data-dir", default=None,
                    help="持久化目录 (批事件日志位于 <dir>/batch/)")
    ap.add_argument("--serve", action="store_true",
                    help="演示后继续提供 HTTP 服务")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    bm = run_demo(args.data_dir)
    if not args.serve:
        bm.journal.close()
        return

    # 用演示管理器装配 HTTP 服务 (主作业服务同时可用, 二者日志独立)
    from hpc_cr.epochs import Scheduler
    from hpc_cr.jobs import JobManager
    from hpc_cr.security import Signer
    from hpc_cr.persistence import load_or_create_secret

    if args.data_dir is not None:
        secret = load_or_create_secret(args.data_dir)
        job_mgr = JobManager(Scheduler(Signer(secret)),
                             data_dir=args.data_dir)
    else:
        job_mgr = JobManager(Scheduler(Signer("demo-batch-secret")))
    server = build_server(job_mgr, host=args.host, port=args.port,
                          data_dir=args.data_dir, batch_manager=bm,
                          enable_batch=True)
    print(f"\n浏览器打开: http://{args.host}:{args.port}/ui/batch")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.shutdown()


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""多租户成组批任务接纳器端到端演示。

演示:
  1. 异构机器登记 (CPU 机 / 带不同型号加速器的 GPU 机);
  2. 权重 + 老化的排队次序 (score = 等待秒数 × 权重);
  3. 大任务全有或全无、不留下部分占用, 也不挡住此刻可启动的小任务;
  4. 成组预留: 高权重租户长期占满集群时, 久候的低权重大任务随机器释放
     逐台汇集预留, 最终成组取得 (不饥饿);
  5. 同业务键幂等返回、键同内容变冲突、撤回/终止/完成的幂等与容量归还。

用法:
  python3 demo_admission.py                 # 内存演示, 打印调度过程
  python3 demo_admission.py --serve 8080    # 演示后继续提供 HTTP/页面服务
  python3 demo_admission.py --data-dir ./adm-data   # 事件落盘, 重启可找回
"""

import argparse
import time

from hpc_cr.admission import AdmissionManager


def hr(title):
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def show_queue(mgr):
    ov = mgr.overview()
    if not ov["queue_order"]:
        print("  队列: (空)")
        return
    print("  队列 (score 降序):")
    for q in ov["queue_order"]:
        reason = (q.get("last_defer") or {}).get("reason", "")
        print(f"    wait={q['wait_seconds']:6.1f}s score={q['score']:7.1f} "
              f"{q['tenant_id']:6s} {q['business_key']:14s} "
              f"×{q['replicas']} {reason}")


def show_machines(mgr):
    print("  机器:")
    for m in mgr.list_machines():
        acc = ", ".join(f"{k}:{m['accelerators_free'].get(k,0)}/"
                        f"{v}" for k, v in m["accelerators_total"].items()) \
            or "-"
        print(f"    {m['machine_id']:8s} {m['state']:7s} "
              f"cpu {m['free_cpus']:2d}/{m['cpus']:2d}  "
              f"mem {m['free_memory_mb']:6d}/{m['memory_mb']:6d}  "
              f"acc[{acc}]  slices={m['active_slices']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=None)
    ap.add_argument("--serve", type=int, default=0, metavar="PORT")
    ap.add_argument("--host", default="127.0.0.1")
    args = ap.parse_args()

    adm_dir = f"{args.data_dir}/admission" if args.data_dir else None
    mgr = AdmissionManager(adm_dir)

    hr("1) 登记租户 (chem 权重 3, bio 权重 1) 与异构机器")
    mgr.upsert_tenant("chem", weight=3, name="化学组")
    mgr.upsert_tenant("bio", weight=1, name="生物组")
    mgr.register_machine("gpu-a100", cpus=16, memory_mb=128000,
                         accelerators={"a100": 8})
    mgr.register_machine("gpu-h100", cpus=16, memory_mb=256000,
                         accelerators={"h100": 4})
    mgr.register_machine("cpu-big", cpus=32, memory_mb=256000)
    mgr.register_machine("cpu-small", cpus=4, memory_mb=16000)
    show_machines(mgr)

    hr("2) 提交: 大模型需 4 副本×2×a100 (整组落 gpu-a100); CPU 小任务落 CPU 机")
    big = mgr.submit(
        business_key="mol-dyn-42", tenant_id="chem", replicas=4,
        cpus_per_replica=4, memory_mb_per_replica=16000,
        accelerators_per_replica={"a100": 2},
        payload={"case": "protein-fold"})
    print(f"  mol-dyn-42 -> {big['state']} 落点={big['machines']}")
    prep = mgr.submit(
        business_key="prep-data", tenant_id="bio", replicas=2,
        cpus_per_replica=4, memory_mb_per_replica=8000)
    print(f"  prep-data  -> {prep['state']} 落点={prep['machines']}")

    hr("3) 永久不可行的提交被明确拒绝 (给出缘由与缺口)")
    bad = mgr.submit(
        business_key="too-large", tenant_id="chem", replicas=1,
        cpus_per_replica=999, memory_mb_per_replica=1000)
    print(f"  too-large -> {bad['state']}: {bad['reject_reason']}")
    show_machines(mgr)

    hr("4) 幂等与冲突")
    again = mgr.submit(
        business_key="mol-dyn-42", tenant_id="chem", replicas=4,
        cpus_per_replica=4, memory_mb_per_replica=16000,
        accelerators_per_replica={"a100": 2},
        payload={"case": "protein-fold"})
    print(f"  同键同内容: task_id 一致={again['task_id'] == big['task_id']}, "
          f"idempotent={again['idempotent_replay']}")
    try:
        mgr.submit(business_key="mol-dyn-42", tenant_id="chem", replicas=5,
                   cpus_per_replica=4, memory_mb_per_replica=16000,
                   accelerators_per_replica={"a100": 2})
    except Exception as exc:  # noqa: BLE001
        print(f"  同键内容变: {type(exc).__name__} ({exc.reason})")

    hr("5) 容量不够: 大任务不占部分容量, 小任务继续上 (不阻塞)")
    # 占满两台 GPU 机和 cpu-big
    holds = []
    holds.append(mgr.submit(
        business_key="hold-h100", tenant_id="chem", replicas=4,
        cpus_per_replica=4, memory_mb_per_replica=16000,
        accelerators_per_replica={"h100": 1}))
    for h in holds:
        mgr.start_task(h["task_id"])
    blocked = mgr.submit(
        business_key="big-h100", tenant_id="bio", replicas=4,
        cpus_per_replica=4, memory_mb_per_replica=16000,
        accelerators_per_replica={"h100": 1})
    print(f"  big-h100 -> {blocked['state']} "
          f"暂缓缘由={((blocked.get('last_defer') or {}).get('reason'))}")
    show_queue(mgr)
    print("  -> 不产生任何部分切片; 其它可满足任务仍可提交")

    hr("6) 终止/完成幂等归还, 排队任务自动推进")
    mgr.terminate_task(holds[0]["task_id"])
    after = mgr.get_task(blocked["task_id"])
    print(f"  释放 h100 后 big-h100 -> {after['state']} 落点={after['machines']}")
    mgr.start_task(big["task_id"])
    done = mgr.complete_task(big["task_id"], result={"loss": 0.01})
    print(f"  mol-dyn-42 -> {done['state']}, 结果={done['result']}")
    show_machines(mgr)

    if args.data_dir:
        print(f"\n事件已落盘: {adm_dir}/admission-journal.jsonl")
        print("重新运行本命令可看到等待项与已分配容量被找回。")

    if args.serve:
        from hpc_cr.api import build_server
        server = build_server(host=args.host, port=args.serve,
                              admission=mgr, data_dir=args.data_dir)
        print(f"\nHTTP 服务: http://{args.host}:{args.serve}")
        print(f"接纳器页面: http://{args.host}:{args.serve}/ui/admission")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.shutdown()


if __name__ == "__main__":
    main()

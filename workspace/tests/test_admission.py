"""多租户成组任务接纳器: 成组分配、全有或全无、不阻塞、公平与不饥饿。

覆盖:
  * 异构机器 (CPU/内存/按型号加速器) 上的副本成组落点与最佳适配;
  * 容量不足时不留下部分占用, 且不挡住此刻可启动的其它租户任务;
  * 权重 + 老化 (score=wait×weight) 的可解释次序与久候不饥饿;
  * 同业务键幂等返回原结果、键同内容变 -> 409 冲突;
  * 撤回排队项 / 终止运行项 / 归还资源的幂等。
"""

import unittest

from hpc_cr.admission import (
    AdmissionManager,
    ALLOCATED,
    CANCELLED,
    QUEUED,
    REJECTED,
    RUNNING,
    SUCCEEDED,
    TERMINATED,
)
from hpc_cr.admission.manager import content_digest
from hpc_cr.admission.model import (
    machine_can_hold_replica,
    plan_gang_placement,
)
from hpc_cr.errors import Conflict, NotFound, ValidationError


class Clock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, dt):
        self.t += dt


def gang(mgr, key, tenant, replicas, cpu, mem, acc=None, **kw):
    return mgr.submit(business_key=key, tenant_id=tenant, replicas=replicas,
                      cpus_per_replica=cpu, memory_mb_per_replica=mem,
                      accelerators_per_replica=acc, **kw)


class PlacementTest(unittest.TestCase):
    def _snaps(self, mgr):
        return mgr.list_machines()

    def test_all_or_nothing_no_partial(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=8, memory_mb=32000)
        mgr.register_machine("n2", cpus=8, memory_mb=32000)
        # 占满 n1
        h = gang(mgr, "hold", "t", 1, 8, 32000)
        self.assertEqual(h["state"], ALLOCATED)
        # 需 2 台 8cpu: n1 忙、n2 空闲 -> 放不下整组
        big = gang(mgr, "big", "t", 2, 8, 32000)
        self.assertEqual(big["state"], QUEUED)
        # 关键: n2 上没有任何切片 (没有部分占用)
        n2 = next(m for m in self._snaps(mgr) if m["machine_id"] == "n2")
        self.assertEqual(n2["active_slices"], 0)
        self.assertEqual(n2["free_cpus"], 8)

    def test_accelerator_heterogeneity(self):
        mgr = AdmissionManager()
        mgr.register_machine("cpu-node", cpus=8, memory_mb=32000)
        mgr.register_machine("gpu-node", cpus=8, memory_mb=64000,
                             accelerators={"a100": 4})
        # 需要 5 个带 a100 的副本 -> 全集群只有 4 个 a100, 永久不足拒绝
        rej = gang(mgr, "gpu2", "t", 5, 2, 8000, {"a100": 1})
        self.assertEqual(rej["state"], REJECTED)
        self.assertEqual(rej["reject_reason"],
                         "cluster_insufficient_capacity")
        # 需要一种集群里完全没有的加速器型号 -> 单台永不可容纳
        rej2 = gang(mgr, "h100", "t", 1, 2, 8000, {"h100": 1})
        self.assertEqual(rej2["state"], REJECTED)
        self.assertEqual(rej2["reject_reason"], "single_machine_too_small")
        # 1 个带卡副本: 必须落在 gpu-node 而不是 cpu-node
        one = gang(mgr, "gpu1", "t", 1, 2, 8000, {"a100": 1})
        self.assertEqual(one["machines"], ["gpu-node"])
        # 纯 CPU 副本优先落在没有加速器的 cpu-node (不浪费带卡机)
        cpu = gang(mgr, "cpu1", "t", 1, 2, 8000)
        self.assertEqual(cpu["machines"], ["cpu-node"])

    def test_exclusive_placement(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=8, memory_mb=32000)
        a = gang(mgr, "a", "t", 1, 1, 1000, exclusive=False)
        self.assertEqual(a["state"], ALLOCATED)
        # 独占副本不能与 a 共享 n1; n1 物理可容纳但此刻被占 -> 排队 (非拒绝)
        b = gang(mgr, "b", "t", 1, 1, 1000, exclusive=True)
        self.assertEqual(b["state"], QUEUED)
        mgr.register_machine("n2", cpus=8, memory_mb=32000)
        mgr.schedule()
        self.assertEqual(mgr.get_task(b["task_id"])["state"], ALLOCATED)
        self.assertEqual(mgr.get_task(b["task_id"])["machines"], ["n2"])

    def test_plan_shadow_allows_multiple_replicas_same_machine(self):
        # 一台 8cpu 机器应能放 4 个 2cpu 副本 (非独占)
        m = {"machine_id": "x", "state": "ONLINE", "free_cpus": 8,
             "free_memory_mb": 32000, "active_slices": 0,
             "accelerators_total": {}, "accelerators_used": {},
             "accelerators_free": {}, "cpus": 8, "memory_mb": 32000}
        from hpc_cr.admission.model import Task
        t = Task(task_id="t", business_key="k", tenant_id="z", name="n",
                 replicas=4, cpus_per_replica=2,
                 memory_mb_per_replica=1000,
                 accelerators_per_replica={}, exclusive=False,
                 content_digest="x", spec={})
        plan = plan_gang_placement(t, [m])
        self.assertTrue(plan["fits"])
        self.assertEqual([mid for _, mid in plan["placements"]],
                         ["x", "x", "x", "x"])


class SubmissionFeasibilityTest(unittest.TestCase):
    def test_single_machine_too_small(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=4, memory_mb=8000)
        t = gang(mgr, "huge", "t", 1, 8, 4000)
        self.assertEqual(t["state"], REJECTED)
        self.assertEqual(t["reject_reason"], "single_machine_too_small")
        self.assertIn("biggest_machine", t["reject_detail"])

    def test_cluster_insufficient_replicas(self):
        mgr = AdmissionManager()
        for i in range(2):
            mgr.register_machine(f"n{i}", cpus=8, memory_mb=32000)
        t = gang(mgr, "many", "t", 3, 8, 32000, exclusive=False)
        self.assertEqual(t["state"], REJECTED)
        self.assertEqual(t["reject_reason"],
                         "cluster_insufficient_capacity")
        self.assertEqual(t["reject_detail"]["cluster_max_replicas"], 2)

    def test_empty_cluster_waits_not_rejects(self):
        mgr = AdmissionManager()
        t = gang(mgr, "future", "t", 2, 1, 1000)
        self.assertEqual(t["state"], QUEUED)  # 机器还没登记, 等待

    def test_bad_demand(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=8, memory_mb=32000)
        with self.assertRaises(ValidationError):
            gang(mgr, "bad", "t", 0, 1, 1000)
        with self.assertRaises(ValidationError):
            gang(mgr, "bad2", "t", 1, -1, 1000)

    def test_weight_must_be_positive(self):
        mgr = AdmissionManager()
        with self.assertRaises(ValidationError):
            mgr.upsert_tenant("x", weight=0)


class IdempotencyConflictTest(unittest.TestCase):
    def setUp(self):
        self.mgr = AdmissionManager()
        self.mgr.register_machine("n1", cpus=4, memory_mb=16000)

    def test_same_key_same_content_returns_original(self):
        t1 = gang(self.mgr, "key-1", "t", 1, 1, 1000)
        t2 = gang(self.mgr, "key-1", "t", 1, 1, 1000)
        self.assertEqual(t1["task_id"], t2["task_id"])
        self.assertTrue(t2["idempotent_replay"])

    def test_same_key_after_completion_returns_result(self):
        t1 = gang(self.mgr, "key-r", "t", 1, 1, 1000)
        self.mgr.start_task(t1["task_id"])
        self.mgr.complete_task(t1["task_id"], result={"answer": 42})
        again = gang(self.mgr, "key-r", "t", 1, 1, 1000)
        self.assertEqual(again["task_id"], t1["task_id"])
        self.assertEqual(again["state"], SUCCEEDED)
        self.assertEqual(again["result"], {"answer": 42})

    def test_same_key_changed_content_conflicts(self):
        gang(self.mgr, "key-c", "t", 1, 1, 1000)
        with self.assertRaises(Conflict) as cm:
            gang(self.mgr, "key-c", "t", 1, 2, 1000)  # cpus 变了
        self.assertEqual(cm.exception.reason, "business_key_conflict")
        # 改 payload 也算内容变化
        with self.assertRaises(Conflict):
            self.mgr.submit(business_key="key-c", tenant_id="t", replicas=1,
                            cpus_per_replica=1, memory_mb_per_replica=1000,
                            payload={"v": 2})

    def test_digest_excludes_tenant_and_key(self):
        d1 = content_digest({"replicas": 1, "payload": {"x": 1}})
        d2 = content_digest({"payload": {"x": 1}, "replicas": 1})
        self.assertEqual(d1, d2)


class LifecycleIdempotentTest(unittest.TestCase):
    def setUp(self):
        self.mgr = AdmissionManager()
        for i in range(2):
            self.mgr.register_machine(f"n{i}", cpus=4, memory_mb=16000)

    def test_cancel_queued_is_idempotent(self):
        # 占满两台机器后, 新任务排队等待
        self.hold = gang(self.mgr, "hold", "h", 2, 4, 16000)
        t = gang(self.mgr, "k", "t", 1, 4, 16000)
        self.assertEqual(t["state"], QUEUED)
        r1 = self.mgr.cancel_task(t["task_id"])
        self.assertEqual(r1["state"], CANCELLED)
        r2 = self.mgr.cancel_task(t["task_id"])
        self.assertTrue(r2["idempotent_replay"])
        # 已取消不能终止
        with self.assertRaises(Conflict):
            self.mgr.terminate_task(t["task_id"])

    def test_terminate_releases_capacity_idempotently(self):
        t = gang(self.mgr, "run", "t", 2, 4, 16000)
        self.assertEqual(t["state"], ALLOCATED)
        self.mgr.start_task(t["task_id"])
        # 占了两台, 新任务排队
        waiter = gang(self.mgr, "wait", "t", 1, 4, 16000)
        self.assertEqual(waiter["state"], QUEUED)
        term = self.mgr.terminate_task(t["task_id"])
        self.assertEqual(term["state"], TERMINATED)
        # waiter 被调度到释放出的机器
        self.assertEqual(self.mgr.get_task(waiter["task_id"])["state"],
                         ALLOCATED)
        # 重复终止幂等, 且不会二次释放
        again = self.mgr.terminate_task(t["task_id"])
        self.assertTrue(again["idempotent_replay"])
        # 全部 slice 已归还
        freed = [s for s in term["slices"]]
        self.assertTrue(all(not s["active"] for s in freed))

    def test_complete_releases_and_reruns_schedule(self):
        t = gang(self.mgr, "job", "t", 2, 4, 16000)
        self.mgr.start_task(t["task_id"])
        waiter = gang(self.mgr, "w", "t", 2, 4, 16000)
        self.assertEqual(waiter["state"], QUEUED)
        done = self.mgr.complete_task(t["task_id"], result={"ok": True})
        self.assertEqual(done["state"], SUCCEEDED)
        self.assertEqual(done["result"], {"ok": True})
        self.assertEqual(self.mgr.get_task(waiter["task_id"])["state"],
                         ALLOCATED)
        # 重复完成幂等
        again = self.mgr.complete_task(t["task_id"])
        self.assertTrue(again["idempotent_replay"])

    def test_queued_cannot_start_or_complete(self):
        gang(self.mgr, "hold", "h", 2, 4, 16000)
        t = gang(self.mgr, "q", "t", 1, 4, 16000)
        self.assertEqual(t["state"], QUEUED)
        with self.assertRaises(Conflict):
            self.mgr.start_task(t["task_id"])
        with self.assertRaises(Conflict):
            self.mgr.complete_task(t["task_id"])

    def test_terminate_queued_asks_for_cancel(self):
        gang(self.mgr, "hold", "h", 2, 4, 16000)
        t = gang(self.mgr, "q", "t", 1, 4, 16000)
        self.assertEqual(t["state"], QUEUED)
        with self.assertRaises(Conflict):
            self.mgr.terminate_task(t["task_id"])


class FairnessStarvationTest(unittest.TestCase):
    def setUp(self):
        self.clock = Clock()
        self.mgr = AdmissionManager(clock=self.clock)
        self.mgr.upsert_tenant("hi", weight=9)
        self.mgr.upsert_tenant("lo", weight=1)
        for i in range(3):
            self.mgr.register_machine(f"n{i}", cpus=1, memory_mb=100)

    def _fill(self):
        ids = []
        for i in range(3):
            r = gang(self.mgr, f"hold-{i}", "hi", 1, 1, 100)
            ids.append(r["task_id"])
            self.mgr.start_task(r["task_id"])
        return ids

    def test_score_ordering_weight_then_aging(self):
        # 占满集群, 两个不可接纳任务同刻到达: 高权重在前
        self._fill()
        a = gang(self.mgr, "a", "hi", 1, 1, 100)
        b = gang(self.mgr, "b", "lo", 1, 1, 100)
        self.assertEqual(a["state"], QUEUED)
        order = [q["tenant_id"] for q in
                 self.mgr.overview()["queue_order"]]
        self.assertEqual(order[0], "hi")
        self.assertIn("lo", order)

    def test_aging_makes_lo_outscore_fresh_hi(self):
        self._fill()
        lo = gang(self.mgr, "lo-job", "lo", 3, 1, 100)
        self.assertEqual(lo["state"], QUEUED)
        # lo 等待很久; hi 每轮补任务, 但预留汇集保证 lo 成组
        for cycle in range(1, 10):
            self.clock.advance(10)
            running = self.mgr.list_tasks(tenant_id="hi", state=RUNNING)
            if running:
                self.mgr.terminate_task(running[0]["task_id"])
            self.mgr.submit(business_key=f"hi-new-{cycle}",
                            tenant_id="hi", replicas=1,
                            cpus_per_replica=1, memory_mb_per_replica=100)
        final = self.mgr.get_task(lo["task_id"])
        self.assertEqual(final["state"], ALLOCATED)
        self.assertEqual(sorted(final["machines"]), ["n0", "n1", "n2"])

    def test_reservation_does_not_block_runnable_others(self):
        # 一台大机忙 + 一台小机空闲; 大任务需两台大机, 小任务用小机
        mgr = AdmissionManager()
        mgr.register_machine("big", cpus=16, memory_mb=64000)
        mgr.register_machine("small", cpus=2, memory_mb=4000)
        h = gang(mgr, "h", "x", 1, 16, 64000)
        mgr.start_task(h["task_id"])
        big = gang(mgr, "big", "x", 2, 4, 8000)  # small 物理放不下
        self.assertEqual(big["state"], QUEUED)
        small = gang(mgr, "small", "y", 1, 1, 1000)
        self.assertEqual(small["state"], ALLOCATED)
        self.assertEqual(small["machines"], ["small"])

    def test_tenant_fifo_within_tenant(self):
        # 同一租户两个任务, 先入队的排在前面 (enqueue seq 决胜)
        mgr = AdmissionManager()
        mgr.register_machine("n", cpus=1, memory_mb=100)
        hold = gang(mgr, "hold", "t", 1, 1, 100)  # 占住唯一机器
        first = gang(mgr, "first", "t", 1, 1, 100)
        second = gang(mgr, "second", "t", 1, 1, 100)
        self.assertEqual(first["state"], QUEUED)
        order = [q["task_id"] for q in mgr.overview()["queue_order"]]
        self.assertEqual(order, [first["task_id"], second["task_id"]])


class MachineLifecycleTest(unittest.TestCase):
    def test_drain_then_offline_blocks_new_allocations(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=4, memory_mb=16000)
        mgr.drain_machine("n1")
        t = gang(mgr, "t", "x", 1, 1, 1000)
        self.assertEqual(t["state"], QUEUED)
        # 重新上线即接纳
        mgr.online_machine("n1")
        self.assertEqual(mgr.get_task(t["task_id"])["state"], ALLOCATED)

    def test_offline_refused_while_slices_held(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=4, memory_mb=16000)
        t = gang(mgr, "t", "x", 1, 1, 1000)
        with self.assertRaises(Conflict):
            mgr.offline_machine("n1")
        mgr.terminate_task(t["task_id"])
        snap = mgr.offline_machine("n1")
        self.assertEqual(snap["state"], "OFFLINE")

    def test_shrink_with_active_slice_refused(self):
        mgr = AdmissionManager()
        mgr.register_machine("n1", cpus=8, memory_mb=32000)
        gang(mgr, "t", "x", 1, 4, 8000)
        with self.assertRaises(Conflict):
            mgr.register_machine("n1", cpus=2, memory_mb=32000)


    def test_cancelling_holder_does_not_deadlock_gathered_machines(self):
        # holder 任务一旦因机器汇集而能成组就会立即被接纳, 真正需要验证的是:
        # 排队中的持有者被撤回后, 它名下的预留立即消失, 其它排队任务能使用
        # 这些机器 (预留不会变成永久锁)。
        mgr = AdmissionManager()
        mgr.register_machine("a", cpus=8, memory_mb=32000)
        mgr.register_machine("b", cpus=8, memory_mb=32000)
        # 一台占着 (a), 一台空 (b)
        hold = gang(mgr, "hold", "t", 1, 8, 32000)
        mgr.start_task(hold["task_id"])
        # big 需要 2 台 -> 排队并成为预留持有者, 汇集空闲的 b
        big = gang(mgr, "big", "t", 2, 8, 32000)
        self.assertEqual(big["state"], QUEUED)
        res = mgr.last_schedule()["reservations"]
        self.assertEqual(res[0]["machine_ids"], ["b"])
        # other 需要 1 台: a 忙、b 已预留 -> 排队
        other = gang(mgr, "other", "t", 1, 8, 32000)
        self.assertEqual(other["state"], QUEUED)
        # 撤回持有者 big -> 预留 b 释放; 但 a 仍忙, other 还无法接纳。
        # 再终止 hold 释放 a, other 应立即被接纳。
        mgr.cancel_task(big["task_id"])
        mgr.terminate_task(hold["task_id"])
        self.assertEqual(mgr.get_task(other["task_id"])["state"],
                         ALLOCATED)
        # 不应留下任何指向已取消任务的预留
        alive = {r["task_id"] for r in
                 (mgr.last_schedule() or {}).get("reservations", [])}
        self.assertNotIn(big["task_id"], alive)


if __name__ == "__main__":
    unittest.main()

"""批接纳器: 成组分配、权重公平、老化防饥饿、幂等与拒绝缘由。"""

import unittest

from hpc_cr.batch import (
    BatchConflict,
    BatchManager,
    BatchNotFound,
    BatchValidationError,
)
from hpc_cr.batch.model import (
    CANCELLED,
    COMPLETED,
    DRAINING,
    ENABLED,
    QUEUED,
    RUNNING,
    TERMINATED,
)
from hpc_cr.batch.scheduling import (
    DEFER_FREE_CAPACITY,
    REJECT_CLUSTER_CPU,
    REJECT_TOO_FEW_MATCHING,
)


class FakeClock:
    def __init__(self, t=1000.0):
        self.t = t

    def __call__(self):
        return self.t

    def bump(self, dt):
        self.t += dt


def need(cpu=0, mem=0, gpu=0):
    return {"cpu": cpu, "mem": mem, "gpu": gpu}


class BatchTestBase(unittest.TestCase):
    aging_after = 30.0
    aging_rate = 2.0

    def setUp(self):
        self.clk = FakeClock()
        self.bm = BatchManager(
            clock=self.clk, aging_after=self.aging_after,
            aging_rate=self.aging_rate)

    def tenant(self, tid, w=1.0):
        self.bm.register_tenant(tid, w)
        return tid

    def machine(self, mid, cpu=8, mem=32, gpu=0, labels=None):
        self.bm.add_machine(mid, {"cpu": cpu, "mem": mem, "gpu": gpu},
                            labels=labels)
        return mid


class GangAllocationTest(BatchTestBase):
    def test_all_or_nothing_and_no_partial_hold(self):
        self.tenant("a")
        self.machine("g1", gpu=1, labels={"gpu": "a100"})
        self.machine("g2", gpu=1, labels={"gpu": "a100"})
        # 需要 2 台 GPU 机器协同, 先占一台
        occ = self.bm.submit_task("a", "occ", 1, need(gpu=1),
                                  selector={"gpu": "a100"})
        self.assertEqual(occ["state"], RUNNING)
        gang = self.bm.submit_task("a", "gang", 2, need(gpu=1),
                                   selector={"gpu": "a100"})
        # 装不下: 保持 QUEUED, 机器上没有该任务的任何部分占用
        self.assertEqual(gang["state"], QUEUED)
        self.assertEqual(
            gang["last_defer"]["code"], DEFER_FREE_CAPACITY)
        # 装不下时任务没有任何分配, 机器账目里也没有该任务的占用
        self.assertEqual(gang["allocations"], [])
        self.assertEqual(
            self.bm.get_task(gang["task_id"])["allocations"], [])
        free = {m["machine_id"]: m["free"]["gpu"]
                for m in self.bm.list_machines()}
        # 空闲机器仍完整可用 (未被部分预留): 一台占满(0), 一台全空(1)
        self.assertEqual(free["g1"] + free["g2"], 1)

        # 释放占用机后, 两台同时取得才启动
        self.bm.terminate_task(occ["task_id"])
        self.bm.run_scheduler_pass()
        after = self.bm.get_task(gang["task_id"])
        self.assertEqual(after["state"], RUNNING)
        self.assertEqual(
            sorted(a["machine_id"] for a in after["allocations"]),
            ["g1", "g2"])

    def test_other_tenants_not_blocked_by_big_gang(self):
        # 3 台机器, b 的 3 机大任务此刻装不下 (1 台被占),
        # a 的单机小任务必须照常启动, 不被 b 的队头挡住
        self.machine("m1", cpu=4)
        self.machine("m2", cpu=4)
        self.machine("m3", cpu=4)
        self.tenant("a", 1.0)
        self.tenant("b", 1.0)
        blocker = self.bm.submit_task("b", "blocker", 1, need(cpu=4))
        self.assertEqual(blocker["state"], RUNNING)
        big = self.bm.submit_task("b", "big", 3, need(cpu=1))
        self.assertEqual(big["state"], QUEUED)
        self.assertEqual(
            big["last_defer"]["code"], DEFER_FREE_CAPACITY)
        small = self.bm.submit_task("a", "small", 1, need(cpu=1))
        self.assertEqual(small["state"], RUNNING)
        self.assertEqual(len(small["allocations"]), 1)

    def test_machine_selector_and_no_candidate(self):
        self.tenant("a")
        self.machine("cpu-node", cpu=8)
        # 选择器无任何机器匹配 -> 接纳闸门放行的是"物理总量"判断,
        # 这里匹配机器数为 0, 直接拒绝入队 (没有机器能跑)
        with self.assertRaises(BatchConflict) as cm:
            self.bm.submit_task("a", "gpu-job", 1, need(gpu=1),
                                selector={"gpu": "h100"})
        self.assertEqual(cm.exception.reason, REJECT_TOO_FEW_MATCHING)
        self.machine("gpu-node", cpu=8, gpu=2, labels={"gpu": "h100"})
        t = self.bm.submit_task("a", "gpu-job2", 1, need(gpu=1),
                                selector={"gpu": "h100"})
        self.assertEqual(t["state"], RUNNING)

    def test_reject_at_admission_when_cluster_cannot_ever_fit(self):
        self.tenant("a")
        self.machine("m1", cpu=8, mem=32, gpu=1)
        with self.assertRaises(BatchConflict) as cm:
            self.bm.submit_task("a", "huge", 2, need(gpu=1))
        self.assertEqual(cm.exception.reason, REJECT_TOO_FEW_MATCHING)
        with self.assertRaises(BatchConflict) as cm:
            self.bm.submit_task("a", "cpu-huge", 1, need(cpu=64))
        self.assertEqual(cm.exception.reason, REJECT_CLUSTER_CPU)
        # 瞬时碎片不拒绝: 只剩 0 空闲但总量够, 仍入队
        occ = self.bm.submit_task("a", "occ", 1, need(cpu=8))
        self.assertEqual(occ["state"], RUNNING)
        waiting = self.bm.submit_task("a", "wait-cpu", 1, need(cpu=8))
        self.assertEqual(waiting["state"], QUEUED)
        self.assertEqual(
            waiting["last_defer"]["code"], DEFER_FREE_CAPACITY)

    def test_draining_machine_not_assigned(self):
        self.tenant("a")
        self.machine("m1", cpu=4)
        self.machine("m2", cpu=4)
        # m1 进入 DRAINING: 物理上仍计入可恢复容量, 任务入队但不分配给它;
        # m2 被占满时, 任务只能继续等待
        self.bm.set_machine_state("m1", DRAINING)
        blocker = self.bm.submit_task("a", "blk", 1, need(cpu=4))
        self.assertEqual(blocker["state"], RUNNING)
        self.assertEqual(blocker["allocations"][0]["machine_id"], "m2")
        t = self.bm.submit_task("a", "t", 1, need(cpu=1))
        self.assertEqual(t["state"], QUEUED)
        self.assertEqual(t["last_defer"]["code"], DEFER_FREE_CAPACITY)
        # m1 恢复 ENABLED 后立即可以接纳
        self.bm.set_machine_state("m1", ENABLED)
        self.assertEqual(self.bm.get_task(t["task_id"])["state"], RUNNING)
        self.assertEqual(
            self.bm.get_task(t["task_id"])["allocations"][0]["machine_id"],
            "m1")

    def test_best_fit_packs_heterogeneous_machines(self):
        self.tenant("a")
        self.machine("small", cpu=2)
        self.machine("big", cpu=16)
        t = self.bm.submit_task("a", "t", 1, need(cpu=2))
        self.assertEqual(t["state"], RUNNING)
        self.assertEqual(t["allocations"][0]["machine_id"], "small")


class FairnessTest(BatchTestBase):
    def _cluster(self, n=6, cpu=1):
        for i in range(n):
            self.machine(f"m{i}", cpu=cpu)

    def test_weight_gives_proportional_throughput(self):
        # 无限积压 (两队始终补满) 下, 权重 3:1 的长期完成数应约为 3:1。
        # 老化关闭以免等候补偿干扰纯权重比例。
        self.bm = BatchManager(
            clock=self.clk, aging_after=10 ** 9, aging_rate=0.0)
        for i in range(10):
            self.machine(f"m{i}", cpu=1)
        self.tenant("hi", 3.0)
        self.tenant("lo", 1.0)
        counter = {"n": 0}

        def backfill():
            for tid in ("hi", "lo"):
                queued = len(self.bm.list_tasks(tenant_id=tid,
                                                state="QUEUED"))
                for _ in range(20 - queued):
                    counter["n"] += 1
                    self.bm.submit_task(
                        tid, f"{tid}-{counter['n']}", 1, need(cpu=1))

        backfill()
        self.bm.run_scheduler_pass()
        for _ in range(40):
            self.clk.bump(1)
            for t in self.bm.list_tasks(state="RUNNING"):
                self.bm.complete_task(t["task_id"])
            backfill()
        hi = sum(1 for t in self.bm.list_tasks(tenant_id="hi")
                 if t["state"] == COMPLETED)
        lo = sum(1 for t in self.bm.list_tasks(tenant_id="lo")
                 if t["state"] == COMPLETED)
        self.assertGreater(hi, 2.6 * lo)
        self.assertLess(hi, 3.4 * lo)

    def test_aging_prevents_indefinite_starvation(self):
        # 机器先被高权重租户占满, 低权重任务此时入队; hi 持续补新任务。
        # 无老化时 hi 凭权重永远先得; 老化使久候的 lo 在有限墙上时间启动。
        self.bm = BatchManager(
            clock=self.clk, aging_after=5.0, aging_rate=4.0)
        self.machine("m0", cpu=1)
        self.tenant("hi", 100.0)
        self.tenant("lo", 1.0)
        first = self.bm.submit_task("hi", "h0", 1, need(cpu=1))
        self.assertEqual(first["state"], RUNNING)
        # 机器已被占住时 lo 才入队: 必然排队
        for i in range(1, 30):
            self.bm.submit_task("hi", f"h{i}", 1, need(cpu=1))
        lo_t = self.bm.submit_task("lo", "lone", 1, need(cpu=1))
        self.assertEqual(
            self.bm.get_task(lo_t["task_id"])["state"], QUEUED)
        # 未跨老化阈值: hi 仍连续优先
        self.clk.bump(3)
        self.bm.complete_task(first["task_id"])
        self.assertEqual(
            self.bm.get_task(lo_t["task_id"])["state"], QUEUED)
        # 跨过阈值后, lo 的队首老化得分持续下移, 必定先启动
        for cycle in range(20):
            self.clk.bump(3)
            running = self.bm.list_tasks(state="RUNNING")
            for t in running:
                self.bm.complete_task(t["task_id"])
            if self.bm.get_task(lo_t["task_id"])["state"] == RUNNING:
                break
        self.assertEqual(
            self.bm.get_task(lo_t["task_id"])["state"], RUNNING)

    def test_new_tenant_baseline_aligns_and_does_not_jump(self):
        # a 已经积累了较高 fair_key; 新租户 b 注册时基线对齐到当前最大值,
        # 因此不会以 0 键插到老租户前面, 也不会被压到队尾无法起步。
        self._cluster(2)
        self.tenant("a", 1.0)
        for i in range(4):
            t = self.bm.submit_task("a", f"a{i}", 1, need(cpu=1))
            self.bm.complete_task(t["task_id"])
        # 占满两台机器, a 与新租户 b 各排一个任务
        self.bm.submit_task("a", "long1", 1, need(cpu=1))
        self.bm.submit_task("a", "long2", 1, need(cpu=1))
        wait_a = self.bm.submit_task("a", "wait-a", 1, need(cpu=1))
        self.tenant("b", 1.0)
        wait_b = self.bm.submit_task("b", "wait-b", 1, need(cpu=1))
        # b 的 fair_key 基线 = a 的当前值 (4.0), 二者同分按租户 id 决胜,
        # "a" < "b" => a 的老排队任务先启动; b 紧接着启动, 不被饿死
        for t in self.bm.list_tasks(state="RUNNING"):
            self.bm.complete_task(t["task_id"])
        self.bm.run_scheduler_pass()
        self.assertEqual(
            self.bm.get_task(wait_a["task_id"])["state"], RUNNING)
        self.assertEqual(
            self.bm.get_task(wait_b["task_id"])["state"], RUNNING)

    def test_schedule_order_is_explained_in_pass(self):
        self._cluster(2)
        self.tenant("a", 2.0)
        self.tenant("b", 1.0)
        self.bm.submit_task("a", "a1", 1, need(cpu=1))
        self.bm.submit_task("b", "b1", 1, need(cpu=1))
        p = self.bm.run_scheduler_pass()
        # 上一轮提交已经启动; 再排两个等机器的, 轮次记录推迟原因
        self.bm.submit_task("a", "a2", 1, need(cpu=1))
        self.bm.submit_task("b", "b2", 1, need(cpu=1))
        p = self.bm.run_scheduler_pass()
        tenants_in_order = [o["tenant_id"] for o in p["order"]]
        self.assertIn("a", tenants_in_order)
        self.assertIn("b", tenants_in_order)
        for o in p["order"]:
            if o["decision"] == "deferred":
                self.assertIn("reason", o)
                self.assertIn("score", o)


class IdempotencyTest(BatchTestBase):
    def setUp(self):
        super().setUp()
        self.tenant("a")
        self.machine("m1", cpu=8, mem=32, gpu=2)

    def test_same_key_same_content_returns_original(self):
        r1 = self.bm.submit_task("a", "key", 1, need(cpu=2, gpu=1))
        r2 = self.bm.submit_task("a", "key", 1, need(cpu=2, gpu=1))
        self.assertTrue(r2["idempotent"])
        self.assertEqual(r1["task_id"], r2["task_id"])

    def test_same_key_changed_content_conflicts(self):
        self.bm.submit_task("a", "key", 1, need(cpu=2))
        with self.assertRaises(BatchConflict) as cm:
            self.bm.submit_task("a", "key", 1, need(cpu=3))
        self.assertEqual(cm.exception.reason, "business_key_conflict")
        with self.assertRaises(BatchConflict):
            self.bm.submit_task("a", "key", 2, need(cpu=2))

    def test_original_result_returned_after_completion(self):
        r = self.bm.submit_task("a", "key", 1, need(cpu=1))
        self.bm.complete_task(r["task_id"], {"artifact": "cas://x"})
        again = self.bm.submit_task("a", "key", 1, need(cpu=1))
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["state"], COMPLETED)
        self.assertEqual(again["result"], {"artifact": "cas://x"})

    def test_cancel_terminate_release_are_idempotent(self):
        self.machine("m2", cpu=8, mem=32, gpu=2)
        for key in ("blk1", "blk2"):
            self.assertEqual(
                self.bm.submit_task("a", key, 1,
                                    need(cpu=8, gpu=2))["state"],
                RUNNING)
        queued = self.bm.submit_task("a", "q", 2, need(cpu=1))
        self.assertEqual(queued["state"], QUEUED)
        c1 = self.bm.cancel_task(queued["task_id"])
        c2 = self.bm.cancel_task(queued["task_id"])
        self.assertFalse(c1["idempotent"])
        self.assertTrue(c2["idempotent"])

        # 腾出一台机器再提交, 任务应立即 RUNNING, 再测终止幂等
        self.bm.terminate_task(
            self.bm.list_tasks(tenant_id="a", state="RUNNING")[0]["task_id"])
        running = self.bm.submit_task("a", "r", 1, need(cpu=1))
        self.assertEqual(running["state"], RUNNING)
        t1 = self.bm.terminate_task(running["task_id"])
        t2 = self.bm.terminate_task(running["task_id"])
        self.assertFalse(t1["idempotent"])
        self.assertTrue(t2["idempotent"])

        # release_resources 对任意状态幂等
        rel = self.bm.release_resources(running["task_id"])
        self.assertTrue(rel["idempotent"])
        self.assertEqual(rel["state"], TERMINATED)

    def test_illegal_transitions_rejected(self):
        self.machine("m2", cpu=8, mem=32, gpu=2)
        # 先占住两台机器, 使 2 机成组任务此刻一台都拿不到 -> QUEUED
        for key, mid_hint in (("blk1", None), ("blk2", None)):
            blk = self.bm.submit_task("a", key, 1, need(cpu=8, gpu=2))
            self.assertEqual(blk["state"], RUNNING)
        t = self.bm.submit_task("a", "q", 2, need(cpu=1))
        self.assertEqual(t["state"], QUEUED)
        # QUEUED 不能终止
        with self.assertRaises(BatchConflict) as cm:
            self.bm.terminate_task(t["task_id"])
        self.assertEqual(cm.exception.reason, "not_terminatable")
        # 撤回可执行一次, 之后重复撤回按规格幂等返回同一终态
        c1 = self.bm.cancel_task(t["task_id"])
        c2 = self.bm.cancel_task(t["task_id"])
        self.assertFalse(c1["idempotent"])
        self.assertTrue(c2["idempotent"])
        self.assertEqual(c2["state"], CANCELLED)
        # 已撤回的任务不能终止 (不是运行项)
        with self.assertRaises(BatchConflict) as cm:
            self.bm.terminate_task(t["task_id"])
        self.assertEqual(cm.exception.reason, "not_terminatable")

    def test_capacity_returned_exactly_once(self):
        t = self.bm.submit_task("a", "r", 1, need(cpu=4, gpu=2))
        self.assertEqual(self.bm.list_machines()[0]["used"]["gpu"], 2)
        self.bm.terminate_task(t["task_id"])
        self.bm.terminate_task(t["task_id"])
        self.bm.release_resources(t["task_id"])
        m = self.bm.list_machines()[0]
        self.assertEqual(m["used"], {"cpu": 0, "mem": 0, "gpu": 0})
        self.assertEqual(m["allocations"], [])


class LifecycleValidationTest(BatchTestBase):
    def test_validation_errors(self):
        self.tenant("a")
        with self.assertRaises(BatchConflict):
            self.bm.register_tenant("a", 1)
        with self.assertRaises(BatchValidationError):
            self.bm.register_tenant("neg", -1)
        with self.assertRaises(BatchValidationError):
            self.bm.add_machine("m1", need(cpu=-1))
        with self.assertRaises(BatchValidationError):
            self.bm.submit_task("a", "k", 0, need(cpu=1))
        with self.assertRaises(BatchValidationError):
            self.bm.submit_task("a", "k2", 1, need())

    def test_unknown_entities(self):
        with self.assertRaises(BatchNotFound):
            self.bm.get_task("nope")
        with self.assertRaises(BatchNotFound):
            self.bm.submit_task("ghost", "k", 1, need(cpu=1))

    def test_deadline_expiry_cancels_queued_task(self):
        self.machine("m1", cpu=1)
        self.tenant("a")
        # 占满机器再排一个带截止时间的任务
        self.bm.submit_task("a", "long", 1, need(cpu=1))
        t = self.bm.submit_task("a", "soon", 1, need(cpu=1),
                                deadline_seconds=5)
        self.clk.bump(6)
        self.bm.run_scheduler_pass()
        done = self.bm.get_task(t["task_id"])
        self.assertEqual(done["state"], CANCELLED)
        self.assertEqual(done["finish_reason"], "deadline_expired")

    def test_machine_capacity_change_timeline(self):
        self.machine("m1", cpu=4)
        self.bm.update_machine("m1", total=need(cpu=8))
        view = [x for x in self.bm.list_machines()
                if x["machine_id"] == "m1"][0]
        self.assertEqual(view["total"]["cpu"], 8)
        kinds = [e["kind"] for e in view["timeline"]]
        self.assertIn("updated", kinds)
        # 不能缩容到用量以下
        self.tenant("a")
        t = self.bm.submit_task("a", "t", 1, need(cpu=6))
        self.assertEqual(t["state"], RUNNING)
        with self.assertRaises(BatchConflict):
            self.bm.update_machine("m1", total=need(cpu=4))

    def test_busy_machine_cannot_be_removed(self):
        self.tenant("a")
        self.machine("m1", cpu=4)
        t = self.bm.submit_task("a", "t", 1, need(cpu=1))
        with self.assertRaises(BatchConflict):
            self.bm.set_machine_state("m1", "REMOVED")
        self.bm.terminate_task(t["task_id"])
        view = self.bm.set_machine_state("m1", "REMOVED")
        self.assertEqual(view["state"], "REMOVED")


if __name__ == "__main__":
    unittest.main()

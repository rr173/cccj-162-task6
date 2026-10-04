"""接纳器并发竞争: 多线程提交/终止/成组调度下的一致性与幂等。

  * 多线程同时用同一 business_key 提交 (相同内容) -> 恰好建立一个任务;
  * 多线程同时用同一 business_key 提交不同内容 -> 只有一个胜出, 其余 409;
  * 高并发分配/终止后机器已用容量与 slice 计数严格一致, 无泄漏无超卖;
  * 并发终止同一运行任务 -> 终态幂等, 释放切片数恰好等于副本数。
"""

import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from hpc_cr.admission import (
    AdmissionManager,
    ALLOCATED,
    RUNNING,
    SUCCEEDED,
    TERMINATED,
)
from hpc_cr.errors import Conflict


class AdmissionConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.mgr = AdmissionManager()
        for i in range(8):
            self.mgr.register_machine(f"n{i}", cpus=8, memory_mb=32000)

    def test_concurrent_same_key_same_content_one_task(self):
        barrier = threading.Barrier(8)
        results: list = []
        lock = threading.Lock()

        def submit(i):
            barrier.wait()
            # 相同业务键 + 相同内容 -> 幂等返回同一任务
            r = self.mgr.submit(
                business_key="same-key", tenant_id="t", replicas=1,
                cpus_per_replica=1, memory_mb_per_replica=1000,
                payload={"data": "identical"})
            with lock:
                results.append(r)

        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(submit, range(8)))
        ids = {r["task_id"] for r in results}
        self.assertEqual(len(ids), 1)
        # 只有一个任务存在, 机器上只有一个切片
        tasks = self.mgr.list_tasks()
        self.assertEqual(len([t for t in tasks
                              if t["business_key"] == "same-key"]), 1)
        total_slices = sum(m["active_slices"] for m in
                           self.mgr.list_machines())
        self.assertEqual(total_slices, 1)

    def test_concurrent_same_key_different_content(self):
        # 先固定一个原任务
        first = self.mgr.submit(
            business_key="k", tenant_id="t", replicas=1,
            cpus_per_replica=1, memory_mb_per_replica=1000, payload={"v": 1})
        barrier = threading.Barrier(6)
        outcomes = []
        lock = threading.Lock()

        def submit(v):
            barrier.wait()
            try:
                r = self.mgr.submit(
                    business_key="k", tenant_id="t", replicas=1,
                    cpus_per_replica=1, memory_mb_per_replica=1000,
                    payload={"v": v})
                with lock:
                    outcomes.append(("ok", r["content_digest"]))
            except Conflict as exc:
                with lock:
                    outcomes.append(("conflict",
                                     getattr(exc, "reason", None)))

        with ThreadPoolExecutor(max_workers=6) as ex:
            list(ex.map(submit, range(2, 8)))
        # 原任务保持不变; 所有并发改内容请求都冲突
        self.assertTrue(all(o[0] == "conflict"
                            and o[1] == "business_key_conflict"
                            for o in outcomes))
        self.assertEqual(
            self.mgr.get_task(first["task_id"])["content_digest"],
            first["content_digest"])

    def test_concurrent_allocations_no_overcommit(self):
        # 64 个单副本任务 (每副本 1 cpu), 集群共 64 cpu -> 恰好全部接纳,
        # 每台机器 8 切片; 再多则排队, 绝不超卖。
        barrier = threading.Barrier(16)

        def submit(i):
            barrier.wait()
            return self.mgr.submit(
                business_key=f"job-{i}", tenant_id="t", replicas=1,
                cpus_per_replica=1, memory_mb_per_replica=1000)

        with ThreadPoolExecutor(max_workers=16) as ex:
            tasks = list(ex.map(submit, range(64)))
        self.assertTrue(all(t["state"] == ALLOCATED for t in tasks))
        for m in self.mgr.list_machines():
            self.assertEqual(m["used_cpus"], 8)
            self.assertEqual(m["free_cpus"], 0)
            self.assertEqual(m["active_slices"], 8)

        # 第 65 个任务无容量 -> 排队, 无超卖
        extra = self.mgr.submit(
            business_key="extra", tenant_id="t", replicas=1,
            cpus_per_replica=1, memory_mb_per_replica=1000)
        self.assertEqual(extra["state"], "QUEUED")

    def test_concurrent_terminate_idempotent_release(self):
        t = self.mgr.submit(
            business_key="gang", tenant_id="t", replicas=4,
            cpus_per_replica=2, memory_mb_per_replica=2000)
        self.assertEqual(t["state"], ALLOCATED)
        tid = t["task_id"]
        self.mgr.start_task(tid)

        barrier = threading.Barrier(8)
        states = []
        lock = threading.Lock()

        def terminate():
            barrier.wait()
            r = self.mgr.terminate_task(tid)
            with lock:
                states.append(r["state"])

        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(lambda _: terminate(), range(8)))
        self.assertTrue(all(s == TERMINATED for s in states))
        final = self.mgr.get_task(tid)
        self.assertEqual(final["state"], TERMINATED)
        # 4 个切片恰好全部归还一次
        self.assertEqual(len(final["slices"]), 4)
        self.assertTrue(all(not s["active"] for s in final["slices"]))
        # 占用该任务的机器容量完全释放
        used = {m["machine_id"]: m["active_slices"]
                for m in self.mgr.list_machines() if m["active_slices"]}
        self.assertEqual(used, {})

    def test_concurrent_terminate_and_admit(self):
        # 占满集群的任务被并发终止时, 大量等待任务被接纳; 最终总占用守恒。
        holders = []
        for i in range(8):
            r = self.mgr.submit(
                business_key=f"hold-{i}", tenant_id="h", replicas=1,
                cpus_per_replica=8, memory_mb_per_replica=32000)
            holders.append(r["task_id"])
            self.mgr.start_task(r["task_id"])
        waiters = [
            self.mgr.submit(
                business_key=f"w-{i}", tenant_id="w", replicas=1,
                cpus_per_replica=8, memory_mb_per_replica=32000)
            for i in range(8)]
        self.assertTrue(all(w["state"] == "QUEUED" for w in waiters))

        def terminate(i):
            self.mgr.terminate_task(holders[i])

        with ThreadPoolExecutor(max_workers=8) as ex:
            list(ex.map(terminate, range(8)))
        # 8 台全部释放 -> 8 个等待任务恰好各占一台
        final_waiters = [self.mgr.get_task(w["task_id"]) for w in waiters]
        admitted = [w for w in final_waiters
                    if w["state"] in (ALLOCATED, RUNNING)]
        self.assertEqual(len(admitted), 8)
        machines_used = {mid for w in admitted for mid in w["machines"]}
        self.assertEqual(len(machines_used), 8)
        total_used = sum(m["used_cpus"] for m in self.mgr.list_machines())
        self.assertEqual(total_used, 64)


if __name__ == "__main__":
    unittest.main()

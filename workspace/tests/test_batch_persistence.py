"""批接纳器: 事件溯源重启恢复 (排队项 + 已分配容量账目)。"""

import tempfile
import unittest

from hpc_cr.batch import BatchManager
from hpc_cr.batch.model import CANCELLED, QUEUED, RUNNING, TERMINATED


class BatchPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="batch-journal-")

    def _mgr(self, **kw):
        return BatchManager(data_dir=self.tmp, **kw)

    def test_queued_and_allocated_state_survives_restart(self):
        bm = self._mgr()
        bm.register_tenant("a", 2.0)
        bm.register_tenant("b", 1.0)
        bm.add_machine("m1", {"cpu": 8, "mem": 32, "gpu": 2})
        bm.add_machine("m2", {"cpu": 8, "mem": 32, "gpu": 2})
        # 一个 RUNNING (占满两台), 一个 QUEUED (需要 2 台, 此刻放不下)
        run = bm.submit_task("a", "run", 2, {"cpu": 8, "mem": 32, "gpu": 2})
        waiting = bm.submit_task("b", "wait", 2, {"cpu": 8, "mem": 32,
                                                  "gpu": 2})
        self.assertEqual(run["state"], RUNNING)
        self.assertEqual(waiting["state"], QUEUED)
        bm.journal.close()

        bm2 = self._mgr()
        run2 = bm2.get_task(run["task_id"])
        wait2 = bm2.get_task(waiting["task_id"])
        self.assertEqual(run2["state"], RUNNING)
        self.assertEqual(wait2["state"], QUEUED)
        # 机器占用账目精确恢复 (容量没有在重启时丢失或重复)
        machines = {m["machine_id"]: m for m in bm2.list_machines()}
        self.assertEqual(machines["m1"]["used"],
                         {"cpu": 8, "mem": 32, "gpu": 2})
        self.assertEqual(machines["m2"]["used"],
                         {"cpu": 8, "mem": 32, "gpu": 2})
        self.assertEqual(
            sorted(a["machine_id"] for a in run2["allocations"]),
            ["m1", "m2"])
        # 业务键幂等索引也恢复: 同键同内容返回原排队项
        again = bm2.submit_task("b", "wait", 2,
                                {"cpu": 8, "mem": 32, "gpu": 2})
        self.assertTrue(again["idempotent"])
        self.assertEqual(again["task_id"], waiting["task_id"])
        # 释放占用后, 等待项在新进程状态下可以被成组启动
        bm2.terminate_task(run["task_id"])
        started = bm2.get_task(waiting["task_id"])
        self.assertEqual(started["state"], RUNNING)
        self.assertEqual(len(started["allocations"]), 2)
        bm2.journal.close()

    def test_terminal_tasks_and_idempotency_survive_restart(self):
        bm = self._mgr()
        bm.register_tenant("a")
        bm.add_machine("m1", {"cpu": 4, "mem": 4, "gpu": 0})
        t = bm.submit_task("a", "k", 1, {"cpu": 1})
        bm.complete_task(t["task_id"], {"artifact": "ok"})
        t2 = bm.submit_task("a", "k2", 1, {"cpu": 1})
        bm.terminate_task(t2["task_id"], reason="manual")
        bm.journal.close()

        bm2 = self._mgr()
        got = bm2.submit_task("a", "k", 1, {"cpu": 1})
        self.assertTrue(got["idempotent"])
        self.assertEqual(got["state"], "COMPLETED")
        self.assertEqual(got["result"], {"artifact": "ok"})
        got2 = bm2.get_task(t2["task_id"])
        self.assertEqual(got2["state"], TERMINATED)
        self.assertEqual(got2["finish_reason"], "manual")
        # 已终止任务的容量恢复后可用
        self.assertEqual(bm2.list_machines()[0]["used"],
                         {"cpu": 0, "mem": 0, "gpu": 0})
        # 幂等终止/归还不二次扣减
        bm2.terminate_task(t2["task_id"])
        bm2.release_resources(t2["task_id"])
        self.assertEqual(bm2.list_machines()[0]["used"],
                         {"cpu": 0, "mem": 0, "gpu": 0})
        bm2.journal.close()

    def test_restart_does_not_replay_duplicate_allocations(self):
        # 崩溃恢复不变量: 每个 (任务, 机器) 账目至多一条
        bm = self._mgr()
        bm.register_tenant("a")
        bm.add_machine("m1", {"cpu": 8, "mem": 8, "gpu": 0})
        bm.add_machine("m2", {"cpu": 8, "mem": 8, "gpu": 0})
        t = bm.submit_task("a", "gang", 2, {"cpu": 2})
        self.assertEqual(t["state"], RUNNING)
        bm.journal.close()
        bm2 = self._mgr()
        for m in bm2.list_machines():
            self.assertEqual(len(m["allocations"]), 1)
        bm3 = self._mgr()  # 再次重启仍然不重复
        for m in bm3.list_machines():
            self.assertEqual(len(m["allocations"]), 1)
        bm2.journal.close()
        bm3.journal.close()

    def test_batch_journal_is_separate_from_job_journal(self):
        # 批接纳器使用独立 <data_dir>/batch/journal.jsonl
        import os
        bm = self._mgr()
        bm.register_tenant("a")
        bm.journal.close()
        self.assertTrue(
            os.path.exists(os.path.join(self.tmp, "batch",
                                        "journal.jsonl")))


if __name__ == "__main__":
    unittest.main()

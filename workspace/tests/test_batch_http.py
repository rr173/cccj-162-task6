"""批接纳器 HTTP API、页面与并发幂等。"""

import http.client
import json
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor

from hpc_cr.api import build_server
from hpc_cr.batch import BatchManager


def _free_port_server(**batch_kw):
    from hpc_cr.epochs import Scheduler
    from hpc_cr.jobs import JobManager
    from hpc_cr.security import Signer

    mgr = JobManager(Scheduler(Signer("batch-test-secret")))
    bm = BatchManager(**batch_kw)
    srv = build_server(mgr, host="127.0.0.1", port=0,
                       batch_manager=bm, enable_batch=True)
    return srv, bm


class BatchHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="batch-http-")
        cls.srv, cls.bm = _free_port_server(data_dir=cls.tmp)
        cls.port = cls.srv.server_address[1]
        cls.thread = threading.Thread(target=cls.srv.serve_forever,
                                      daemon=True)
        cls.thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.srv.shutdown()
        cls.srv.server_close()

    def _req(self, method, path, obj=None, raw_body=None, ctype=None):
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        headers = {}
        body = None
        if obj is not None:
            body = json.dumps(obj).encode()
            headers["Content-Type"] = "application/json"
        elif raw_body is not None:
            body = raw_body
            headers["Content-Type"] = ctype or "application/json"
        conn.request(method, path, body=body, headers=headers)
        resp = conn.getresponse()
        data = resp.read()
        parsed = json.loads(data) if data and resp.getheader(
            "Content-Type", "").startswith("application/json") else data
        conn.close()
        return resp.status, parsed

    def test_full_lifecycle_and_ui(self):
        self.assertEqual(self._req("POST", "/api/batch/tenants",
                                   {"tenant_id": "t1", "weight": 2})[0], 201)
        self.assertEqual(self._req("POST", "/api/batch/machines", {
            "machine_id": "h1", "cpu": 8, "mem": 32, "gpu": 1,
            "labels": {"gpu": "a100"}})[0], 201)
        status, t = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t1", "business_key": "bk1", "machine_count": 1,
            "need": {"cpu": 2, "mem": 4, "gpu": 1}})
        self.assertEqual(status, 201)
        self.assertEqual(t["state"], "RUNNING")
        tid = t["task_id"]

        # GET 视图
        self.assertEqual(self._req("GET", "/api/batch/dashboard")[0], 200)
        self.assertEqual(self._req("GET", "/api/batch/tenants")[0], 200)
        self.assertEqual(self._req("GET", "/api/batch/machines")[0], 200)
        self.assertEqual(self._req("GET", "/api/batch/tasks")[0], 200)
        self.assertEqual(self._req("GET", f"/api/batch/tasks/{tid}")[0], 200)
        self.assertEqual(self._req("GET", "/api/batch/passes")[0], 200)
        for path in ("/ui/batch", f"/ui/batch/tasks/{tid}"):
            status, body = self._req("GET", path)
            self.assertEqual(status, 200)
            self.assertIn(b"<html", body)

        # 同键同内容 -> 201 幂等; 同键异内容 -> 409
        s, again = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t1", "business_key": "bk1", "machine_count": 1,
            "need": {"cpu": 2, "mem": 4, "gpu": 1}})
        self.assertTrue(again["idempotent"])
        s, err = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t1", "business_key": "bk1", "machine_count": 2,
            "need": {"cpu": 2, "mem": 4, "gpu": 1}})
        self.assertEqual(s, 409)
        self.assertEqual(err["reason"], "business_key_conflict")

        # 终止幂等
        self.assertEqual(
            self._req("POST", f"/api/batch/tasks/{tid}/terminate", {})[0],
            200)
        s, second = self._req(
            "POST", f"/api/batch/tasks/{tid}/terminate", {})
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["state"], "TERMINATED")

    def test_admission_rejection_and_defer_reasons(self):
        self._req("POST", "/api/batch/tenants", {"tenant_id": "t2"})
        self._req("POST", "/api/batch/machines",
                  {"machine_id": "small1", "cpu": 2, "mem": 2, "gpu": 0})
        # 超总量 -> 409 + 稳定 reason
        s, err = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t2", "business_key": "toobig", "machine_count": 1,
            "need": {"cpu": 99}})
        self.assertEqual(s, 409)
        self.assertEqual(err["reason"], "cluster_cpu_exceeded")
        # 瞬时碎片 -> 201 入队 + last_defer
        occ = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t2", "business_key": "occ", "machine_count": 1,
            "need": {"cpu": 2}})[1]
        self.assertEqual(occ["state"], "RUNNING")
        s, waiting = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t2", "business_key": "w", "machine_count": 1,
            "need": {"cpu": 1}})
        self.assertEqual(s, 201)
        self.assertEqual(waiting["state"], "QUEUED")
        self.assertEqual(
            waiting["last_defer"]["code"], "insufficient_free_capacity")
        # 400 校验错误
        s, err = self._req("POST", "/api/batch/tasks", {
            "tenant_id": "t2", "business_key": "bad", "machine_count": 0,
            "need": {"cpu": 1}})
        self.assertEqual(s, 400)
        # 404
        self.assertEqual(
            self._req("GET", "/api/batch/tasks/nope")[0], 404)

    def test_form_submission_and_redirect(self):
        body = "tenant_id=t3&weight=1"
        s, _ = self._req(
            "POST", "/ui/batch/forms/tenant",
            raw_body=body.encode(),
            ctype="application/x-www-form-urlencoded")
        # 表单成功走 303 重定向 (无响应体)
        self.assertEqual(s, 303)
        s, tenants = self._req("GET", "/api/batch/tenants")
        self.assertIn("t3", [t["tenant_id"] for t in tenants["tenants"]])

    def test_batch_disabled_returns_404_without_affecting_jobs(self):
        from hpc_cr.epochs import Scheduler
        from hpc_cr.jobs import JobManager
        from hpc_cr.security import Signer

        mgr = JobManager(Scheduler(Signer("x")))
        srv = build_server(mgr, host="127.0.0.1", port=0,
                           enable_batch=False)
        port = srv.server_address[1]
        th = threading.Thread(target=srv.serve_forever, daemon=True)
        th.start()
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/api/batch/dashboard")
            self.assertEqual(conn.getresponse().status, 404)
            conn.close()
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
            conn.request("GET", "/api/jobs")
            self.assertEqual(conn.getresponse().status, 200)
            conn.close()
        finally:
            srv.shutdown()
            srv.server_close()


class BatchConcurrencyTest(unittest.TestCase):
    def test_concurrent_duplicate_submit_starts_once(self):
        bm = BatchManager()
        bm.register_tenant("a", 1.0)
        bm.add_machine("m1", {"cpu": 1, "mem": 1, "gpu": 0})

        def submit(i):
            return bm.submit_task("a", "same-key", 1, {"cpu": 1})

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(submit, range(16)))
        task_ids = {r["task_id"] for r in results}
        self.assertEqual(len(task_ids), 1)
        running = bm.list_tasks(state="RUNNING")
        self.assertEqual(len(running), 1)
        self.assertEqual(len(bm.list_machines()[0]["allocations"]), 1)

    def test_concurrent_terminate_and_complete_settles_once(self):
        bm = BatchManager()
        bm.register_tenant("a", 1.0)
        bm.add_machine("m1", {"cpu": 2, "mem": 2, "gpu": 0})
        t = bm.submit_task("a", "k", 1, {"cpu": 1})
        outcomes = []

        def op(which):
            try:
                if which % 2 == 0:
                    return ("complete", bm.complete_task(t["task_id"]))
                return ("terminate", bm.terminate_task(t["task_id"]))
            except Exception as exc:  # noqa: BLE001
                return ("conflict", getattr(exc, "reason", str(exc)))

        with ThreadPoolExecutor(max_workers=12) as pool:
            outcomes = list(pool.map(op, range(12)))
        final = bm.get_task(t["task_id"])["state"]
        self.assertIn(final, ("COMPLETED", "TERMINATED"))
        winner = {o[0] for o in outcomes if o[0] != "conflict"}
        self.assertEqual(len(winner), 1)  # 恰有一个操作生效
        # 容量账目无论谁赢都已清空且不重复
        self.assertEqual(bm.list_machines()[0]["allocations"], [])

    def test_concurrent_gang_submits_no_overcommit(self):
        # 2 台机器、每个任务都要 2 台 => 任意时刻最多 1 个 RUNNING,
        # 容量绝不超卖, 且机器账目每台至多一条
        bm = BatchManager()
        bm.register_tenant("a", 1.0)
        bm.add_machine("m1", {"cpu": 4, "mem": 4, "gpu": 0})
        bm.add_machine("m2", {"cpu": 4, "mem": 4, "gpu": 0})

        def submit(i):
            return bm.submit_task("a", f"gang-{i}", 2, {"cpu": 4})

        with ThreadPoolExecutor(max_workers=16) as pool:
            results = list(pool.map(submit, range(16)))
        running = [r for r in results if r["state"] == "RUNNING"]
        self.assertEqual(len(running), 1)
        for m in bm.list_machines():
            self.assertLessEqual(len(m["allocations"]), 1)


if __name__ == "__main__":
    unittest.main()

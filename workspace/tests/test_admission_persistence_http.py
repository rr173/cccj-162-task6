"""接纳器持久化与重启恢复 + HTTP 端到端。

验证:
  * 程序重新拉起后找回等待项 (QUEUED) 与已分配但未归还的容量 (slice);
  * 重启后终止/完成能正确归还重建出的容量, 排队项继续推进;
  * journal 与作业子系统物理隔离 (data_dir/admission/admission-journal.jsonl);
  * HTTP 接口可运行: 租户/机器/任务/调度/撤回/终止/完成 + 页面可打开;
  * 既有作业 API 与页面在同一服务器上行为不变。
"""

import json
import os
import shutil
import tempfile
import unittest
import urllib.error
import urllib.request

from hpc_cr.admission import AdmissionManager
from hpc_cr.admission.journal import ADMISSION_JOURNAL_NAME
from hpc_cr.api import build_server


class RestartPersistenceTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="adm-persist-")
        self.addCleanup(shutil.rmtree, self.dir, ignore_errors=True)

    def _mgr(self):
        return AdmissionManager(self.dir)

    def test_replay_recovers_queued_and_held_capacity(self):
        m = self._mgr()
        m.upsert_tenant("alpha", weight=3, name="Alpha")
        m.register_machine("n1", cpus=8, memory_mb=32000,
                           accelerators={"a100": 2})
        m.register_machine("n2", cpus=8, memory_mb=32000)
        # 已分配 (ALLOCATED) 任务
        t = m.submit(business_key="gang", tenant_id="alpha", replicas=2,
                     cpus_per_replica=4, memory_mb_per_replica=8000,
                     accelerators_per_replica={"a100": 1})
        self.assertEqual(t["state"], "ALLOCATED")
        tid = t["task_id"]
        # 排队任务 (需 2 副本×4cpu 带 a100; 唯一带卡的 n1 已被占满)
        m.start_task(tid)
        q = m.submit(business_key="queued", tenant_id="alpha", replicas=2,
                     cpus_per_replica=4, memory_mb_per_replica=8000,
                     accelerators_per_replica={"a100": 1})
        self.assertEqual(q["state"], "QUEUED")
        qid = q["task_id"]

        # journal 文件隔离存在
        self.assertTrue(os.path.exists(
            os.path.join(self.dir, ADMISSION_JOURNAL_NAME)))

        # ---- 重启
        m2 = self._mgr()
        rt = m2.get_task(tid)
        self.assertEqual(rt["state"], "RUNNING")  # start 已落盘
        self.assertEqual(sorted(rt["machines"]), ["n1"])
        self.assertEqual(len(rt["slices"]), 2)
        self.assertTrue(all(s["active"] for s in rt["slices"]))
        # 容量确实被占用
        n1 = next(x for x in m2.list_machines()
                  if x["machine_id"] == "n1")
        self.assertEqual(n1["free_cpus"], 0)
        self.assertEqual(n1["active_slices"], 2)
        # 租户权重找回
        tenant = next(x for x in m2.list_tenants()
                      if x["tenant_id"] == "alpha")
        self.assertEqual(tenant["weight"], 3)
        # 排队项找回
        self.assertEqual(m2.get_task(qid)["state"], "QUEUED")

        # 重启后完成运行任务 -> 容量归还 -> 排队项自动推进到 n1
        done = m2.complete_task(tid, result={"answer": 7})
        self.assertEqual(done["state"], "SUCCEEDED")
        rq = m2.get_task(qid)
        self.assertEqual(rq["state"], "ALLOCATED")
        self.assertEqual(rq["machines"], ["n1"])
        # 容量没有泄漏: 释放出的 8cpu 现在由排队项持有
        n1b = next(x for x in m2.list_machines()
                   if x["machine_id"] == "n1")
        self.assertEqual(n1b["active_slices"], 2)
        self.assertEqual(n1b["free_cpus"], 0)
        # 终止排队项后容量完全归还
        m2.terminate_task(qid)
        n1c = next(x for x in m2.list_machines()
                   if x["machine_id"] == "n1")
        self.assertEqual(n1c["free_cpus"], 8)
        self.assertEqual(n1c["active_slices"], 0)

    def test_replay_terminate_then_capacity_free(self):
        m = self._mgr()
        m.register_machine("n1", cpus=4, memory_mb=16000)
        t = m.submit(business_key="k", tenant_id="auto", replicas=1,
                     cpus_per_replica=4, memory_mb_per_replica=16000)
        tid = t["task_id"]
        m.terminate_task(tid)
        m2 = self._mgr()
        rt = m2.get_task(tid)
        self.assertEqual(rt["state"], "TERMINATED")
        n1 = m2.list_machines()[0]
        self.assertEqual(n1["active_slices"], 0)
        self.assertEqual(n1["free_cpus"], 4)
        # 幂等: 重启后再次终止同一任务
        again = m2.terminate_task(tid)
        self.assertTrue(again["idempotent_replay"])

    def test_replay_recovers_rejected_and_result(self):
        m = self._mgr()
        m.register_machine("n1", cpus=1, memory_mb=100)
        bad = m.submit(business_key="bad", tenant_id="x", replicas=1,
                       cpus_per_replica=99, memory_mb_per_replica=100)
        self.assertEqual(bad["state"], "REJECTED")
        good = m.submit(business_key="good", tenant_id="x", replicas=1,
                        cpus_per_replica=1, memory_mb_per_replica=100)
        m.start_task(good["task_id"])
        m.complete_task(good["task_id"], result={"v": "done"})
        m2 = self._mgr()
        self.assertEqual(m2.get_task(bad["task_id"])["state"], "REJECTED")
        rg = m2.get_task(good["task_id"])
        self.assertEqual(rg["state"], "SUCCEEDED")
        self.assertEqual(rg["result"], {"v": "done"})


def _req(method, url, body=None):
    data = None
    headers = {"Content-Type": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode())


class AdmissionHttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.mkdtemp(prefix="adm-http-")
        adm_dir = os.path.join(cls.tmp, "admission")
        cls.adm = AdmissionManager(adm_dir)
        cls.server = build_server(host="127.0.0.1", port=0,
                                  admission=cls.adm)
        import threading
        cls.thread = threading.Thread(
            target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def _url(self, path):
        return self.base + path

    def test_full_flow_over_http(self):
        # 租户
        st, _ = _req("POST", self._url("/api/admission/tenants"),
                     {"tenant_id": "lab-a", "weight": 2})
        self.assertEqual(st, 201)
        # 机器
        st, _ = _req("POST", self._url("/api/admission/machines"),
                     {"machine_id": "g1", "cpus": 8, "memory_mb": 32000,
                      "accelerators": {"a100": 2}})
        self.assertEqual(st, 201)
        st, _ = _req("POST", self._url("/api/admission/machines"),
                     {"machine_id": "c1", "cpus": 4, "memory_mb": 16000})
        self.assertEqual(st, 201)
        # 先占住 g1 的两块卡 (2 个 1卡 副本)
        st, hold = _req("POST", self._url("/api/admission/tasks"),
                        {"business_key": "hold-gpu", "tenant_id": "lab-a",
                         "replicas": 2, "cpus_per_replica": 2,
                         "memory_mb_per_replica": 4000,
                         "accelerators_per_replica": {"a100": 1}})
        self.assertEqual(st, 201)
        self.assertEqual(hold["state"], "ALLOCATED")
        # 提交任务 (2 副本带卡 -> g1 的卡已被占满, 排队)
        st, big = _req("POST", self._url("/api/admission/tasks"),
                       {"business_key": "train-7b", "tenant_id": "lab-a",
                        "replicas": 2, "cpus_per_replica": 4,
                        "memory_mb_per_replica": 8000,
                        "accelerators_per_replica": {"a100": 1}})
        self.assertEqual(st, 201)
        self.assertEqual(big["state"], "QUEUED")
        # 小任务在 c1 上立即接纳 (不被大任务挡住)
        st, small = _req("POST", self._url("/api/admission/tasks"),
                         {"business_key": "prep", "tenant_id": "lab-a",
                          "replicas": 1, "cpus_per_replica": 2,
                          "memory_mb_per_replica": 4000})
        self.assertEqual(st, 201)
        self.assertEqual(small["state"], "ALLOCATED")
        self.assertEqual(small["machines"], ["c1"])
        # 同键同内容幂等
        st, again = _req("POST", self._url("/api/admission/tasks"),
                         {"business_key": "prep", "tenant_id": "lab-a",
                          "replicas": 1, "cpus_per_replica": 2,
                          "memory_mb_per_replica": 4000})
        self.assertEqual(st, 201)
        self.assertEqual(again["task_id"], small["task_id"])
        self.assertTrue(again["idempotent_replay"])
        # 同键内容变 -> 409
        st, err = _req("POST", self._url("/api/admission/tasks"),
                       {"business_key": "prep", "tenant_id": "lab-a",
                        "replicas": 1, "cpus_per_replica": 3,
                        "memory_mb_per_replica": 4000})
        self.assertEqual(st, 409)
        self.assertEqual(err["reason"], "business_key_conflict")
        # 撤回排队项
        st, canc = _req("POST",
                        self._url(f"/api/admission/tasks/{big['task_id']}/cancel"),
                        {"reason": "plan changed"})
        self.assertEqual(st, 200)
        self.assertEqual(canc["state"], "CANCELLED")
        st, canc2 = _req("POST",
                         self._url(f"/api/admission/tasks/{big['task_id']}/cancel"),
                         {})
        self.assertTrue(canc2["idempotent_replay"])
        # 终止已分配的小任务 -> 容量归还
        st, term = _req("POST",
                        self._url(f"/api/admission/tasks/{small['task_id']}/terminate"),
                        {})
        self.assertEqual(term["state"], "TERMINATED")
        st, machines = _req("GET", self._url("/api/admission/machines"))
        c1 = next(m for m in machines["machines"]
                  if m["machine_id"] == "c1")
        self.assertEqual(c1["free_cpus"], 4)
        self.assertEqual(c1["active_slices"], 0)

    def test_pages_render(self):
        with urllib.request.urlopen(self._url("/ui/admission"),
                                    timeout=5) as r:
            html_text = r.read().decode()
        self.assertIn("多租户", html_text)
        # overview JSON
        st, ov = _req("GET", self._url("/api/admission/overview"))
        self.assertEqual(st, 200)
        self.assertIn("machines", ov)

    def test_existing_job_api_still_works(self):
        # 既有作业接口在同一服务器上不受影响
        st, job = _req("POST", self._url("/api/jobs"),
                       {"name": "existing", "world_size": 2,
                        "shards_per_rank": 1})
        self.assertEqual(st, 201)
        jid = job["job_id"]
        st, snap = _req("GET", self._url(f"/api/jobs/{jid}"))
        self.assertEqual(st, 200)
        self.assertEqual(snap["state"], "RUNNING")
        with urllib.request.urlopen(
                self._url(f"/ui/jobs/{jid}"), timeout=5) as r:
            self.assertEqual(r.status, 200)

    def test_unknown_routes_404(self):
        st, _ = _req("GET", self._url("/api/admission/nope"))
        self.assertEqual(st, 404)
        st, _ = _req("GET", self._url("/api/admission/tasks/nope"))
        self.assertEqual(st, 404)


if __name__ == "__main__":
    unittest.main()

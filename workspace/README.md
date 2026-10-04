# HPC 作业抢占与检查点恢复服务

实现调度器回收节点场景下的**抢占纪元签发、内容寻址检查点、全量校验释放、
同代际恢复与输出租约栅栏**。纯 Python 标准库，无外部依赖。

## 核心不变量

| # | 不变量 | 实现位置 |
|---|--------|----------|
| 1 | 回收前签发**抢占纪元**（单调 epoch/generation + HMAC 签名令牌）；重复抢占通知幂等，不轮换令牌 | `epochs.py`、`jobs.py::preempt` |
| 2 | 检查点分片为**内容寻址**（`sha256(payload)`），相同分片跨代际天然去重 | `storage.py::ContentStore` |
| 3 | 期望分片集合在开检查点时由「进程集合 × 每进程分片数」固定；提交时**逐个从 CAS 重读重算哈希**，缺片/损坏一律拒绝，检查点停留在 `PENDING`，绝不提前可用 | `checkpoints.py::commit` |
| 4 | **只有全部校验完成**（`COMPLETE`）才允许确认释放；不完整只能 `abandon`（`INCOMPLETE`/`ABORTED`） | `jobs.py::confirm_release/abort_epoch` |
| 5 | **恢复必须使用同一检查点代际**：恢复时再次全量重校验、核对进程集合、回传每个分片输入位点，校验通过才签发**新的输出租约** | `jobs.py::restore`、`leases.py` |
| 6 | 抢占瞬间吊销旧租约；旧进程迟到写入（旧 generation/已 REVOKED 令牌）返回 `412 FenceViolation`；输出按 `(generation, output_id)` 幂等去重 —— 无双重输出 | `leases.py::authorize`、`storage.py::OutputLog` |
| 7 | 节点失联：节点与 rank 标记 `LOST`，其分片不被接受、提交被阻塞；心跳恢复后 incarnation+1，补齐方可整体提交 | `jobs.py::mark_node_lost/heartbeat` |
| 8 | 新一轮抢占使旧的未完成检查点 `SUPERSEDED`；旧纪元令牌不能写新代际 | `checkpoints.py`、`jobs.py` |
| 9 | **结果提交授权失败统一处理**：缺失/伪造/超期/撤销/跨作业/旧代际凭证一律返回稳定错误（固定状态码+`reason`+固定消息，不回显凭证），并写入一条**顺序化安全事件**（全局单调 seq，不含凭证原文）；授权在写入前终止，重复并发请求不增加结果条目 | `leases.py::authorize`、`jobs.py::_deny` |
| 10 | **事件溯源持久化**：所有状态变更（含安全事件、输出条目、恢复报告、GC 计划/删除）顺序追加到 `data_dir/journal.jsonl`，blob 原子落盘；服务重启按 seq 重放重建，页面与验证报告仍可查询，旧令牌仍被栅栏，同密钥重新派生 | `persistence.py`、`jobs.py::_replay` |
| 11 | **可审计空间回收**：GC 先生成带 journal 水位、保留代际、引用指纹、预计释放量和候选明细的计划；可锁定/解锁代际；计划生成后引用关系变化则拒绝执行；上传/恢复/回收共享全局引用临界区，绝不删除仍可访问数据；RUNNING 计划崩溃重启后可继续，每个删除动作和跳过/恢复原因均可查询 | `gc.py`、`jobs.py::create_gc_plan/execute_gc_plan` |
| 12 | **可恢复检查点迁移**：导出端冻结对象清单（逐项 sha256+清单摘要，源载荷逐个重读重算）并临时保留涉及 blob；接收端按摘要复用已有对象、缺失才传输，每项两端重算哈希；全部内容校验通过后才一次性原子发布可恢复代际；传输中断、重复请求、服务重启后继续同一任务；清单变化或摘要错误绝不产生可恢复代际；取消/失败即释放临时保留且不阻碍正常 GC；接收端凭发布清单 adopt 后可同代际恢复 | `migration.py`、`jobs.py::create_migration/transfer_migration/finalize_migration/adopt_generation` |

## 状态机

```
作业:   RUNNING ──preempt──▶ PREEMPTING ──release(仅COMPLETE)──▶ RELEASED
                                  │                                   │
                                  └──abandon──▶ ABORTED              │
                                      ▲                              │
        RELEASED ──restore(同代际+重校验+新租约)──▶ RUNNING ◀─────────┘

检查点: PENDING ──全部分片重校验通过──▶ COMPLETE      (可重复 commit: 幂等)
            ├── 缺片/损坏: 提交拒绝, 保持 PENDING
            ├── 新纪元接管: SUPERSEDED
            └── 宽限期耗尽放弃: INCOMPLETE
```

## 运行

```bash
# 测试
python3 -m unittest discover -s tests -v        # 77 个测试, 含 GC/迁移/并发竞争与重启持久化

# 端到端演示 (打印恢复验证报告 Markdown)
python3 demo.py
# 持久化演示 (状态/安全事件落盘, 重跑自动重放)
python3 demo.py --data-dir ./hpc-data
# 演示后继续提供 HTTP 服务, 浏览器打开节点进度页
python3 demo.py --serve --port 8080
#   http://127.0.0.1:8080/ui/jobs/climate-7

# 空服务 (纯内存, 重启不保留)
python3 -m hpc_cr            # 默认 127.0.0.1:8080
# 持久化服务 (事件日志 + blob + 自动生成 0600 签名密钥)
python3 -m hpc_cr --data-dir ./hpc-data
# 也可用环境变量 HPC_CR_SECRET 固定签名密钥 (重启后旧令牌才可继续验证)
```

## 作业 API

所有写操作为 `POST /api/jobs/{id}`，JSON body 用 `action` 区分；
凭证走请求头：`X-Epoch-Token`（抢占纪元令牌）、`X-Lease-Token`（输出租约令牌）。
二进制数据以 `data_b64`（标准 base64）传递。

| 方法/路径 | 说明 |
|---|---|
| `POST /api/jobs` | 创建作业（world_size、shards_per_rank、rank→node 放置） |
| `GET  /api/jobs` / `GET /api/jobs/{id}` | 作业列表 / 快照（纪元、宽限剩余、当前检查点、租约） |
| `action=preempt` | 签发抢占纪元；重复通知幂等，`force_new_epoch:true` 表示新一轮抢占 |
| `action=heartbeat` | 节点心跳（LOST→UP，进程 incarnation+1） |
| `action=mark_node_lost` | 标记节点失联，阻塞其分片 |
| `action=progress` | 上报 rank 输入位点 |
| `action=upload_shard` + `X-Epoch-Token` | 上传分片（rank/index/node/input_pos/data_b64），重复同内容幂等 |
| `action=commit_checkpoint` + `X-Epoch-Token` | 全量重校验提交；缺片/损坏返回 409 |
| `action=confirm_release` + `X-Epoch-Token` | 确认释放；非 COMPLETE 返回 409 |
| `action=abandon` | 宽限耗尽放弃，检查点 INCOMPLETE |
| `action=append_output` + `X-Lease-Token` | 提交输出；旧代际/吊销租约 412；同 output_id 幂等 |
| `action=restore` `{generation}` | 同代际恢复验证，返回恢复验证报告 + 新租约 |
| `action=pin_generation` `{generation, unpin, reason}` | 锁定/解锁需要保留的检查点/输出代际；变更会推进引用水位，使旧 GC 计划过期 |
| `POST /api/gc/plans` | 生成 GC 计划（`policy`、`min_retain_generation`、`retain_generations`、`include_pins`），返回预计释放量、候选 blob、保留原因、引用指纹和水位，不删除数据 |
| `POST /api/gc/plans/{id}` `{action:"execute"}` | 按计划执行；水位推进、引用变化或物理 blob 集合变化时返回 409 并把计划标记 `STALE` |
| `GET  /api/gc/plans` / `GET /api/gc/plans/{id}` | 全部计划 / 单计划查询：预计/实际删除项、字节、跳过和崩溃恢复原因、最终状态 |
| `GET  /api/jobs/{id}/gc/pins` | 查询作业已锁定代际 |
| `POST /api/migration-destinations` | 注册接收端内容存储（`destination_id`、`data_dir`），磁盘后端重启自动重建 |
| `POST /api/migrations` | 创建迁移任务（`job_id`、`generation`、`destination_id`）：导出端冻结清单+源端重校验+临时保留；同 (作业,代际,接收端) 活跃任务重复请求幂等 |
| `POST /api/migrations/{id}` `{action:"transfer"}` | 续传/传输：`object_ids` 指定对象、`limit` 限本次项数、`auto_finalize`（默认 true 全部完成即发布）；按摘要复用，每项两端重算哈希，中断/重复/重启续传 |
| `POST /api/migrations/{id}` `{action:"finalize"}` | 全量闸门：清单重检+全部对象目标端重读重算，通过才一次性发布可恢复代际；否则 FAILED 不发布 |
| `POST /api/migrations/{id}` `{action:"cancel"}` | 取消活跃任务：不发布代际，立即释放临时保留（终态幂等） |
| `GET  /api/migrations` / `GET /api/migrations/{id}` | 全部/单个迁移任务：计划总量/字节、已传输项、复用项、失败项、逐项状态、最终状态与原因 |
| `GET  /api/published-generations` | 已发布可恢复代际目录（可按 `destination_id`/`job_id` 过滤），含 adopt 所需完整清单 |
| `POST /api/adoptions` | 接收端落地：`manifest`（跨集群）或本地 `migration_id` + `destination_id`；全量内容校验通过才生成 RELEASED 作业的 COMPLETE 代际（可 `restore`），否则拒绝 |
| `GET  /api/migration-destinations` | 已注册接收端存储及其 blob 统计 |
| `GET  /api/jobs/{id}/progress.json` | 进度页聚合数据（节点/进程/分片矩阵/输出/审计） |
| `GET  /api/jobs/{id}/checkpoints` | 全部代际检查点 |
| `GET  /api/jobs/{id}/outputs` | 输出条目（按代际隔离） |
| `GET  /api/jobs/{id}/security-events` | **授权失败安全事件**（顺序化 seq/原因/绑定字段，不含凭证原文） |
| `GET  /api/jobs/{id}/restore/report` | 最近一次恢复验证报告（JSON，含截至当前的安全事件列表） |
| `GET  /ui/jobs/{id}` | **节点进度页面**（2s 自动刷新，含安全事件表） |

### 授权失败稳定错误 (`append_output` 等凭证接口)

错误响应固定为 `{error, reason, message}`，同一类拒绝永远得到同一三元组，
消息不随请求/令牌变化，不回显任何凭证原文：

| 场景 | HTTP | error | reason |
|---|---|---|---|
| 缺失凭证头 | 401 | invalid_token | `missing_token` |
| 伪造/篡改/格式损坏令牌 | 401 | invalid_token | `bad_signature` / `malformed_token` / `bad_token_version` / `undecodable_payload` |
| 令牌类型错用（epoch 充当 lease） | 401 | invalid_token | `wrong_token_type` |
| 已签发但租约 ID 不可识别 | 401 | invalid_token | `unknown_lease` |
| 旧代际凭证迟到写入 | 412 | fence_violation | `stale_generation` |
| 跨作业凭证 | 412 | fence_violation | `cross_job_token` |
| 抢占时已撤销租约 | 412 | fence_violation | `lease_revoked` |
| 超过租约/令牌有效期 | 412 | fence_violation | `lease_expired` |
| 纪元令牌跨作业/旧纪元 | 412 | fence_violation | `epoch_cross_job_or_epoch` |

每次拒绝同步写入一条 `security_authz_denied` 事件：包含全局单调 `seq`、
`credential`（`output_lease`/`epoch_token`）、`operation`、`reason`、
`http_status`、`expected_generation` 与仅可公开的绑定字段 `claimed`
（job/generation/lease id），**绝不包含令牌串或签名**。授权在校验通过前
终止，blob 与 OutputLog 不发生任何写入，故重复并发的无效/重复请求不会
增加结果条目（合法重复提交仍按 `(generation, output_id)` 幂等）。

其余错误映射：`400` 请求格式、`404` 对象不存在、
`409` 状态冲突（缺片提交/重复分片/状态机）。

## 恢复验证报告

`restore()` 与 `/restore/report` 返回结构化报告（`RestoreReport`），并可用
`report.to_markdown()` 渲染，包含：清单摘要、分片重算哈希结果（逐个列出失败
分片）、每个分片的**输入位点 seek 表**、旧/新进程放置与迁移 rank、新输出租约、
**安全事件表（授权拒绝证据，读取时补齐到最新）**、警告；任一检查项失败时
`success=false` 且作业保持 `RELEASED`，不会带着坏检查点运行。报告随事件日志
持久化，服务重启后仍可查询。

## 空间回收 (GC) 语义

回收采用 **计划 → 锁定/复核 → 执行 → 查询** 的显式流程:

- 计划生成在全局引用临界区和全部作业锁内完成, 记录:
  `watermark_seq`（引用类事件水位）、全部逻辑引用关系的
  `reference_fingerprint`、每代际保留原因、物理 blob 清单、候选 blob、
  `estimated_reclaim_items/bytes`。计划创建本身只追加 `gc_plan_created`。
- 默认可恢复策略 (`recoverable`) 保留当前抢占/PENDING 检查点、
  已释放待恢复检查点、运行中输出代际及其输出; `all_complete`
  保留所有 COMPLETE 检查点; `watermark + min_retain_generation`
  保留指定水位之后的代际。`pin_generation` 可持久化锁定任意代际。
- 执行时重新验证水位、引用指纹和物理集合: 计划生成后发生上传、
  抢占、释放、放弃、恢复、新输出、代际 pin/unpin 或出现未纳管 blob,
  一律返回 409，计划转为 `STALE`，不会删除任何数据。
- 每个 blob 删除成功后独立追加 `gc_blob_deleted`；计划完成追加
  `gc_run_finished`，包含实际删除项/字节、`skip_reasons` 和最终状态。
  若崩溃发生在物理 unlink 与删除事件之间，重启后继续同一计划时该项计入
  实际删除，并记录恢复原因 `already_deleted_after_crash`。
- 上传分片、提交输出、恢复和 GC 扫描/执行共享全局引用锁，因此不会看到
  "blob 已落盘但元数据引用尚未建立" 的中间态；正在执行的计划会阻止生成
  新计划，COMPLETED 计划重复执行幂等。

## 检查点迁移语义 (跨内容存储, 可恢复)

集群下线或作业搬迁时, 把一个已 COMPLETE 的检查点及其同代际输出迁到另一套
内容存储。迁移是显式的 **注册接收端 → 冻结清单 → 传输/复用 → 全量校验 →
一次性发布** 流程:

- **冻结清单 + 临时保留**: 创建任务时在全局引用临界区内枚举该代际全部分片
  与输出, 记录逐项 `sha256/大小` 与整体 *清单摘要*; 同时对源载荷逐个从
  CAS 重读重算哈希, 缺片/损坏直接拒绝导出 (不建任务、不加保留)。任务涉及
  的 blob 在终态前以 migration hold 临时保留。
- **按摘要复用, 缺失才传输**: 接收端已有同摘要且重算完好的对象计为
  `REUSED`; 缺失对象实际复制并在两端各重算一次哈希。每个对象有独立
  `PLANNED/TRANSFERRED/REUSED/ITEM_FAILED` 状态, `transfer` 可用
  `object_ids`/`limit` 分批推进; 传输中断、重复请求、服务重启后继续同一
  任务, 已完成对象跳过。
- **只有全量校验通过才一次性发布**: `finalize`(或全部完成时自动) 重检清单
  摘要并对 *全部* 对象在目标端重读重算哈希。冻结后源代际产生新输出导致
  清单变化 (`manifest_changed`)、任一对象摘要不符
  (`destination_digest_mismatch`)、仍有未完成项 (`incomplete_objects`)
  一律把任务置为 `FAILED`, **绝不写入发布目录**。
- **取消/失败释放临时保留, 不阻碍空间回收**: hold 是 blob 级临时保留,
  迁移创建/发布/失败/取消都推进 GC 引用水位; 活跃迁移期间的 GC 计划把
  held blob 排除为候选, 迁移终态后该 blob 立即恢复正常回收资格。已复制到
  接收端的孤儿副本由接收端 GC 正常回收。
- **发布目录与接收端 adopt**: 全部校验通过后原子写入 published catalog
  (含完整冻结清单与复用/传输统计); 接收集群 `POST /api/adoptions` 凭清单
  在本地再次全量校验, 通过才生成 RELEASED 作业的 COMPLETE 检查点和输出,
  随后即可用既有的同代际 `restore()` 恢复 (新输出租约/输入位点表等语义不
  变)。adopt 与发布事件均持久化, 接收端重启后仍可恢复。

任务状态: `PLANNED → TRANSFERRING → PUBLISHED`, 任意活跃态可 `CANCELLED`;
清单变化/摘要错误/终检不过为终态 `FAILED` (失败原因稳定可查)。作业进度页
展示每个任务的计划总量、已传输项、复用项、失败项、进度条、逐项明细、最终
状态与原因, 以及已发布可恢复代际表。

## 并发正确性

- 每作业一把 `RLock`；另有全局引用锁和 GC 状态锁，固定顺序为
  reference → GC state → 各 Job.lock（按 job_id 排序）；CAS 存储独立细锁。
- 测试 `tests/test_concurrency_api.py` 覆盖：
  - 旧进程迟到写入与恢复后新进程并发提交相同 `output_id` → 旧写入 412、恰好一次落盘；
  - 8 线程并发重复 output / shard → 只有一次 delivered/stored；
  - 未补齐期间并发提交 6 次全部失败，补齐后才 COMPLETE；
  - 失联恢复与提交循环并发 → COMPLETE 最多出现一次且在补齐之后。
- 测试 `tests/test_gc.py` 覆盖：
  - 计划预计释放量/保留原因/候选明细，执行后实际删除项和字节；
  - 代际 pin 与代际水位；计划后恢复/输出/pin 变化 → STALE 且拒绝执行；
  - 上传、恢复与 GC 并发时仍可访问的数据绝不被删除；
  - “物理删除已成功但事件未落盘”的崩溃重启续跑与查询接口/作业页面展示。
- 测试 `tests/test_migration.py` 覆盖：
  - 仅 COMPLETE 可导出、未知接收端/代际拒绝、冻结即源端重校验；
  - 按摘要复用、分片+输出整体迁移、接收端 adopt 后同代际恢复与重启持久化；
  - 分批传输/中断/重复请求/服务重启后继续同一任务，最终只发布一次；
  - 冻结后清单变化、目标端损坏“同键”对象、终检摘要不符 → FAILED 且无发布；
  - 取消/失败释放临时保留；活跃迁移 hold 使旧 GC 计划 STALE、新计划排除
    held blob，终态后源 blob 恢复可回收而接收端副本完好；
  - 8 线程并发续跑同一任务计数一致、恰好发布一次；HTTP API 与作业页面进度。
- 测试 `tests/test_authz_security.py` 覆盖：
  - 缺失/伪造/错类型/撤销/超期/跨作业/旧代际凭证的稳定错误分类与无回显；
  - 每次拒绝一条顺序化安全事件（seq 唯一递增、无凭证原文）；
  - 8 线程并发无效/混合请求不增加结果条目，合法重复恰好一次；
  - 落盘后新建管理器（模拟重启）：页面、`/security-events`、恢复验证报告
    （JSON 与 Markdown）仍可查询事件，输出/检查点/blob 完整且旧凭证仍被拒。

## 代码结构

```
hpc_cr/
  security.py     # HMAC 签名令牌、规范化 JSON、sha256 CAS 原语
  persistence.py  # 顺序化事件日志 Journal、持久化 blob 后端、签名密钥加载
  epochs.py       # 调度器、抢占纪元 (epoch/generation/deadline)
  storage.py      # 内容寻址 blob 存储 (内存/磁盘) + 输出去重日志
  checkpoints.py  # 分片、检查点状态机、全量重校验提交、重放回填
  leases.py       # 输出租约、代际栅栏、授权失败稳定归类
  migration.py    # 迁移冻结清单/摘要、任务与对象视图、发布代际记录
  gc.py           # GC 引用快照、保留代际、候选与计划视图
  jobs.py         # Job/JobManager 聚合根、恢复报告、GC/迁移编排、事件溯源/重放
  api.py          # stdlib ThreadingHTTPServer JSON API
  ui.py           # 节点进度页面 HTML 渲染 (含安全事件、GC 计划与迁移进度表)
tests/            # 77 个单元/并发/HTTP/GC/迁移/重启持久化测试
demo.py           # 端到端场景脚本 (可选 --data-dir 持久化)
```

## 持久化与重启语义

`data_dir` 布局：

- `journal.jsonl`：只追加事件日志，每行一个带全局单调 `seq` 的 JSON；
  写入 `flush + fsync`，重启按 seq 重放重建作业/检查点/租约/输出/审计/
  安全事件/GC 计划与删除进度。崩溃最多丢失未落盘的最后一行（该行对外
  也不可见）；GC 删除若先于事件落盘，重放后按同一计划安全续跑。
- `blobs/<sha256>`：分片与输出载荷，临时文件 fsync + 原子 rename。
- `signer.key`：首次启动自动生成的 HMAC 密钥（0600）；也可用
  `HPC_CR_SECRET` 覆盖。令牌自描述不入库，重放时以同密钥重新派生，
  因此旧令牌重启后仍能被验证或被栅栏拒绝。

不给定 `data_dir` 时全部退化为内存实现，接口行为一致。

---

# 多租户批任务接纳器（增量子系统）

`hpc_cr/batch/` 是与抢占/检查点恢复服务**完全独立**的增量子系统：用户按
**租户**提交需要多台机器**协同启动**的成组（gang）批任务，组件按各租户
**权重与等候时长**安排次序，再按 **CPU / 内存 / 加速器（GPU）** 需求从
异构机器中**成组**分配，并提供浏览器页面查看队列、分配明细与运行状态。
它有独立的锁、独立事件日志（`<data_dir>/batch/journal.jsonl`）与独立
路由前缀（`/api/batch/*`、`/ui/batch/*`），**不改变既有作业恢复行为**
（全部既有 92 个测试仍通过；`--no-batch` 可完全禁用该子系统）。

## 批接纳核心不变量

| # | 不变量 | 实现位置 |
|---|--------|----------|
| B1 | **成组容量全有或全无**：一项任务所需的 N 台机器容量必须**一次性全部取得**才启动；资源不够时不留下任何部分占用，任务保持 QUEUED | `scheduling.find_placement`、`manager._schedule_locked` |
| B2 | **无跨租户队头阻塞**：某租户队首任务此刻装不下时只跳过该租户，继续尝试后续租户，其他租户此刻可启动的任务照常启动 | `manager._schedule_locked` 多趟扫描 |
| B3 | **权重公平可解释**：每租户一个加权虚拟时间 `fair_key`，每接纳一个任务前进 `1/weight`；无限积压下吞吐严格按权重比例（如 3:1 实测 3.00）。调度轮次逐租户记录 `(score, decision, reason)` | `scheduling.order_queued/tenant_score`、`/api/batch/passes` |
| B4 | **久候不无限期饥饿**：老化只按*该租户队首自身*等候时长（超过阈值后得分线性下移），所有租户同积压时老化量相互抵消故权重比例不被破坏；持续被跳过的低权重租户在有限墙上时间内必定启动（默认 30s 阈值） | `scheduling.tenant_score`、后台 ticker / `scheduler/tick` |
| B5 | **业务键幂等与冲突**：相同 `(tenant, business_key)` 同内容（规范化 JSON 的 sha256 指纹）再次提交返回原任务/原结果；键相同但内容变化稳定返回 `409 business_key_conflict`，绝不覆盖 | `manager.compute_digest/submit_task` |
| B6 | **接纳期总量闸门**：全集群（未 REMOVED 的匹配机器）物理总量都不可能满足的任务在提交时即 409 拒绝（稳定原因码），不制造注定永远排队的等待项；瞬时碎片 / DRAINING（可能恢复）只排队不拒绝 | `scheduling.cluster_feasible` |
| B7 | **撤回 / 终止 / 归还幂等**：撤回 QUEUED、终止 RUNNING、`release` 对任意状态均幂等；容量账目逐台 pop，重复/并发/重启都恰好归还一次，绝不二次扣减 | `manager.cancel/terminate/release_resources/_finish_locked` |
| B8 | **事件溯源重启恢复**：排队项与已分配机器容量均从独立 journal 重放重建；重启后业务键索引、占用账目、幂等语义全部继续生效 | `batch/manager._replay`、`persistence.Journal` |

## 任务 / 机器状态机

```
任务: QUEUED ──成组容量全部取得──▶ RUNNING ──complete──▶ COMPLETED
          │                           ├──fail──────────▶ FAILED
          ├──cancel(幂等)──▶ CANCELLED └──terminate(幂等)─▶ TERMINATED
          └──deadline 到期自动取消 (有界等待)
机器: ENABLED ⇄ DRAINING (不再接纳新任务, 可恢复); 任意状态(空闲) ──▶ REMOVED
```

## 批接纳 HTTP 接口

| 方法/路径 | 说明 |
|---|---|
| `POST /api/batch/tenants` | 登记租户（`tenant_id`、`weight`）；重复登记 409 |
| `POST /api/batch/tenants/{id}` | 调整权重（立即影响公平次序） |
| `GET  /api/batch/tenants` | 租户权重、虚拟键、已接纳数、排队/运行数、最久等候与当前得分 |
| `POST /api/batch/machines` | 登记异构机器（`cpu/mem/gpu` 总量、`labels`） |
| `POST /api/batch/machines/{id}` | 调整总量（不得缩到当前用量以下）/ 标签 |
| `POST /api/batch/machines/state` | ENABLED / DRAINING / REMOVED（占用中不允许 REMOVED） |
| `GET  /api/batch/machines` | 机器总量/已用/空闲/占用明细/容量变化时间线 |
| `POST /api/batch/tasks` | 提交成组任务：`tenant_id`、`business_key`、`machine_count`、每台 `need{cpu,mem,gpu}`、标签 `selector`、可选 `deadline_seconds`；同键同内容幂等、异内容 409、超总量 409 |
| `POST /api/batch/tasks/{id}/cancel` | 撤回排队项（幂等） |
| `POST /api/batch/tasks/{id}/terminate` | 终止运行项并归还全部容量（幂等） |
| `POST /api/batch/tasks/{id}/complete` / `fail` | 上报完成（带 `result`，幂等）/ 失败 |
| `POST /api/batch/tasks/{id}/release` | 幂等归还资源（RUNNING→终止，QUEUED→撤回，终态→空操作） |
| `POST /api/batch/scheduler/tick` | 显式触发一轮调度（默认也有 1s 后台 ticker 处理老化/过期） |
| `GET  /api/batch/tasks` | 任务列表（可按 `tenant_id`/`state` 过滤） |
| `GET  /api/batch/tasks/{id}` | 单任务：分配去向、容量时间线、最近推迟缘由、结果 |
| `GET  /api/batch/passes` | 最近调度轮次（每租户得分/决策/原因，公平性可解释） |
| `GET  /api/batch/dashboard` | 总览聚合数据（页面同源数据） |
| `GET  /ui/batch` | **队列/权重/机器/运行/终态总览页面**（2s 刷新，含提交/撤回/终止表单） |
| `GET  /ui/batch/tasks/{id}` | **单任务页面**：每台分配去向、容量变化、拒绝缘由 |
| `POST /ui/batch/forms/{tenant,machine,submit}` | 页面表单（成功 303 跳回总览） |

### 稳定拒绝原因码（`reason`，页面逐项展示）

- 接纳期（409）：`insufficient_cluster_machines`（匹配机器总数不足）、
  `cluster_cpu_exceeded` / `cluster_mem_exceeded` / `cluster_gpu_exceeded`
  （全集群对应维度总量不足）、`business_key_conflict`（同键异内容）。
- 调度推迟（任务保持 QUEUED，`last_defer`）：`no_enabled_machine`、
  `insufficient_matching_machines`、`insufficient_free_capacity`
  （附匹配机器数、此刻可放下数与聚合缺口）。
- 状态机（409）：`not_cancellable` / `not_terminatable` / `not_running`。

## 批接纳运行与演示

```bash
# 端到端演示 (纯内存)
python3 demo_batch.py
# 持久化演示 (事件落盘, 重跑自动找回排队项与已分配容量)
python3 demo_batch.py --data-dir ./bm-data
# 演示后继续提供 HTTP 服务, 浏览器打开总览
python3 demo_batch.py --serve --port 8080        # http://127.0.0.1:8080/ui/batch

# 随主服务启动 (默认启用, 独立 <data_dir>/batch 日志)
python3 -m hpc_cr --data-dir ./hpc-data
python3 -m hpc_cr --no-batch                     # 禁用批接纳器, 行为与旧版一致

# 批接纳测试 (32 个: 成组/公平/老化/幂等/HTTP/并发/重启持久化)
python3 -m unittest tests.test_batch_scheduler \
                      tests.test_batch_http tests.test_batch_persistence -v
```

持久化目录布局新增（不触碰既有文件）：

- `batch/journal.jsonl`：批接纳器独立只追加事件日志（租户/机器/任务提交、
  推迟、成组分配、完成/失败/撤回/终止），与作业 `journal.jsonl` 分离；
  重启重放后，RUNNING 任务在每台机器上的容量占用精确恢复（不多不少），
  QUEUED 任务与业务键幂等索引继续生效。


# ImportRun 持久化暂存与事务化导入改造 - 产品需求文档

## Overview
- **Summary**: 将 `import_app` 的导入执行链路从「边读边写、崩溃即失、格式语义不一致」改造为「固化元数据 → 全量解析/验证 → 事务化提交 → 可恢复、可重试、可诊断」的两阶段管线，统一 CSV、Excel、QIF 三种格式的事务语义。
- **Purpose**: 保证严格模式的整单原子性与容错模式的逐行隔离；让进度与诊断在领域数据回滚后依然可见，同时绝不把未提交交易计入成功；支持工作进程崩溃恢复、同文件并发防护、规则任务恰好一次触发，以及临时文件的确定性生命周期。
- **Target Users**: 通过 Web 界面或 REST API 发起文件导入的最终用户；排查失败导入的用户；维护 worker/部署的运维者。

## Goals
- ImportRun 固化：源文件摘要（文件名/大小/内容哈希/分节行数）、导入配置快照（入队时的 profile YAML）、任务租约（owner + 过期时间）、持久化游标、执行阶段与模式。
- 每个源行持久化：原始负载、映射结果、幂等键、状态、结构化失败原因、尝试次数、提交产物引用。
- 两阶段执行：先完成全量解析与验证；严格模式在**单个数据库事务**中提交全部领域对象（任何失败零入账）；容错模式逐行独立事务（保存点）提交，失败行记录结构化原因并标记是否可重试。
- 任务可从持久化游标/暂存状态恢复：worker 崩溃后任务重入时跳过已终态行，只处理未完成与可重试行。
- 交易规则只在对应行**真实提交后**的 `on_commit` 阶段入队，并保证每条已提交交易恰好触发一次。
- 同文件（同 profile + 同内容哈希）在存在活动 Run 时拒绝重复提交并返回既有任务。
- 页面与 API 展示已提交（committed）、跳过（skipped）、失败（failed）、可重试（retryable）状态及结构化诊断。
- 统一 QIF 与 CSV/Excel 的事务语义，同时保留「允许跳过错误行」的既有行为。

## Non-Goals
- 不改造规则引擎（`apps/rules`）自身的动作执行幂等性；「恰好一次」仅指导入侧对规则任务的**入队/触发**恰好一次。
- 不改造非导入路径（手工建账、规则建账、周期交易等）的信号行为。
- 不做导入结果的业务回滚/撤销功能（删除 Run 仍保留已入账数据，维持现状文案）。
- 不引入新的文件存储后端；继续使用共享的本地 `/usr/src/app/temp` 卷。
- 不增加 ImportProfile/ImportRun 的多租户权限模型调整（维持现有可见性）。

## Background & Context
- 现状代码：[services/v1.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/services/v1.py)
  - CSV/Excel：逐行直接 `create`，依赖自动提交；严格模式（`skip_errors=False`）首行报错即中断，但**此前行已经入账**，并非原子。
  - QIF：严格模式由 `_process_qif` 外层 `transaction.atomic()` 包裹实现整单回滚；容错模式每条记录内层 `atomic()`；与 CSV/Excel 语义不一致。
  - 进度计数（successful/skipped/failed/processed）与日志在处理过程中随时写入；一旦外层事务回滚，QIF 严格模式下计数与日志也会回滚（且当前代码在事务内自增成功数，口径错误）。
  - 规则通过同步信号 `transaction_created.send()` 触发，[signals.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/rules/signals.py) 内立即 `.defer()`，未与提交边界对齐；重复处理/重放会重复入队。
  - 源文件路径只作为 procrastinate 任务参数传递，未持久化；`process_file` 的 `finally` 无条件删除临时文件，崩溃/失败后无法恢复或重试。
- 队列：procrastinate 3.8.1（PostgreSQL），已在 [common/tasks.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/common/tasks.py) 使用 `queueing_lock`；任务经 `process_import.defer` 投递。
- 数据库：PostgreSQL 15（JSONField、条件唯一约束、`select_for_update`、`transaction.on_commit` 均可用）。
- 部署：web 与 worker 共享命名卷 `wygiwyh_temp:/usr/src/app/temp/`（见 docker-compose）。
- 既有测试须维持的行为契约：
  - [test_qif_import.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/tests/test_qif_import.py)：严格模式非法日期 → 0 笔交易且抛出 "Import failed"；容错模式 → 1 笔入账；QIF internal_id 哈希去重；账户缺失失败。
  - [test_import_service_v1.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/tests/test_import_service_v1.py)：去重过滤逻辑。

## Functional Requirements

### FR-1：Run 元数据固化
入队时创建的 ImportRun 必须持久化：源文件摘要（原始文件名、字节大小、SHA-256 内容哈希、识别出的分节/工作表及行数）、配置快照（入队时刻 profile 的 YAML 文本及其解析结果，执行期以快照为准）、临时文件绝对路径、执行模式（strict / fault_tolerant，由快照中 `skip_errors` 派生）、执行阶段（enqueued/parsing/committing/finished/failed）。

### FR-2：任务租约
任务开始处理时以条件更新获取租约（`lease_owner` + `lease_expires_at`），仅当租约空闲或已过期时才能获取；处理期间按批次续租。租约被其他活跃 worker 持有时任务直接退出。过期租约可被恢复任务接管。

### FR-3：行级暂存与幂等键
每个源行在解析阶段写入 `ImportRow`：所属 Run、全局顺序号 sequence、分节名（CSV 为文件名、Excel 为 sheet 名、QIF/ZIP 为成员文件名）、源内行号、原始负载（JSON）、映射后负载（JSON，可空）、`idempotency_key`（SHA-256：文件哈希 + 分节 + 源内行号 + 原始负载归一化）、状态、尝试次数、结构化失败原因、提交产物引用（交易 ID，可空）、提交时间。`(run, sequence)` 与幂等键在 Run 范围内唯一。

### FR-4：两阶段——解析与验证
- 阶段 1 读取源文件，逐行做映射、类型强制、必填校验、外部对象存在性预校验（账户/类别/标签/实体/币种按 mapping 的 type/create 语义），结果写入 ImportRow：通过为 STAGED，失败为 FAILED_PERMANENT 并记录 `{stage:"parse|validate", code, message, line}`。
- 严格模式：阶段 1 出现任意 FAILED_PERMANENT 即终止，Run 标记 FAILED，**不创建任何领域对象**；失败诊断与进度在阶段 1 的独立事务中落库，不受后续影响。
- 容错模式：阶段 1 失败行保留诊断，继续处理其余行。

### FR-5：两阶段——提交
- 严格模式：阶段 2 在**单个** `transaction.atomic()` 块内创建全部领域对象（交易/账户/币种/类别/标签/实体及其 m2m、与 Run 的关联）、执行去重判定（命中 → SKIPPED）、回填 ImportRow 的产物引用；块内任何异常导致全部回滚，Run FAILED，失败行在回滚后的独立事务中标记 FAILED_RETRYABLE 或 FAILED_PERMANENT 与原因；**successful 计数保持 0**。
- 容错模式：阶段 2 每行一个独立事务（嵌套 atomic 保存点隔离），单行业务失败只回滚该行，记 FAILED_RETRYABLE/FAILED_PERMANENT + 结构化原因，其余行继续。
- 失败分类：数据/校验类（格式、必填、找不到且不可创建的外部对象）= FAILED_PERMANENT；瞬时类（数据库异常、死锁、连接错误等）= FAILED_RETRYABLE。
- 计数口径：`successful_rows` 只统计**事务提交成功后**的行（在 atomic 块成功退出/`on_commit` 后递增）；`skipped_rows` = 去重跳过；`failed_rows` = 两类失败之和；另设 `retryable_rows` 统计可重试失败；`processed_rows = committed + skipped + failed`。

### FR-6：进度与诊断的独立持久化
所有 ImportRow 状态、计数、日志、阶段/游标更新都在领域事务之外的独立事务（自动提交或独立连接）中写入；领域事务回滚不得回滚进度与诊断。提交进行中不得预先增加成功数。

### FR-7：崩溃恢复与游标
任务入口幂等：依据 ImportRow 状态恢复——PENDING 继续解析、STAGED 继续提交、FAILED_RETRYABLE 在自动重入时重试（attempts+1）、COMMITTED/SKIPPED/FAILED_PERMANENT 跳过；游标（最高已持久化 sequence）随批次推进。源文件丢失时 Run 置 FAILED，原因码 `source_file_missing`。新增周期任务扫描租约过期的 PROCESSING Run 并重新投递 `process_import`。

### FR-8：手动重试
- Web：Run 卡片对 FAILED 及租约过期的 PROCESSING Run 提供「重试」按钮（HTMX  POST）。
- API：提供 retry action；成功返回 202 与 run_id。
- 语义：容错 Run 只重试非永久失败（PENDING/STAGED/FAILED_RETRYABLE），COMMITTED/SKIPPED 不再处理（避免重复入账）；严格模式 FAILED Run 因无任何已提交数据，允许从源文件重新执行阶段 1（清空旧暂存行后重建）。源文件不存在时拒绝重试并给出明确提示。

### FR-9：规则任务 on_commit 恰好一次
导入路径不再在事务内同步 `send(transaction_created)`；改为在对应行事务成功提交后通过 `transaction.on_commit` 入队 `check_for_transaction_rules`（signal="transaction_created"），并以交易维度的 `queueing_lock`（如 `import-rule-created-<transaction_id>`）去重，捕获 already-enqueued 类异常；仅当配置 `trigger_transaction_rules=true` 时入队。回滚行不得入队。

### FR-10：同文件并发/重复提交防护
入队时先计算文件 SHA-256；同一 profile 下存在 QUEUED/PROCESSING 且 `file_hash` 相同的 Run 时：Web 给出提示并展示既有 Run；API 返回 409 与既有 `import_run_id`。数据库层以 PostgreSQL 部分唯一约束兜底竞态。

### FR-11：临时文件生命周期
- Run FINISHED：处理结束后删除其临时文件。
- Run FAILED：保留临时文件以供诊断/手动重试。
- 删除 Run（Web/API/级联）：尽力删除其专属临时文件。
- 新增周期清理任务：删除 temp 目录中不被任何未终结 Run 引用、且超过宽限期的孤儿文件。
- 所有文件访问继续受 `TEMP_DIR` 路径校验约束。

### FR-12：QIF 与 CSV/Excel 统一
QIF 改造为统一行管线：以 `^` 分隔的每条记录 = 一行（ZIP 中每个 .qif 成员为一个分节）；阶段 1 完成 D/T 解析与账户存在性校验；记录的 raw 行哈希继续写入 `Transaction.internal_id` 并作为去重依据（既有已存在 → SKIPPED，维持两次导入只入 1 笔的行为）；严格模式整单原子、容错模式逐行隔离；`[Transfer]`、`Label:Tag`、Payee→Entity 等映射语义保持不变；`skip_errors` 行为保持不变。

### FR-13：展示与接口
- Web Run 卡片：展示 total/committed/skipped/failed/retryable、阶段与租约状态；失败行可查看结构化原因（日志 offcanvas 增加失败行明细：行号/分节/阶段/代码/消息/尝试次数）；提供重试按钮。
- API：`ImportRunSerializer` 增加新字段（含 mode、phase、file_hash、retryable_rows、租约信息、游标）；提供只读的行诊断 action（可按 status 过滤）；retry action；重复上传返回 409。

## Non-Functional Requirements
- **NFR-1 正确性**：所有计数以数据库真实提交状态为准；并发/崩溃/重放场景下不产生重复入账、重复规则入队。
- **NFR-2 性能**：暂存行以批量方式（bulk/create + 分块）写入；计数器更新节流（按批次而非每行一次 UPDATE）；导入全程维持 `cachalot_disabled()`。
- **NFR-3 可观测性**：失败原因结构化且可机读；日志保留人读文本；周期任务记录执行日志。
- **NFR-4 兼容性**：`makemigrations`/`migrate` 在 PostgreSQL 上干净执行；旧 Run 记录以默认值/空值继续可展示；既有 QIF/去重测试全部通过。
- **NFR-5 安全性**：文件路径校验不放松；API 维持认证要求；不泄露文件系统绝对路径给非授权方（file_path 仅服务端使用，API 不输出）。

## Constraints
- **Technical**: Django + DRF + PostgreSQL 15；procrastinate 3.8.1（`queueing_lock`、periodic 装饰器）；Pydantic v1 风格的 profile schema；Excel（openpyxl/xlrd）、CSV、QIF/ZIP 三种读取器。
- **Business**: 容错模式「跳过错误行继续导入」的既有用户行为不可破坏；删除 Run 不删除已入账交易。
- **Dependencies**: worker 与 web 共享 `/usr/src/app/temp` 卷；procrastinate worker 进程需要加载新的 periodic 任务。

## Assumptions
- 单个 Run 同一时刻只被一个 worker 执行（租约串行化）；procrastinate 作业默认可能重复投递，业务层必须幂等。
- 周期任务（stale run 恢复、孤儿文件清理）随现有 procrastinate periodic 机制部署（每日/每小时级频率即可）。
- 文件哈希在上传保存时计算一次，临时文件内容在处理期间不可变（文件名含 storage 去重后缀，专属单 Run）。

## Acceptance Criteria

### AC-1: Run 元数据与租约固化
- **Type**: `rule`
- **Given**: 用户通过 Web 或 API 上传文件发起导入
- **When**: Run 创建完成
- **Then**: ImportRun 落库且包含 file_hash(sha256)、file_size、file_path、config_snapshot（入队时 YAML）、mode、phase=enqueued、source_summary；获取租约后 lease_owner/lease_expires_at 被写入并按批次续租
- **Pass Condition**: 迁移后字段存在；检查新 Run 行可见上述非空字段；配置快照与入队时 profile 内容一致且执行期修改 profile 不影响本次执行
- **Evidence**: 迁移文件、模型/服务代码、针对快照与租约的单元测试断言

### AC-2: 行级暂存与幂等键
- **Type**: `rule`
- **Given**: 任一格式文件进入阶段 1
- **When**: 解析每一行
- **Then**: 每个源行存在一条 ImportRow，含 sequence/section/row_number/raw_payload/mapped_payload/status/attempts/idempotency_key；同 Run 内 (run, sequence) 唯一、idempotency_key 唯一；任务重入不会产生重复 ImportRow
- **Pass Condition**: 导入含 N 条记录的文件后 ImportRow 计数 = N；重复执行任务不新增行
- **Evidence**: 模型约束与服务代码、测试查询断言

### AC-3: 严格模式全量解析后单事务提交（三格式统一）
- **Type**: `rule`
- **Given**: profile `skip_errors: false`，文件中存在一条无法通过解析/验证的记录（CSV 缺必填、Excel 坏日期、QIF 非法日期/账户缺失）
- **When**: 执行导入
- **Then**: Run FAILED；领域对象（交易及关联对象）零创建；阶段 1 的进度/诊断已落库可见；异常契约保持 QIF 测试期望（"Import failed"）
- **Pass Condition**: 三种格式分别构造坏行用例，`Transaction.objects.count()` 等均为 0，Run.failed_rows 与 ImportRow FAILED_PERMANENT 诊断可查，成功数为 0
- **Evidence**: 三种格式的严格模式测试

### AC-4: 容错模式逐行隔离且不破坏跳过行为
- **Type**: `rule`
- **Given**: profile `skip_errors: true`，文件中混杂合法行与坏行
- **When**: 执行导入
- **Then**: 合法行提交、坏行回滚为 FAILED_PERMANENT/FAILED_RETRYABLE 并记录结构化原因，其他行不受影响；QIF 既有 skip_errors 测试继续通过
- **Pass Condition**: 合法行全部可查、坏行无残留部分写入；processed = committed + skipped + failed
- **Evidence**: 三格式容错模式测试

### AC-5: 回滚后进度/诊断可见且未提交不计成功
- **Type**: `rule`
- **Given**: 严格模式阶段 2 因异常整体回滚
- **When**: Run 终止为 FAILED 后查询
- **Then**: Run 的计数、日志、阶段、ImportRow 诊断仍然存在（独立事务写入）；successful_rows = 0；页面/API 可见失败与可重试数
- **Pass Condition**: 触发提交期异常（如数据库级错误）后断言计数与行诊断存在且成功数为 0
- **Evidence**: 服务层测试（模拟提交异常）

### AC-6: 结构化失败原因与可重试分类
- **Type**: `rule`
- **Given**: 行处理在 parse/validate/commit 阶段失败
- **When**: 失败落库
- **Then**: ImportRow.failure_reason 含 stage/code/message/line（可含异常类名），attempts 递增；瞬时错误为 FAILED_RETRYABLE 并计入 retryable_rows，数据错误为 FAILED_PERMANENT
- **Pass Condition**: 分别构造两类错误，断言状态、原因 JSON 结构与计数
- **Evidence**: 服务层测试

### AC-7: 持久化游标与崩溃恢复
- **Type**: `rule`
- **Given**: Run 处理中 worker 崩溃（租约过期）
- **When**: 任务被重新投递/周期恢复任务扫描到
- **Then**: 任务从 ImportRow 状态与游标恢复：COMMITTED/SKIPPED/PERMANENT 行不重复处理，PENDING/STAGED/RETRYABLE 行继续；恢复后不产生重复交易与重复计数
- **Pass Condition**: 测试模拟「提交若干行后中断→重入」，最终入账数与一次跑完一致；过期租约可被接管、未过期租约被跳过
- **Evidence**: 服务层测试 + 周期任务测试

### AC-8: 规则任务 on_commit 恰好一次入队
- **Type**: `rule`
- **Given**: `trigger_transaction_rules: true` 的导入
- **When**: 行事务提交成功
- **Then**: 每条已提交交易在 on_commit 中恰好入队一个 check_for_transaction_rules 作业（queueing_lock 按交易去重，重复入队被吞并）；回滚行无作业；`trigger_transaction_rules: false` 不入队
- **Pass Condition**: 用伪造的 defer/on_commit 钩子断言调用次数与参数；容错模式坏行不产生作业；恢复重放不产生第二个人作业
- **Evidence**: 服务层测试（mock/拦截 procrastinate defer）

### AC-9: 同文件并发/重复提交被拒
- **Type**: `rule`
- **Given**: 同 profile 下已存在同 file_hash 的 QUEUED/PROCESSING Run
- **When**: 再次上传同一文件（Web 或 API）
- **Then**: 不创建第二个 Run；Web 返回提示，API 返回 409 与既有 import_run_id；并发双请求下数据库约束保证只有一个活动 Run
- **Pass Condition**: API 测试断言 409 与负载；并发创建测试断言仅 1 条活动 Run
- **Evidence**: API/视图测试、部分唯一约束迁移

### AC-10: 手动重试入口与语义
- **Type**: `rule`
- **Given**: FAILED 或租约过期 PROCESSING 的 Run
- **When**: 用户点击重试按钮或调用 API retry action
- **Then**: 容错 Run 仅重试未完成/可重试行；严格 FAILED Run 清空暂存后从文件重跑两阶段；文件缺失时拒绝并提示；成功投递返回 202
- **Pass Condition**: Web 视图与 API action 测试覆盖三种分支
- **Evidence**: 视图/API 测试

### AC-11: 临时文件生命周期与孤儿清理
- **Type**: `rule`
- **Given**: Run 终结或被删除，以及 temp 目录存在无主文件
- **When**: 终结/删除/周期清理发生
- **Then**: FINISHED 后文件删除；FAILED 后文件保留；删除 Run 删除其文件；清理任务删除超过宽限期且无未终结 Run 引用的文件，不删除仍被引用的文件
- **Pass Condition**: 文件系统断言测试（含周期任务单测）
- **Evidence**: 服务/任务测试

### AC-12: 页面与 API 状态展示完整
- **Type**: `rule`
- **Given**: 包含四种行结局的 Run
- **When**: 打开 Run 列表/日志页或请求 runs API/行诊断 action
- **Then**: committed/skipped/failed/retryable 计数一致；日志页可见失败行结构化明细；API 输出新字段且可按行状态过滤；重试控件在可重试态出现
- **Pass Condition**: 模板渲染测试/接口测试与字段断言
- **Evidence**: 模板与 API 测试

### AC-13: QIF 语义统一且既有契约不变
- **Type**: `rule`
- **Given**: 既有 QIF 测试夹具（合法两条、重复哈希、坏日期严格/容错、账户缺失、ZIP）
- **When**: 在新管线执行
- **Then**: test_qif_import.py 全部通过；ZIP 多分节行数与去重正确；internal_id 行为不变
- **Pass Condition**: `python manage.py test apps.import_app` 全绿
- **Evidence**: 测试运行输出

### AC-14: 工程质量与回归
- **Type**: `rubric`
- **Dimension**: 实现与既有代码风格、分层、错误处理、测试质量的一致性
- **Scale**: 1-5
- **Anchors**: 1 = 引入回归或明显风格割裂；3 = 功能完整但存在冗余/边界疏漏；5 = 复用既有抽象、边界完备、测试覆盖崩溃/并发/回滚场景
- **Pass Threshold**: >= 4
- **Evidence**: 独立评审代码走读与全量测试结果

# ImportRun 持久化暂存与事务化导入 — 评审材料

- 日期：2026-10-05
- 范围：`apps/import_app`（模型/服务/任务/视图/模板/迁移/测试）、`apps/api`（serializers/views）、`locale/en`
- 规格：[spec.md](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/.trae/specs/import-run-durability/spec.md)
- 任务与逐条证据：[tasks.md](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/.trae/specs/import-run-durability/tasks.md)

## 变更清单

| 层 | 文件 | 要点 |
|---|---|---|
| 模型/迁移 | [models.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/models.py)，迁移 0002/0003/0004 | ImportRun 固化 file_hash/file_size/stored_file_path/config_snapshot/mode/phase/cursor/lease_owner/lease_expires_at/run_attempts/requested_by/五个计数器；部分唯一约束 `uniq_active_run_profile_filehash`；ImportRow 行暂存模型（(run,idempotency_key) 唯一）；`lease_is_active`/`is_retriable` |
| 入队 | [enqueue.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/services/enqueue.py) | sha256 摘要、配置快照、mode 派生、409 活动 Run 复用、ORPHAN_GRACE_PERIOD_SECONDS |
| 解析 | [source_rows.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/services/source_rows.py) | CSV/Excel/QIF(ZIP) 统一行迭代器、幂等键 sha256(file|section|row|raw) |
| 管线 | [v1.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/services/v1.py) | 两阶段：parse_and_stage（bulk_create 200/批 + 游标/计数/心跳节流）→ commit_rows（严格单 atomic 全有或全无；容错逐行 atomic 保存点）；诊断写均在领域 atomic 外 autocommit；严格引用校验 `_validate_references`；租约获取/心跳/释放；`process_file` 收尾契约（FAILED 保留文件并 raise Exception("Import failed")，FINISHED 删文件） |
| 重试 | [retry.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/services/retry.py) | 严格=清行重跑；容错=RETRYABLE→STAGED 保留 attempts 与终态；文件缺失/非法状态 409；IntegrityError→409 active_run_exists |
| 任务 | [tasks.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/tasks.py) | process_import（thread user、租约、stored_file_path 兜底）；periodic recover_stale_import_runs（*/5，per-run queueing_lock）；periodic cleanup_orphan_temp_files（每日 03:30，白名单+24h 宽限） |
| 规则 | v1.py `_schedule_rules` | transaction.on_commit 内 defer，queueing_lock=`import-rule-created-<tx_id>`，trigger 开关；QIF 永不触发；信号其他调用点未动 |
| Web | [views.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/views.py)、[urls.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/import_app/urls.py)、[list.html](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/templates/import_app/fragments/runs/list.html)、[log.html](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/templates/import_app/fragments/runs/log.html) | retry 视图（204+HX-Trigger）、删除清文件、Retryable 计数卡、过期租约态、失败行明细表、mode/phase 展示 |
| API | [imports.py](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/apps/api/views/imports.py)、[serializers/imports.py](file:///Users/kkcarrot/swe-project/WYGIWYH_fork1/app/apps/api/serializers/imports.py) | retry action（202/409）、rows action（分页+status 过滤，字段白名单不含 raw_payload/mapped_payload）、run 诊断字段、filterset mode/phase/retryable_rows |
| i18n | [django.po](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/locale/en/LC_MESSAGES/django.po) | en 目录重新提取，其余语言 gettext 回退 |
| 测试 | [test_pipeline.py](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/apps/import_app/tests/test_pipeline.py)（17）、[test_recovery.py](file:///Users/kkcarrot/spe-project/WYGIWYH_fork1/app/apps/import_app/tests/test_recovery.py)（18）+ Task 1/2 既有 | 崩溃恢复/并发/回滚/恰好一次四类故障注入 |

## AC 自评映射

- **AC-1**：Task 1/2 证据——0002/0003 迁移字段齐全；enqueue 测试断言快照冻结、改 profile 不影响已入队 Run；租约条件 UPDATE + 心跳。
- **AC-2**：Task 3——管线用例含 N 行计数与 TR-3.3 重入零新增；`(run,sequence)` 与 `(run,idempotency_key)` **两个 DB 唯一约束并存**（models.py Meta，迁移 0003），重入经 existing_keys 跳过 + bulk_create(ignore_conflicts)。
- **AC-3**：TR-3.2 CSV 缺字段、Excel 坏日期、QIF 坏日期/账户缺失（既有 5 个 QIF 契约全绿）严格零入账。
- **AC-4**：TR-4.2 容错 3 行混合；processed=committed+skipped+failed 由 `_reconcile_counters` 聚合保证。
- **AC-5**：TR-4.1/4.3/4.4、TR-6.2——领域 atomic 回滚后，行诊断/计数在 autocommit 独立写入仍可查；successful 仅在提交后增长，严格回滚后为 0。
- **AC-6**：failure_reason {stage,code,message,section,line}；OperationalError→RETRYABLE（attempts=2 用例），其余 PERMANENT。
- **AC-7**：TR-7.1/7.2/7.3（见 tasks.md 证据）。
- **AC-8**：TR-6.1~6.4 + TR-5.4 QIF 零 defer。
- **AC-9**：Task 2——API 409 负载含既有 import_run_id、Web 204 提示、部分唯一约束 + enqueue 竞态 IntegrityError 复用。
- **AC-10**：TR-7.4/7.5 全覆盖（严格重置/容错保留/文件缺失/非法状态/Web）。
- **AC-11**：TR-8.1~8.4。
- **AC-12**：TR-9.1~9.4。
- **AC-13**：test_qif_import.py 5 用例全绿（191 总测试通过）；internal_id 语义不含账户名的既有行为保持（ZIP 相同内容去重）。
- **AC-14（rubric）**：自评 5/5，证据 TR-10.3；阈值 ≥4，待独立评审定级。

## 关键设计决策（请评审重点挑战）

1. **诊断写在领域事务外**：Django 5.2 已移除 `transaction.autocommit_block()`。实现通过结构约束保证 `_log/_save_fields/_reconcile_counters/_mark_failed_rows` 只在领域 atomic 块外调用（容错每行先回滚再标记；严格异常退出 atomic 后标记异常行）。评审请核查是否存在任何在 atomic 嵌套内调用这些方法的路径。
2. **attempts 口径**：`_commit_row` 内随领域事务 +1（回滚撤销），`_mark_failed_rows` 在回滚后独立事务再 +1，避免重复计数。
3. **无 config_snapshot 的直接构造 Run（旧测试兼容）**：服务 `__init__` 从 settings.skip_errors 派生 mode 并 UPDATE 落库；enqueue 链路的 Run 始终冻结快照不受影响。
4. **QIF 规则**：QIF profile 无 trigger_transaction_rules 字段，永不入队规则任务（保持旧行为）。
5. **行级 rows action 绕开 self.get_object()**：避免 run 级 filterset 校验 `status` 查询参数（FAILED_PERMANENT 非 Run 状态选择）返回 400。
6. **rows API 白名单**：不输出 raw_payload/mapped_payload（体积与敏感数据考量），仅 failure_reason/状态/定位/attempts/committed_at/transaction。
7. **临时文件路径安全**：`_validate_file_path` 要求 startswith TEMP_DIR；周期清理仅扫普通文件、OSError 全吞。
8. **i18n**：仅补 en 目录提取（spec 允许「其余走 gettext 占位」）；其他 13 语言 .po 未批量更新。

## 已知非阻塞限制

- `_log` 按失败行追加 UPDATE（O(失败行)），为回滚后诊断可见的刻意取舍；超大文件全坏行场景可优化为批内追加。
- 恢复依赖租约 TTL（10min）与周期扫描（5min），最坏接管延迟 ≈ 15min；心跳每 200 行一次；评审加固后失主 worker 会在批次边界/行领取/终结点自动退出，单批 200 行处理超 10min 也不会重复入账（行锁 + LeaseLost），仅延迟恢复（可配置类属性 LEASE_TTL）。

## 验证命令与结果

```bash
cd app
SECRET_KEY=dev DEBUG=true SQL_DATABASE=wygiwyh SQL_USER=wygiwyh \
SQL_PASSWORD=wygiwyh SQL_HOST=127.0.0.1 SQL_PORT=55432 \
../.venv/bin/python manage.py test apps.import_app apps.api apps.rules -v 1
# Ran 197 tests — OK（评审后加固复跑；初次评审时 191）
../.venv/bin/python manage.py makemigrations --check --dry-run
# No changes detected（EXIT=0；仅两个改造前既有的 vite/static 无害 WARNING）
```

## 独立评审结论

独立只读评审（2026-10-05，实测 191 测试 + makemigrations）：**PASS**，AC-1~AC-13 全 PASS，**AC-14 = 4/5（≥ 阈值 4）**，无 blocker。提出 1 major（租约无 fencing/行领取无锁的极端时序重复入账）+ 4 minor + 2 nit。

### 评审后加固（已实现并复测）

针对评审 Issue 全部 major 与可快速收敛的 minor 完成加固，新增 6 个测试（总数 191 → **197，全绿**）：

1. **major-1 租约 fencing + 行级互斥**（TR-7.6/7.7）：
   - `_commit_row` 首语句改为条件领取 UPDATE（STAGED/RETRYABLE/PENDING→PENDING）：UPDATE 行锁串行化竞争 worker，胜者提交后败者按新行版本匹配 0 行 → 新增 `LeaseLostError` 立即中止，杜绝重复领域对象；
   - `_heartbeat` 返回 bool，新增 `_check_lease()`（解析每批、容错提交每批、严格大事务前）失主即中止；
   - 新增条件化 `_finalize()`：终态/文件操作仅在租约仍属本 worker 时执行，失主安静退出（不覆写状态、不删源文件），由当前 owner 或周期恢复收尾；
   - 双线程跨连接竞态测试实测：交叠提交同一行最终仅 1 笔交易。
   - 残留理论窗口：单个行事务自身持续超过 TTL（默认 10min，类属性可调）——行锁仍串行化、败者中止，最坏由周期恢复幂等收敛。
2. **minor-2 重试半重置**（TR-7.8）：状态迁移改为条件 UPDATE（FAILED 或 PROCESSING+过期）且与严格模式行删除同处一个 atomic，部分唯一约束冲突时整体回滚；同时容错重试立即修正 failed/retryable 计数、重置 started_at。
3. **minor-3 on_commit 异常吞没过宽**（TR-6.3b）：`AlreadyEnqueued` 静默 debug；其他异常 warning 并追加 Run 日志留痕。
4. **minor-4 提交游标**（TR-7.9）：提交阶段每批与收尾推进 cursor={phase:"committing",sequence}。
5. **AC-2 文档订正**：(run,sequence) 与 (run,idempotency_key) 两个唯一约束本就并存。

未处理（评审 nit，存量/非本次放松或刻意取舍）：`_validate_file_path` 前缀匹配（与旧实现逐字相同，NFR-5 未放松）、IntegrityError→active_run_exists 错误码归并、lease_owner 主机信息对认证用户可见、`_log` O(失败行) UPDATE、admin 删 Run 靠 24h 孤儿清理兜底。

最终验证（2026-10-05）：`manage.py test apps.import_app apps.api apps.rules` → **Ran 197 tests — OK**；`makemigrations --check --dry-run` → No changes detected（仅两个改造前既有 WARNING）。

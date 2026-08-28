# FileUpdate、Activity 与 FileHistory 排查指南

本文用于排查 `seafevents` 消费 `repo-update` 事件后，`FileUpdate`、`Activity` 或 `FileHistory` 未写入的问题。

适用范围：13.0 分支中为 `RepoUpdateEventHandler`、`FileUpdateEventHandler` 和消息分发器增加 INFO 日志后的版本。

## 准备工作

1. 重启 `seafevents`，使新增日志生效。
2. 对一个测试仓库执行一次普通文件创建、上传、修改或删除操作。
3. 记录操作时间、`repo_id`、`commit_id` 和操作用户。
4. 在 `seafile-events.log` 中用 `commit_id` 或 `repo_id` 检索完整日志上下文。
5. 使用同一个 `commit_id` 关联 `RepoUpdate` 和 `FileUpdate` 日志。不要只根据 `FileAudit` 表有数据判断更新审计链路正常。

`FileAuditEventHandler` 处理下载、上传同步等访问事件；`FileUpdateEventHandler` 只处理 `seaf_server.event:repo-update`。因此 `FileAudit` 能写入不代表相应的 `FileUpdate`、`Activity` 或 `FileHistory` 必然能写入。

## 排查日志总表

| 阶段 | 关键日志 | 正常预期 | 异常含义 | 下一步 |
| --- | --- | --- | --- | --- |
| 事件进入 RepoUpdate | `[RepoUpdate] received repo-update event: ...` | 每次有效文件变更均有日志 | 没有日志表示 `repo-update` 未发布、未被消费，或运行的不是包含日志修改的代码 | 检查 seafile-pro-server 日志、seafevents 进程、`[Audit] enabled` 和 `Subscribe to channels` |
| 消息字段 | `[RepoUpdate] processing repo_id=..., commit_id=...` | `repo_id`、`commit_id` 都有值 | 原有 warning `repo_id: ..., or commit_id: ... invalid.` 表示消息不完整，后续不会生成记录 | 检查 seafile-pro-server 发布的 `repo-update` 内容 |
| 读取当前 Commit | `[RepoUpdate] load_commit version=1 returned a commit ...` | 通常 version 1 返回 commit | `None` 表示无法按 version 1 读取 commit | 查看 version 0 回退结果和 seafile-server RPC 错误 |
| Commit 版本回退 | `[RepoUpdate] load_commit version=0 returned a commit/None ...` | version 1 失败时，version 0 可作为兼容回退 | 两次均为 `None` 时，不会生成 Activity、FileHistory 或 FileUpdate | 检查 `SearpcError`、数据库 1040、commit/object 读取错误 |
| Commit 跳过 | `[RepoUpdate] skipped because commit is missing, has no parent, or is a merge commit ...` | 普通文件修改不应出现 | commit 不存在、首次提交没有 parent，或属于 merge commit；当前逻辑会跳过 | 确认是否首次提交/merge；若为普通修改，检查 commit 加载 |
| 读取父 Commit | `[RepoUpdate] skipped because parent commit could not be loaded ...` | 普通变更应能读取 parent | 无法计算 diff，因此不会生成 Activity/FileHistory | 检查 parent commit、对象库、RPC 和 Seafile 数据库 |
| Diff 计算 | `[RepoUpdate] diff ... added_files=N, ... modified_files=N, ...` | 至少一个变动计数大于 0 | 九类计数均为 0 时，不会写 Activity/FileHistory | 对比该 commit 与 parent 的文件差异 |
| 空 Diff 跳过 | `[RepoUpdate] skipped activity and file history because diff is empty ...` | 普通文件修改不应出现 | handler 正常执行，但 commit diff 为空，属于当前代码预期 | 检查实际操作是否产生新 commit、diff 逻辑和仓库数据 |
| 用户与组织 | `[RepoUpdate] resolved org_id=..., owner=..., user_count=N ...` | 有 owner 且用户数通常大于 0 | owner/共享用户 RPC 异常会中断；无用户时会跳过 | 检查仓库归属、共享关系、`get_repo_owner` / `get_org_repo_owner` 和紧邻 traceback |
| 无用户跳过 | `[RepoUpdate] skipped activity and file history because no users were found ...` | 一般不应出现 | owner 为空或用户列表异常，Activity/FileHistory 不写入 | 检查仓库 owner、组织库 `OrgRepo` 及 RPC 返回值 |
| 文件历史开关 | `[RepoUpdate] skipped file history because it is disabled ...` | 需要记录历史时不应出现 | `[FILE HISTORY] enabled = false`，只影响 FileHistory，不应影响 Activity/FileUpdate | 检查 seafevents 实际读取的 `seafile.conf` |
| 生成 FileHistory | `[RepoUpdate] generated N file history records ...` | 普通文件变更通常 `N > 0` | `N = 0` 表示 diff 存在，但记录生成规则过滤了该操作 | 检查文件路径、后缀和文件历史过滤规则 |
| 写入 FileHistory | `[RepoUpdate] saved file history records ...` | 应出现 | 有 `generated` 但无 `saved`，表示写入过程中异常 | 查看 `error in handler RepoUpdateEventHandler ...` 的完整 traceback |
| 生成 Activity/Trash | `[RepoUpdate] generated N activity records and N trash records ...` | 普通修改的 Activity 通常 `N > 0`；非删除操作 Trash 可为 0 | Activity 为 0 表示被生成规则过滤，或该操作不生成活动记录 | 对照 diff 类型、文件路径和 `generate_activity_records()` 规则 |
| 写入 Activity/Trash | `[RepoUpdate] saved activity and trash records ...` | 应出现 | 有 `generated` 但无 `saved`，表示写入 Activity 或 Trash 时异常 | 查看 RepoUpdate traceback，重点检查 MySQL 1040、事务和连接异常 |
| 事件进入 FileUpdate | `[FileUpdate] received repo-update event: ...` | 应在 RepoUpdate 处理后出现 | RepoUpdate 已收到但本日志缺失，检查 Audit 开关、实际部署代码和 handler 注册 | 确认 `[Audit] enabled = true`；查看启动时 `Subscribe to channels` |
| FileUpdate 字段 | `[FileUpdate] processing repo_id=..., commit_id=...` | 字段均有值 | `skipped invalid repo_id=..., commit_id=...` 表示消息字段异常 | 回查 server 发布的 `repo-update` 消息 |
| FileUpdate 组织 ID | `[FileUpdate] resolved org_id=... for repo_id=...` | 组织库应对应正确组织 ID | 值异常表示组织查询退化，但通常不单独阻止插入 | 检查 Seafile DB 的 `OrgRepo` 和 RPC/数据库日志 |
| FileUpdate 读取 Commit | `[FileUpdate] get_commit version=1 returned a commit/None ...` | 通常返回 `a commit` | `None` 表示通过 RPC 读取 commit 失败或未找到 | 继续查看 version 0 回退并检查 seafile-server/RPC |
| FileUpdate 回退 | `[FileUpdate] get_commit version=0 returned a commit ...` | version 1 失败时可回退成功 | 两版本均无 commit 时，FileUpdate 不会写入 | 核实 commit 是否存在；检查 server、RPC 和 DB 连接错误 |
| FileUpdate 静默跳过 | `[FileUpdate] skipped because get_commit returned None for both versions ...` | 普通文件修改不应出现 | FileUpdate 特有故障点。事件已消费，不会重试或自动补写 | 优先检查 `get_commit` RPC、故障时间的 DB 连接耗尽及 server 日志 |
| FileUpdate 写入完成 | `[FileUpdate] saved FileUpdate record ...` | 应出现且表中可查 | 有日志但表中无数据时，可能查错数据库、读写分离延迟或查询条件有误 | 在 `SEAFILE_MYSQL_DB_SEAHUB_DB_NAME` 对应库中按 commit 查询 |
| RepoUpdate 异常 | `error in handler RepoUpdateEventHandler for message type seaf_server.event:repo-update: ...` | 不应出现 | RepoUpdate 中断，通常会导致 Activity/FileHistory 缺失 | 保存完整 traceback，重点查看最早出现的异常 |
| FileUpdate 异常 | `error in handler FileUpdateEventHandler for message type seaf_server.event:repo-update: ...` | 不应出现 | FileUpdate 已进入 handler，但在时间转换、RPC、字段或数据库提交时失败 | 查看 traceback 中首个异常，例如 `OperationalError`、`PendingRollbackError` 或约束错误 |
| 数据库连接耗尽 | `Too many connections` 或 `OperationalError: (1040, ...)` | 不应出现 | 当前处理的消息会失败且不重新投递，故障窗口内可能有记录缺失 | 分析 MySQL 连接来源和峰值；恢复后用新事件复测 |
| Session 事务失败 | `PendingRollbackError`、`This Session's transaction has been rolled back`、`Can't reconnect until invalid transaction is rolled back` | 不应出现 | 前序 handler 提交失败且未 rollback，导致同一 `repo-update` 后续 handler 失败 | 保存同一 commit 的完整日志顺序；这是 Activity/FileHistory/FileUpdate 同时缺失而后续 FileAudit 正常的强证据 |
| 获取事件失败 | `Failed to get event: ...` | 不应持续出现 | 无法调用 `pop_event()` 获取 server 事件，线程会每 3 秒重试 | 检查 seafile-server RPC/socket、seafevents 连接配置和 server 状态 |
| 消息格式错误 | `invalid message format: ...` 或 `got bad message: ...` | 不应出现 | 消息 JSON 或字段不符合处理器预期，当前消息不会处理 | 保存原始消息并检查发布端 |

## 正常链路的最小日志序列

一次普通文件修改至少应出现以下顺序的日志：

```text
[RepoUpdate] received repo-update event
[RepoUpdate] processing repo_id=..., commit_id=...
[RepoUpdate] load_commit version=1 returned a commit
[RepoUpdate] diff ... modified_files=1 ...
[RepoUpdate] generated N file history records
[RepoUpdate] saved file history records
[RepoUpdate] generated N activity records and 0 trash records
[RepoUpdate] saved activity and trash records
[FileUpdate] received repo-update event
[FileUpdate] processing repo_id=..., commit_id=...
[FileUpdate] get_commit version=1 returned a commit
[FileUpdate] saved FileUpdate record
```

`FileUpdateEventHandler` 和 `RepoUpdateEventHandler` 都处理同一个 `repo-update` 事件，但 `RepoUpdateEventHandler` 先执行。若前者出现数据库提交异常，后者在同一 SQLAlchemy session 中可能受到失败事务状态影响。

## 推荐排查顺序

1. 使用新的普通文件修改操作复测，不只依据历史缺失记录。
2. 从 `[RepoUpdate] received` 开始，按 `commit_id` 串联日志。
3. 确认当前 commit 和 parent commit 都能加载。
4. 确认 diff 中至少一类文件变动数量大于 0。
5. 确认 FileHistory/Activity 的 `generated` 和 `saved` 日志均出现。
6. 确认 `[FileUpdate] received` 出现，且 `get_commit` 返回 `a commit`。
7. 以 `[FileUpdate] saved FileUpdate record` 和数据库查询作为成功判定。
8. 如存在异常，保存同一 `commit_id` 前后完整日志，首先定位最早的 traceback，而不是只分析后续报错。

## 数据库核验

在 `SEAFILE_MYSQL_DB_SEAHUB_DB_NAME` 对应的数据库中执行。不同安装的表字段可能略有差异，先核对表结构：

```sql
SHOW CREATE TABLE FileUpdate;
SHOW CREATE TABLE Activity;
SHOW CREATE TABLE FileHistory;
```

核验 FileUpdate：

```sql
SELECT eid, timestamp, repo_id, commit_id, user, file_oper
FROM FileUpdate
WHERE repo_id = '<repo_id>'
  AND commit_id = '<commit_id>';
```

核验 Activity：

```sql
SELECT id, repo_id, commit_id, path, op_type, timestamp
FROM Activity
WHERE repo_id = '<repo_id>'
  AND commit_id = '<commit_id>'
ORDER BY id;
```

核验 FileHistory：

```sql
SELECT id, repo_id, commit_id, path, op_type, timestamp
FROM FileHistory
WHERE repo_id = '<repo_id>'
  AND commit_id = '<commit_id>'
ORDER BY id;
```

发生 `Too many connections` 后，确认数据库当前容量和连接峰值：

```sql
SHOW VARIABLES LIKE 'max_connections';
SHOW GLOBAL STATUS LIKE 'Threads_connected';
SHOW GLOBAL STATUS LIKE 'Max_used_connections';
SHOW FULL PROCESSLIST;
```

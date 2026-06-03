# 使用 Instance ID 和 Attach 运行长作业

本文档说明 MaxCompute Google Sheets 插件如何通过 **Instance ID + Attach** 机制支持长时间运行的 SQL 作业。这个方案适用于 SQL 在 MaxCompute 侧运行时间较长，可能超过 Google Apps Script 单次请求执行时间限制的场景。

## 适用场景

适合使用本方案的情况：

- SQL 查询本身需要数分钟或更久才能在 MaxCompute 中完成。
- Google Sheets 侧边栏关闭、浏览器刷新、网络短暂中断后，希望继续等待同一个 MaxCompute 作业。
- 希望先提交作业，稍后再把已完成的结果写回指定 Sheet。

不适合用来解决的情况：

- 查询结果超过插件写入上限。插件单次最多写入 10000 行结果。
- 查询结果本身过大，导致拉取结果或写入 Sheet 超出 Google Sheets / Apps Script 的其它限制。
- 希望后台无人值守自动写回结果。当前机制需要用户打开侧边栏并执行 Attach。

## 背景限制

Google Apps Script 对单次脚本执行有时间限制。当前插件按单次执行约 6 分钟的限制设计，不能让一次 Apps Script 调用一直阻塞等待长作业完成，否则可能在 MaxCompute 作业完成前就被 Google 终止。

因此，插件采用“提交作业”和“等待结果”分离的方式：

1. 先把 SQL 提交到 MaxCompute，创建一个 MaxCompute Instance。
2. 插件拿到 Instance ID 后立即返回给侧边栏。
3. 侧边栏用多次短请求轮询作业状态。
4. 作业完成后，插件再单独发起一次请求拉取结果并写入 Sheet。

每一次状态查询和结果写入都是独立的 Apps Script 调用，不需要让同一个 Apps Script 调用持续运行超过数分钟。

## 工作机制

### 1. 提交 SQL

用户点击 **Run Query** 后，插件会调用 MaxCompute Instance Job API 提交 SQL。

如果 MaxCompute 立即返回结果，插件会直接写入 Sheet。对于长作业，MaxCompute 会创建异步 Instance，插件会从响应中取得 Instance ID。

侧边栏会显示：

- Instance ID
- Logview 链接
- Cancel 按钮
- 当前等待状态

### 2. 前端轮询作业状态

拿到 Instance ID 后，侧边栏会定期调用后端查询状态。

轮询间隔为：

```text
1s -> 2s -> 4s -> 8s -> 8s -> ...
```

每次轮询只做一件很短的事情：查询当前 Instance 是否已经结束。如果还没结束，就等待下一次轮询。

### 3. 作业完成后写入 Sheet

当 MaxCompute Instance 进入 `Terminated` 状态后，插件会继续查询 Task 状态。

- `Success`：拉取结果并写入用户选择的 Target Sheet。
- `Failed`：显示失败摘要。
- `Cancelled`：显示作业已取消。
- 其它状态：显示任务状态异常。

写入结果时，插件会使用用户当前选择的 **Target Sheet**。如果用户是通过 Attach 恢复作业，也会使用 Attach 时侧边栏里选择的 Target Sheet。

## 用户操作流程

### 正常运行长作业

1. 打开 Google Sheets 中的 MaxCompute 插件侧边栏。
2. 在 SQL 编辑器中输入查询。
3. 在 **Target Sheet** 中选择结果写入位置。
4. 点击 **Run Query**。
5. 看到 Instance ID 后，可以保持侧边栏打开等待，也可以复制保存 Instance ID。
6. 作业完成后，结果会自动写入 Target Sheet。

### 关闭侧边栏后恢复作业

如果侧边栏被关闭、浏览器刷新，或者 Google Sheets 页面被重新打开：

1. 重新打开 MaxCompute 插件侧边栏。
2. 在 **Target Sheet** 中选择希望写入结果的 Sheet。
3. 展开 **Attach to existing job**。
4. 从 **Recent jobs** 下拉列表中选择最近的 Instance ID，或者手动粘贴 Instance ID。
5. 点击 **Attach**。
6. 插件会查询该 Instance 的当前状态：
   - 如果还在运行，会继续轮询。
   - 如果已经成功，会拉取结果并写入 Target Sheet。
   - 如果失败或取消，会显示对应状态。

## Instance ID 历史记录

插件会在浏览器本地保存最近提交或 Attach 过的 Instance ID，方便用户在 **Attach to existing job** 中选择。

当前规则：

- 仅保存在当前浏览器的 `localStorage`。
- 最多保留 10 条。
- 有效期为 1 天。
- 不会同步到其它浏览器或其它设备。
- 清理浏览器站点数据后，历史记录会消失。

如果下拉列表中没有目标 Instance ID，用户仍然可以手动输入或粘贴 Instance ID。

## 注意事项

### Attach 不会重新执行 SQL

Attach 只是恢复对已有 MaxCompute Instance 的跟踪，不会重新提交 SQL。

如果同一个 Instance 已经完成，Attach 会尝试读取这个 Instance 的结果并写入当前选择的 Target Sheet。

### Attach 时需要选择正确的 Target Sheet

Instance ID 只代表 MaxCompute 侧的作业，不包含用户这次希望写入哪个 Google Sheet 标签页。

因此，恢复作业前请确认侧边栏里的 **Target Sheet** 选择正确。插件会把 Attach 后获取到的结果写入当前选择的 Target Sheet。

### 不要修改或删除 MaxCompute 作业结果

Attach 依赖 MaxCompute 侧还能通过 Instance ID 查询状态和读取结果。如果 MaxCompute 侧的作业记录或结果不可用，插件无法恢复写入。

### 结果写入仍然受限制

Instance ID + Attach 解决的是“SQL 等待时间超过单次 Apps Script 调用限制”的问题，不代表结果写入可以无限大。

仍然需要注意：

- 插件最多写入 10000 行。
- Google Sheets 本身有单表格单元格数量、写入耗时等限制。
- 结果列数过多、单元格内容过大，也可能导致写入变慢或失败。

建议长查询仍然在 SQL 中使用 `LIMIT`、分区过滤、字段裁剪等方式控制返回结果规模。

## 常见问题

### 我已经拿到 Instance ID，可以关闭 Google Sheets 吗？

可以。MaxCompute 作业已经在服务端运行，关闭 Google Sheets 不会自动取消该 MaxCompute Instance。

之后重新打开 Google Sheets 和插件侧边栏，使用 Attach 恢复即可。

### 关闭侧边栏后，插件还会继续自动写入结果吗？

不会。侧边栏关闭后，前端轮询也会停止。作业仍会在 MaxCompute 侧继续运行，但需要用户重新打开侧边栏并 Attach，插件才会继续查询状态并写入结果。

### Attach 会写入原来选择的 Sheet 吗？

不会自动记住原来的 Target Sheet。Attach 会写入当前侧边栏里选择的 Target Sheet。

恢复作业前，请先确认 Target Sheet。

### Recent jobs 里找不到 Instance ID 怎么办？

Recent jobs 只保留当前浏览器 1 天内的最近 10 条记录。如果列表中没有目标作业，可以从之前复制的 Instance ID、Logview 页面或 MaxCompute 控制台作业记录中找到 Instance ID，然后手动粘贴到 Attach 输入框。

### Attach 到失败的作业会怎样？

插件会查询该 Instance 的 Task 状态。如果 Task 已失败，会显示失败摘要。Attach 不会自动重试或重新提交 SQL。

如需重新运行，请回到 SQL 编辑器重新点击 **Run Query**。

## 建议给用户的使用方式

对于预计运行时间较长的 SQL，建议用户：

1. 点击 **Run Query** 后立即复制 Instance ID。
2. 打开 Logview 链接确认作业在 MaxCompute 侧正常运行。
3. 如果 Google Sheets 页面中断，重新打开侧边栏。
4. 选择正确的 Target Sheet。
5. 使用 **Attach to existing job** 恢复作业并写回结果。

这个流程可以避免因为 Google Apps Script 单次请求时间限制导致长 SQL 在前端等待阶段失败，同时保留用户手动恢复和指定写入 Sheet 的能力。

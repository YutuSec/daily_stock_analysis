# 多 LLM 本地分析与回测对比

该入口用于在一次本地运行中并行调用多个已配置的 LiteLLM 模型/路由，为相同股票列表分别生成独立 Markdown 报告，并写入本地 `analysis_history`。后续回测可通过实际的 `model_used` 精确筛选，比较通用方向准确率、持仓决策准确率和模拟收益率。

## 1. 配置模型渠道

沿用项目现有的 `LLM_CHANNELS` 配置，不需要新增密钥格式。例如：

```dotenv
LLM_CHANNELS=deepseek,gemini
LLM_DEEPSEEK_API_KEY=your-deepseek-key
LLM_DEEPSEEK_MODELS=deepseek-chat
LLM_GEMINI_API_KEYS=your-gemini-key
LLM_GEMINI_MODELS=gemini-2.5-flash

STOCK_LIST=002714
DATABASE_PATH=data/stock_analysis.db
```

`.env`、本地 SQLite 数据库和 `reports/` 均已被 `.gitignore` 忽略，不要把真实密钥加入版本控制。

## 2. 同批生成多份报告

模型参数使用项目已解析的完整模型/路由名：

```bash
python scripts/run_llm_comparison.py \
  --stocks 002714 \
  --models deepseek/deepseek-chat,gemini/gemini-2.5-flash
```

多个股票或模型可用英文逗号分隔。若省略 `--stocks`，脚本读取 `STOCK_LIST`。每个模型内部默认串行分析股票，可用 `--workers` 调整；所有指定模型并行执行。

脚本会：

- 为各模型流水线冻结同一个参考时间并使用同一股票列表；
- 强制使用标准分析链路，并关闭跨模型 fallback，避免 A 模型失败后实际落到 B 模型；
- 在本地 SQLite 中分别保存各模型的 `AnalysisHistory`；
- 输出到 `reports/llm_comparison/<批次时间>/`，包含每个模型的 Markdown 和 `manifest.json`；
- 在 manifest 中记录配置路由、实际 `model_used`、分析 query id 和报告路径。

各模型流水线会分别读取行情和新闻。第三方实时响应在并发期间可能变化，因此这是“同批次、同冻结时间”的对比，不是逐字节相同输入的离线基准测试。若需要严格实验，应固定历史行情与新闻快照后再运行。

## 3. 在 Web 中选择或同时运行模型

启动 Web/API 服务后，分析页会从 `GET /api/v1/analysis/models` 读取已配置的模型路由：

- 模型下拉框只影响当前提交，不会写回或覆盖 `.env`；
- 点击“分析”时，请求携带单个 `model`，服务端把标准分析与 Agent 分析都锁定到该模型，并禁用本次请求的跨模型 fallback；
- 模型下拉框也会让首页历史、自选最新报告和今日报告只展示该模型的数据；
- 可从分析输入框旁的自选下拉框直接选入股票；
- 点击“多模型运行”时，请求携带当前全部已配置的 `models`，服务端为同一股票分别创建任务并保存独立历史报告；
- 回测页切换模型后立即筛选结果与汇总指标，并按后端实际返回的全部配置模型动态展示表现卡片。
- 持仓页使用同一份动态模型列表，可针对同一份分析时持仓上下文运行选中的单个模型或全部已配置模型；每个模型独立入库，单个重复任务不阻止其他模型提交。

### 统一决策契约

新报告使用 `raw_score -> guardrail -> adjusted_score -> action` 的单向契约。兼容字段 `sentiment_score` 等于最终 `adjusted_score`，`operation_advice`、`action_label` 和 `decision_type` 都从最终动作派生。可选数据缺失只进入 `limitations` 并降低置信度，不能把评分硬压到 59；只有完整、可审计且算术一致的结构化 `guardrail` 才能改变评分。空仓/持仓场景动作必须由模型显式写入 `position_actions`，系统不会从通用动作或旧自由文本补造。

对应的 API 请求示例：

```bash
# 单模型，仍可使用同步或异步模式
curl -X POST http://127.0.0.1:8000/api/v1/analysis/analyze \
  -H 'Content-Type: application/json' \
  -d '{"stock_code":"002714","async_mode":true,"model":"deepseek/deepseek-chat"}'

# 多模型必须使用异步模式
curl -X POST http://127.0.0.1:8000/api/v1/analysis/analyze \
  -H 'Content-Type: application/json' \
  -d '{"stock_code":"002714","async_mode":true,"models":["deepseek/deepseek-chat","gemini/gemini-2.5-flash"]}'
```

模型名必须精确匹配 `/api/v1/analysis/models` 返回的路由名。多模型请求至少包含两个不同模型，且一个请求产生的“股票数 × 模型数”任务总数仍受 50 个任务上限约束。

## 4. 按模型执行和查询回测

等待足够的未来交易日后，可只回测其中一个模型。`model_used` 应采用 `manifest.json` 的 `actual_models` 值：

```bash
curl -X POST http://127.0.0.1:8000/api/v1/backtest/run \
  -H 'Content-Type: application/json' \
  -d '{"code":"002714","model_used":"deepseek/deepseek-chat","min_age_days":0}'
```

查询单条回测结果：

```bash
curl --get http://127.0.0.1:8000/api/v1/backtest/results \
  --data-urlencode 'code=002714' \
  --data-urlencode 'model_used=deepseek/deepseek-chat'
```

查询单股模型表现：

```bash
curl --get http://127.0.0.1:8000/api/v1/backtest/performance/002714 \
  --data-urlencode 'model_used=deepseek/deepseek-chat'
```

整体模型表现使用 `/api/v1/backtest/performance`，同样传 `model_used`。不传该参数时保持旧行为，统计全部模型的历史记录。

旧 SQLite 数据库启动时会为 `analysis_history` 增加可空的 `model_used` 列，并从合法的 `raw_result.model_used` 幂等回填。缺少模型标记的旧记录继续参与无筛选汇总，但不会匹配具体模型筛选。

### 两套回测指标

回测结果同时提供方向指标和持仓决策指标：

- 方向指标读取报告中冻结的通用最终 `action`，并继续兼容没有结构化动作的旧报告；收益起点是分析日收盘价。
- 持仓决策指标只在报告生成时已保存 `holding_state=holding` 且存在 `position_actions.holding.action` 时适用。`buy/add` 要求未来涨幅超过中性区间，`hold` 按不下跌评估，`reduce/sell/avoid` 按未来明显下跌评估；`watch/alert` 因没有明确暴露动作而不可评估。
- 回测不会读取当前持仓，也不会在历史报告或回测结果中保存账户、数量、成本等持仓隐私。旧报告缺少上述冻结字段时，持仓指标保持 `unavailable`，不会从自由文本或当前持仓反推。

逐条结果通过 `holding_action_applicable` 与 `holding_action_evaluable` 区分“适用”和“具备未来行情、可计分”。`holding_decision_return_pct` 是执行收益：保持/增加多头时等于股票窗口收益，防御动作按转为现金记为 0，并不表示“避免损失的幅度”。汇总 API 新增 `direction_evaluable_count`、`holding_applicable_count`、`holding_evaluable_count`、`holding_decision_accuracy_pct` 与 `avg_holding_decision_return_pct` 等字段。模型筛选同时作用于两套指标。

结果列表的 `action/action_label/operation_advice` 展示报告冻结账户状态对应的有效动作；例如空仓分支为观望、持仓分支为减仓时，列表显示观望。方向指标仍评价通用最终动作，持仓指标仍单独评价持仓分支，原始两套指标语义保持不变。

已有同一 `BACKTEST_ENGINE_VERSION` 的结果不会在启动迁移时自动重算；需要纳入新字段时，请使用 `force=true` 重跑。旧报告本身没有冻结持仓动作时，即使强制重跑也仍保持 `unavailable`。

## 回滚

代码回滚后，已有 `model_used` 列和本地报告无需删除；旧代码会忽略该列。若只需停止多模型运行，不再执行比较脚本即可，普通分析与无筛选回测保持原入口。

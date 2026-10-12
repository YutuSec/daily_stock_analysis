# 机会池研究隔离与市场风险实验实施计划

> 执行者：实施时使用 `superpowers:subagent-driven-development` 或 `superpowers:executing-plans` 逐项执行；当前仅为可评审计划，不开始实施，不执行 commit/tag/push。

**Goal:** 在不污染现有自选和模拟账户的前提下，让机会池持续研究，并以隔离 A/B/C 模拟账本检验有限放宽红灯是否改善净收益。

**Architecture:** 研究用途在报告首次落库前冻结，所有交易消费者统一校验。实验以独立数据库和工作进程运行同一个入口及模拟引擎，共享时点一致的研究/行情输入；在现有模拟交易页和 API 契约上增加范围选择及只读结果汇总。

**Tech Stack:** 现有 Python、SQLAlchemy、FastAPI、任务调度、React/TypeScript 和 pytest；不新增交易引擎或券商依赖。

**Spec:** [完整设计](../specs/2026-09-27-opportunity-market-risk-design.md)。状态：待评审设计，以下全部为待办。

## Global Constraints

- A 股股票、只做多、无杠杆、纸面交易。主账户和现有自选沿用 A。
- 第一轮不修改候选评分、`position` 时域、两日确认、入场结构、退出或既有费用。
- B1 只新增红灯例外；绿黄灯逻辑与 A 一致。0.25 倍不是胜率或收益承诺。
- 研究专用属性首次落库前保存；模型输出和外部 API 不得提升权限。
- 当前脏工作区原样保留；不默认切分支、重置、清理或提交。
- 新配置同步 `.env.example`、配置注册与双语专题文档；界面和行为改动同步 `docs/CHANGELOG.md`。
- 所有独立日期使用交易日历；来源、输入/输出哈希、策略/模型版本和可见时刻可追溯。
- 机会池买入/监控与现有模拟交易同页；不新增交易页面、侧栏入口、三份实验引擎或通用策略平台。范围选择与候选来源分别建模。
- 每个新增模块对应 R1–R8 的明确缺口；保持规则单一实现，不能通过复用之名遗漏权限、确认、隔离、费用、成交限制、恢复或收益验证。

## Review Focus

1. 报告初次保存后的即时 reconcile 早于机会池后置来源写入：任务 1 必须覆盖真实保存入口。
2. 研究报告成为最新报告时遮蔽合法持仓报告，或者改变确认/止损：任务 1 先过滤再取 latest。
3. 独立账户仍共享 thesis、计划键和单例数据库：任务 4 必须进程和数据库同时隔离。
4. 红灯后计划、跨日确认及补偿恢复偷偷改变授权：任务 2、3、5 使用当前证据并测试时间边界。
5. 持仓分化后各组 LLM 输入变化，使单变量实验失真：任务 2、4、6 检查共享研究哈希及账户无关输出。

## Task 1 — R1：可信研究权限及全部消费者

**修改文件：**

- `src/storage.py`：报告首次持久化用途字段及兼容迁移。
- `src/core/pipeline.py`、`src/services/analysis_service.py`：用途由可信任务上下文传播至初次保存，而非模型 JSON。
- `src/services/decision_signal_service.py`、`src/services/paper_trading_service.py`、`src/repositories/paper_trading_repo.py`：计划、补偿、策略转移、风险修订、bootstrap、升级及再分析共同消费权限。
- `api/v1/endpoints/paper_trading.py`、`api/v1/schemas/paper_trading.py`：输出阻断原因，不接受客户端授权覆盖。

**接口决定：**新增可信字段 `execution_intent`（`research_only` / `paper_candidate`）、`execution_scope`（`primary` / `experiment:<id>:<arm>`）、`decision_as_of`、`execution_policy_version`。用途 immutable；新报告缺失/未知用途不授予执行权限。迁移时只为既有普通报告按原契约建立明确 legacy 基线，不重写历史成交。

现有 `create_plan_for_report(source_report_id, *, decision_signal_id=None)` 保持调用兼容，读取存储权限。统一校验必须先于任何策略确认或持仓修改；被拒的结果包含稳定原因 `research_only` 或 `execution_scope_mismatch`。执行 scope 来自受信任服务实例；API 不传入它。

- [ ] 在 `tests/test_strategy_report_producer_contract.py`、`tests/test_paper_trading_service.py`、`tests/test_paper_trading_api.py` 添加首次保存即时补偿、手动建计划、定时补偿、升级/重启和最新报告遮蔽反例，确认旧实现不能通过。
- [ ] 实现首次保存权限及全部消费者；对同日内容复用保留独立用途，不改写已存在自选报告。
- [ ] 运行上述测试；断言主账户计划、确认、风险版本和资金未变，并验证合法旧报告仍可被选中保护持仓。
- [ ] 同步中英文文档的研究权限契约；记录兼容迁移和回退限制。

## Task 2 — R2：红灯研究、研究复核与共同输入

**修改文件：**`src/services/market_opportunity_service.py`、`src/services/market_opportunity_followup_service.py`、`src/repositories/market_opportunity_repository.py`、`src/core/pipeline.py`、`src/services/analysis_context_builder.py`、`src/scheduler.py`、`src/services/runtime_scheduler.py`、`src/config.py`、`src/core/config_registry.py`、`.env.example`。

**接口决定：**沿用 `analyze_run`、`process_pending_jobs` 的外部签名；内部新增可信研究任务种类，与既有执行任务分开。研究允许条件为 paper、工程门槛完成、当前日期/所需数据有效、预算可用；risk_off 本身不拒绝研究。研究跟踪不直接增加主账户确认，实验确认由隔离消费者计算。

共同证据包包含 `experiment_id`、`bundle_id`、`as_of`、`available_at`、候选顺序、模型/提示词/政策版本、输入/输出哈希、账户无关的买入/持有/退出条件。`bundle_id` 幂等；首次与复核都使用当天真实新数据。最早可执行时刻沿用现有时段契约。

第一批只新增原来被红灯阻断的研究通路，绿黄灯原有合法任务保持原用途。A/B/C 共同研究走独立入口，不从带主账户仓位上下文的普通报告冒充账户无关研究。

- [ ] 在 `tests/test_market_opportunity_service.py`、`tests/test_market_opportunity_followup_service.py` 和 `tests/test_strategy_report_producer_contract.py` 固定红灯可研究但不执行、observe 不调用模型、预算不足、跨日迟到、同日重复、原报告复用和账户输入泄漏反例。
- [ ] 实现研究调度、每日共享预算默认 16 且取更严格限制；先复核、后新候选，重试计费。配置名称与注册入口在实现时统一评审，不增加每组独立模型调用预算。
- [ ] 运行上述测试及 `tests/test_scheduler_background.py`、`tests/test_runtime_scheduler_service.py`，用两个有效会话驱动真实仓储，证明确认只发生在实验范围。
- [ ] 对一批当前数据形成完整证据包；显式区分网络失败、无候选、观望和成功研究。在线调用成本必须受预算限制。

## Task 3 — R3：B1 纯函数与统一执行消费

**修改文件：**`src/services/daily_market_context.py`、`src/services/paper_strategy_policy.py`、`src/services/paper_trading_service.py`；如函数无法保持现有模块职责，可新增唯一模块 `src/services/opportunity_market_risk_policy.py`，不得复制成交引擎。

**接口决定：**`evaluate_opportunity_market_policy(snapshot, *, arm, policy_version) -> dict` 为无数据库、无系统当前时间、无模型调用的纯评估。结果至少包含 `entry_allowed`、`allocation_multiplier`、`max_symbol_exposure_pct`、`max_total_exposure_pct`、`reasons`、`as_of`、`policy_version` 和输入哈希；输入携带冻结基础上限及可信指数窗口。arm 只接受注册的 A/B/C。

遵循设计第 4.3 节的 MA 与冲击公式；60 日标准差使用总体标准差，排除当日。A/现网默认分支不变；B/C 仅由受信任实验服务启用。倍率在预算链路只使用一次，风险评估、size 与成交前复检消费同一决策，不允许 size 放行但 resolver 再按旧红灯拒绝。

- [ ] 新增 `tests/test_opportunity_market_risk_policy.py`，覆盖边界等号、3 种红灯情形、未知 arm、零标准差、指数代码错误、断日、未来 K、陈旧大盘和 0.25 倍累计容量。
- [ ] 实现纯规则及现有执行链路的单一消费入口；保留已有市场状态作为事实，不把主库 risk_off 改写为 normal。
- [ ] 运行新增测试及 `tests/test_daily_market_context.py`、`tests/test_market_regime_session_validity.py`、`tests/test_paper_asset_exposure_policy.py`、`tests/test_paper_capital_service.py`。断言同一输入绿黄灯 A/B 一致、红灯其他硬风控不能绕过。

## Task 4 — R4：隔离工作进程与证据导入

**新增文件：**`src/services/opportunity_experiment_service.py`、`scripts/run_opportunity_experiment.py`、`tests/test_opportunity_experiment_service.py`。复用 `PaperTradingRepository(db_manager=...)`、`PaperTradingService(repo=...)`，不另写买卖算法。

只有一个工作入口，由注册配置分别启动 A/B/C 隔离实例；编排模块只负责注册、证据分发、实例生命周期与结果引用，不复制计划、监控、买卖或费用代码。

**接口决定：**实验注册表记录实验 ID、A/B/C 的独立数据库连接引用、共同初始资金、版本哈希、开始时刻及运行状态。连接凭据不进入 API/日志。数据库地址必须三者互异且不等于主库，实例各用独立进程；全局单例也只能指向本组库。

`import_research_bundle(bundle, *, experiment_id, arm) -> dict` 仅由受信任实验编排器调用。校验日期/哈希/版本/注册范围后，生成本组新报告/决策映射并保留原研究引用；不修改原研究用途。所有计划/确认/补偿/风险修订从本组映射读取授权，不能直接把研究报告改成可执行。API 不能上传任意 scope 让报告变成可执行。

各组引用同一行情批次 ID，先记录输入再执行；失败恢复消费原批次，不能重新取更有利报价补填。停用新增风险和退出保护是两个状态。实验关闭通知、持仓同步及向主账户回写。

每组只注册本组任务；主调度不扫描实验数据库，不加载主账户可写配置，出错不得回退主数据库。验收检查网络通知调用、同步记录和调度注册，而不仅检查成交表。

- [ ] 写入三组相同模型、股票、日期的真实数据库集成反例，覆盖确认、计划、风控、再分析、持仓、成交和现金隔离。
- [ ] 实现工作进程、幂等导入与现有引擎调用；验证同名逻辑键在三库中互不污染。
- [ ] 启动独立工作进程执行集成测试，覆盖进程重启、重复批次、损坏包、错组包、主库地址误填和一组故障；断言主库交易表/策略表未变。
- [ ] 保存原始运行日志和账本差异；模拟数据证明工程路径，实际前瞻数据另列，不能互相替代。

## Task 5 — R5：监控、仓位与退出闭环

**修改文件：**`src/services/paper_trading_service.py`、`src/repositories/paper_trading_repo.py`、实验编排；复用盘中/盘后、补偿、开盘检查及退出路径。

- [ ] 在 `tests/test_paper_trading_v3_acceptance.py`、`tests/test_paper_capital_service.py`、`tests/test_opportunity_experiment_service.py` 加入“红灯中期完整→两日确认→等买点→小额成交→T+1 待退出→次日实际价退出”的完整案例。
- [ ] 同场景断言 A 不开仓，B 受 0.25 倍、同股累计 5%/组合 20% 限制，C 仍受资金/成交规则；费用扣减完整、无重复倍率、已超限不强制清仓。
- [ ] 覆盖涨停无成交证据、跳空越过买点、过期报价、买入日止损、重启重复 tick、已有仓位保护和主账户正常运行。
- [ ] 运行相关后端门禁；工程失败禁止新模拟入场，已有持仓的可用保护链路继续。真实行情未出现可成交条件时报告等待，不造交易。

## Task 6 — R6/R7：页面、组合对照与证据口径

**修改文件：**`api/v1/endpoints/market_opportunities.py`、`api/v1/schemas/market_opportunities.py`、`api/v1/endpoints/paper_trading.py`、`api/v1/schemas/paper_trading.py`、`apps/dsa-web/src/api/marketOpportunities.ts`、`apps/dsa-web/src/api/paperTrading.ts`、相应 `types/`、`pages/MarketOpportunitiesPage.tsx`、`pages/PaperTradingPage.tsx`、`src/services/market_opportunity_evaluation_service.py`、实验服务。

**接口决定：**追加 `research_state`、`execution_state`、`primary_block_reason`、`other_block_reasons`、`next_check_at`、`policy_version`、`experiment_id`、`arm`，不重解释旧字段。候选价格前瞻与成交净值分栏；主账户与实验不可汇总成同一收益率。

现有模拟交易读取接口统一追加可选 `scope_id`，省略保持主账户兼容；范围元数据由同一 API 模块提供，后端允许列表解析，不能接受数据库地址。范围选择不修改 Task 1 的可信执行权限。查询响应和行/缓存/深链接身份包含范围；候选来源 `origin` 单独筛选并由冻结证据派生。允许的实验控制通过注册编排器执行，不复用默认主账户 mutation 来冒充实验控制；不支持的操作后端拒绝。

保持现有页面路由，原页增加运行范围与来源选择、同页策略对比，复用原表格/详情/风险展示；不新增实验交易页。机会池页只新增指向原模拟交易页的定位链接。所有数据请求，包括 status、心跳、账户和明细，都必须用所选范围；来源筛选不改变完整账户的风控和总收益口径。

- [ ] 在既有市场机会/模拟交易 API 与页面测试中覆盖“研究成功但未授权”“确认 1/2”“等待买点”“已成交”“样本不足”“试验暂停但退出监控继续”。
- [ ] 实现现有页的范围/来源筛选、只读结果汇总与同页比较；默认保留主账户。切换清空旧表单/操作幂等键/分页并忽略迟到请求；加载失败不得展示上一范围数据；禁止实验内容发给主账户建计划。
- [ ] 扩展现有 API/页面测试：同数字 ID 不串组、快速切组旧请求不回填、深链接准确定位、错误范围不回退主账户、看 B 时运行监控只作用于 B、实验入出金/同步后端拒绝、来源过滤不缩小账户风险/不阻断其他持仓退出、研究记录不重复成为计划或成交。
- [ ] 实现冻结评审日历：第 120 日仅作阶段描述，正式确认固定第 121–520 日、终点只检验一次；不以反复 95% 检验选择采纳日。重叠持仓按日分块，未平仓按可信市值计入，陈旧估值标记不可验收。实验清单冻结区间方法/诊断并经统计审阅后才能作优势判断。测试成本翻倍、全空仓、单笔独占收益、缺行情和外部现金流等反例。
- [ ] 执行对应后端测试，Web `npm ci`、`npm run lint`、`npm run build`；在浏览器核验真实 API 数据并保存仓库外截图。

## Task 7 — R8：运维、文档与总验收

**修改文件：**`docs/market-opportunity-pool.md`、`docs/market-opportunity-pool_EN.md`、`docs/paper-trading.md`、`docs/paper-trading_EN.md`、`.env.example`、`docs/CHANGELOG.md` 以及本次实际新增配置/调度入口。

- [ ] 同步配置默认关闭/兼容语义、模式切换、预算、实验范围、暂停和恢复说明。不得把 8/3 当收益验收。
- [ ] 执行 `./scripts/ci_gate.sh`；按实际改动运行聚焦测试与客户端构建。Docker/调度改动需在实际部署方式验证工作进程、持久卷及重启恢复。
- [ ] 独立审查 R1–R8 的生产者与消费者闭环，保存运行证据与未覆盖项；工程通过和收益证据单独出结论。
- [ ] 对照设计第 5 节复用表审查最终 diff：每个新增模块解释必要性，每个 R1–R8 保留对应入口/消费者/测试；相同数据在关闭实验时保持旧 API 和页面行为。发现重复引擎/规则/交易页必须收敛后才能验收。
- [ ] 交付回退演练：停止新研究与新增风险、保留实验账本、已有持仓继续保护，主账户 A 无变化。不通过隔离/时序验证不得启动 B。

## 计划验收与后续执行边界

本轮验收对象是文档的完整性、路径真实性、设计一致性与独立审阅，代码和运行收益测试不适用。上述任务未实施；收益评审依赖未来真实交易日，不可能通过一次开发验收提前完成。

下一次若按本方案执行，先完成任务 1–2，再完成 3–5 并启动隔离模拟，最后补齐持续评估；任务 6 的最小状态与账本展示需随模拟启动同步提供。不开启真实交易，不自动改主账户政策。

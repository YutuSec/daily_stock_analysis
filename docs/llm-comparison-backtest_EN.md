# Local multi-LLM analysis and backtest comparison

This workflow calls multiple configured LiteLLM models/routes in one local run, saves separate Markdown reports and `AnalysisHistory` records, and lets the backtest APIs compare both general direction accuracy and holding-decision accuracy by the actual `model_used` value.

## Configure and run

Reuse the existing channel configuration; no new secret format is required:

```dotenv
LLM_CHANNELS=deepseek,gemini
LLM_DEEPSEEK_API_KEY=your-deepseek-key
LLM_DEEPSEEK_MODELS=deepseek-chat
LLM_GEMINI_API_KEYS=your-gemini-key
LLM_GEMINI_MODELS=gemini-2.5-flash
STOCK_LIST=002714
DATABASE_PATH=data/stock_analysis.db
```

Then run:

```bash
python scripts/run_llm_comparison.py \
  --stocks 002714 \
  --models deepseek/deepseek-chat,gemini/gemini-2.5-flash
```

All specified model pipelines run concurrently with the same frozen reference time. Cross-model fallback and Agent mode are disabled for the comparison. Reports and a manifest are written under `reports/llm_comparison/<run-id>/`; both the report directory and local database files are ignored by Git.

Each pipeline still reads market and news inputs independently, so third-party real-time response differences may affect the inputs. For a strict offline benchmark, freeze both market and news snapshots first.

## Select or run configured models in Web

When Web/API is running, the analysis page loads configured route names from `GET /api/v1/analysis/models`:

- The model dropdown applies only to the current submission and never rewrites `.env`.
- A normal analysis request sends one `model`. The backend locks standard and Agent generation to it and disables cross-model fallback for that request.
- The model selector also filters Home history, watchlist latest reports, and today's reports to that model.
- The adjacent watchlist selector can fill the analysis input directly.
- “Run all models” sends every currently configured route in `models`, creating a separate task and persisted report per model.
- Changing the backtest model immediately filters results and summaries, while comparison cards are rendered dynamically for every configured route returned by the backend.
- Portfolio analysis uses the same dynamic model list and can submit either one selected model or all configured models against the same report-time portfolio context. Each model persists independently, and a duplicate model task does not block the others.

### Unified decision contract

New reports follow a one-way `raw_score -> guardrail -> adjusted_score -> action` contract. Compatibility field `sentiment_score` equals final `adjusted_score`, while `operation_advice`, `action_label`, and `decision_type` are derived from the final action. Missing optional data only adds `limitations` and may lower confidence; it cannot force a score to 59. Only a complete, auditable, arithmetically consistent structured `guardrail` may change the score. Flat/holding scenario actions must be explicitly produced in `position_actions`; the system does not synthesize them from the general action or legacy prose.

API examples:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/analysis/analyze \
  -H 'Content-Type: application/json' \
  -d '{"stock_code":"002714","async_mode":true,"model":"deepseek/deepseek-chat"}'

curl -X POST http://127.0.0.1:8000/api/v1/analysis/analyze \
  -H 'Content-Type: application/json' \
  -d '{"stock_code":"002714","async_mode":true,"models":["deepseek/deepseek-chat","gemini/gemini-2.5-flash"]}'
```

Model names must exactly match the route names returned by `/api/v1/analysis/models`. A comparison request accepts at least two distinct models, and the stock-count times model-count total remains subject to the 50-task request limit.

## Filter backtests by model

Use an `actual_models` value from `manifest.json`:

```bash
curl -X POST http://127.0.0.1:8000/api/v1/backtest/run \
  -H 'Content-Type: application/json' \
  -d '{"code":"002714","model_used":"deepseek/deepseek-chat","min_age_days":0}'

curl --get http://127.0.0.1:8000/api/v1/backtest/results \
  --data-urlencode 'code=002714' \
  --data-urlencode 'model_used=deepseek/deepseek-chat'

curl --get http://127.0.0.1:8000/api/v1/backtest/performance/002714 \
  --data-urlencode 'model_used=deepseek/deepseek-chat'
```

`GET /api/v1/backtest/performance` accepts the same filter for overall metrics. Omitting `model_used` preserves the existing all-model behavior.

Existing SQLite databases add the nullable model column at startup and idempotently backfill it from valid `raw_result.model_used` values. Older untagged records remain available in unfiltered metrics but do not match a specific model filter.

### Two backtest metric families

Each backtest now exposes both a general direction metric and a holding-decision metric:

- Direction evaluation reads the frozen general final `action`, while retaining legacy text compatibility for older reports. Returns start at the analysis-day close.
- Holding evaluation is applicable only when the report saved `holding_state=holding` and an explicit `position_actions.holding.action` at generation time. `buy/add` requires upside beyond the neutral band, `hold` expects no material decline, and `reduce/sell/avoid` expects a material decline. `watch/alert` is unavailable because it does not define an exposure action.
- Backtesting never reads the current portfolio and never persists account, quantity, or cost details. Older reports without the frozen fields remain `unavailable`; no free-text or current-position inference is performed.

Per-result fields distinguish `holding_action_applicable` from `holding_action_evaluable` (enough future data to score). `holding_decision_return_pct` is an execution return: it follows the stock for actions that keep or add long exposure and is zero for defensive cash actions; it is not an avoided-loss amount. Summary APIs add `direction_evaluable_count`, `holding_applicable_count`, `holding_evaluable_count`, `holding_decision_accuracy_pct`, and `avg_holding_decision_return_pct`. Model filtering applies identically to both metric families.

Result-list `action/action_label/operation_advice` fields display the effective action for the report-time frozen account state. For example, an empty-position watch branch paired with a holding reduce branch is displayed as watch. Direction metrics still evaluate the general final action and holding metrics still evaluate the holding branch, preserving the two original metric families.

Existing rows under the same `BACKTEST_ENGINE_VERSION` are not automatically recomputed during startup migration. Use `force=true` to recompute eligible historical reports. Reports that never froze a holding action remain `unavailable` even after a forced run.

## Rollback

Stopping the comparison only requires no longer running the script. Older application code ignores the added SQLite column, so generated reports and historical records do not need to be deleted.

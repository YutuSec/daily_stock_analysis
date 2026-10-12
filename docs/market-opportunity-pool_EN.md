# Market Opportunity Pool

The market opportunity pool turns structured themes from complete post-market reviews into a bounded, auditable, and rollback-safe candidate pool. It is not the permanent watchlist and it is not a buy signal. Even when candidates later reach model analysis and paper trading, they still have to pass the existing report contract, opening confirmation, quote freshness, sizing, account risk, T+1, stop-loss, and target rules.

## Current delivery status

- API and Web surfaces are available for runs, themes, candidates, model analysis jobs, rejection reasons, provenance, and no-plan reasons, with mode-gated actions for discovery-only and candidate analysis.
- The feature is behind configuration and defaults to `MARKET_OPPORTUNITY_MODE=off`; mode changes are now shared database runtime settings, so Web/API saves do not require container restarts.
- `paper` mode is not considered production-ready just because the option exists. At least three trading days of real `observe` rollout are required first.
- Strategy v3 records complete daily CSRC board snapshots from one trusted source before analysis. Cold-start days are still formal observations, so the pool can accumulate evidence instead of staying unused while history is short.
- The page does not provide watchlist mutation, broker connection, or live trading buttons.

## Mode

```env
MARKET_OPPORTUNITY_MODE=off
```

`MARKET_OPPORTUNITY_MODE` in `.env` is only the bootstrap/default value when no database runtime setting exists. Saving from the opportunity page, or calling `PUT /api/v1/market-opportunities/mode`, persists the mode in `runtime_settings` and appends `runtime_setting_audit`. `stock-server` and `stock-analyzer` read the same database value on the next status check/post-market opportunity task, so containers do not need to be recreated. To fall back to the `.env` default, call `DELETE /api/v1/market-opportunities/mode`; the audit trail is preserved. If the runtime database cannot be read, the system does not fall back to `.env` `paper` or `observe`; it fails closed to `off` with `mode_source=degraded` and `mode_degraded=true`.

- `off`: no new discovery or candidate analysis; historical ledgers remain readable.
- `observe`: record and display discovery evidence only; no LLM calls and no paper plans.
- `paper`: candidates may enter independent analysis by every configured model and then the existing paper-plan path; runtime enforces `MARKET_OPPORTUNITY_OBSERVE_GATE_DAYS`, which defaults to the three-trading-day observe gate. When the market regime is `risk_off`, new candidates receive research-only analysis and never enter the paper-plan path; see [Red-light research and next-session review](#red-light-research-and-next-session-review).

Turning the feature off stops new discovery without deleting history or interrupting protective exits for existing paper positions.

## Limits

```env
MARKET_OPPORTUNITY_BOARD_SOURCE=csrc_tencent
MARKET_OPPORTUNITY_DISCOVERY_MAX_THEMES=15
MARKET_OPPORTUNITY_MAX_OBSERVATION_CANDIDATES=30
MARKET_OPPORTUNITY_MAX_THEMES=3
MARKET_OPPORTUNITY_MAX_CANDIDATES_PER_THEME=3
MARKET_OPPORTUNITY_MAX_TOTAL_CANDIDATES=8
MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN=16
MARKET_OPPORTUNITY_RESEARCH_DAILY_LLM_BUDGET=16
MARKET_OPPORTUNITY_OBSERVE_GATE_DAYS=3
MARKET_OPPORTUNITY_MIN_20D_AVG_AMOUNT_CNY=100000000
MARKET_OPPORTUNITY_BLACKLIST=
MARKET_OPPORTUNITY_STYLE=
MARKET_OPPORTUNITY_RUN_TIME=
```

`MARKET_OPPORTUNITY_STYLE` is a pool-only trading style (`short_swing` / `position` / `trend`). When empty, the pool inherits `PAPER_TRADING_STYLE` and matches the paper-trading main account. When set, it drives both pool screening and the stock analyses the pool triggers (candidate analysis, red-light research, and their next-session reviews): each candidate freezes the style in effect when it was screened, and later analyses use that frozen style's history window, core moving averages, and holding horizon even if the setting changes afterwards. Paper plans created from pool candidates freeze the same style, so their stop geometry and holding horizon follow it, and paper trading's same-session reanalysis of such a plan keeps the plan's frozen style. It does not change the main account's base style (`PAPER_TRADING_STYLE`), watchlist analyses and plans, or the frozen style of existing holdings. In a weak market where medium-term screening is hard, set it to `short_swing`: the history-sufficiency gate loosens from 180 to 80 bars and trend scoring switches to MA5/10/20, so more short-swing names clear the sample-size and trend gates; switch back to empty or `position` when conditions improve. Exact `market:symbol` entries in `PAPER_TRADING_STYLE_OVERRIDES` still take precedence over this base style. Besides editing `.env` directly, the "Screening style (medium/short/long)" dropdown inside the collapsible "Settings and notes" section of the opportunity-pool page switches this key in place, persisted through `PUT /api/v1/system/config` with an optimistic version lock and auto-reload; it takes effect on the next pool run, no restart required.

`MARKET_OPPORTUNITY_RUN_TIME` sets when the scheduler runs the pool (China time, 24-hour `HH:MM`). Empty (the default) keeps the existing behavior: the pool runs right after the scheduled analysis batch. Just after the A-share close, some data sources have not yet published complete daily bars for the session, so many candidates are rejected for a missing same-day bar; in that case set an evening time such as `20:00` (19:00–23:00 recommended, no later than 23:30). When it is set:

- the scheduled analysis batch no longer runs the pool inline and logs "大盘风口机会池改为晚间运行" (pool moved to the evening run);
- the background task `market_opportunity_evening_run` checks every 5 minutes between 15:00 and 23:59 China time and, once the configured time is reached, runs the same pool flow that used to follow the batch once per A-share trading day, logging "大盘风口机会池晚间运行开始" (evening run started);
- the current session's market review must exist and no analysis batch may be running; otherwise the check is skipped and retried 5 minutes later without using up the day's attempt. Closed sessions, an unverifiable trading calendar, pool mode `off`, or a market-review region without A-shares are skipped the same way. The skip reason is recorded as the task's `last_skip_reason` in the scheduler heartbeat (for example `before_run_time`, `market_review_not_ready`, `analysis_busy`, or `trading_calendar_unavailable`);
- the run happens once per trading day across all scheduler instances. In the Docker deployment both the server's runtime scheduler and the analyzer's CLI scheduler register the task, so before starting it claims the day's session lease in the database (task name `market_opportunity_evening_session` in the scheduler heartbeat). An instance that does not get it skips with `session_claimed_by_other_instance`; if the database is briefly unavailable it skips with `session_claim_unavailable`, without running or using up the day's attempt, and retries 5 minutes later;
- a failed run is not retried the same evening, and no other instance re-runs it (matching the previous inline semantics); restarting a process after a finished or failed run does not run it again either. Only when the running instance crashes or is recreated before finishing does the lease expire after 3 hours, after which another instance can take over; the pool run for the same market review is reused through its idempotency key, so no duplicate jobs are created;
- the evening run does not hold the analysis-batch lock, so it never causes an analysis batch to be skipped, but if a scheduled analysis time falls inside the evening run, the two can overlap;
- an invalid value (such as `8:00`, `24:00`, or `20:30:00`) is treated as empty and logged as a warning.

Like `MARKET_OPPORTUNITY_STYLE`, this key can be changed on the Web settings page and is picked up at the next check without a restart.

`MARKET_OPPORTUNITY_BOARD_SOURCE` selects the daily board observation source. The current implementation defaults to `csrc_tencent` and accepts only same-source, same-taxonomy, same-session snapshots with complete identities. `MARKET_OPPORTUNITY_DISCOVERY_MAX_THEMES=15` caps themes entering discovery from the board snapshot. `MARKET_OPPORTUNITY_MAX_OBSERVATION_CANDIDATES=30` caps auditable observation candidates. These budgets are separate from analysis: model analysis remains capped by `MARKET_OPPORTUNITY_MAX_THEMES=3`, `MARKET_OPPORTUNITY_MAX_TOTAL_CANDIDATES=8`, and `MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN=16`.

`MARKET_OPPORTUNITY_RESEARCH_DAILY_LLM_BUDGET=16` caps red-light research model calls per market and trading session. Initial research, next-session reviews, and every retry share it, and the effective cap is the smaller of this value and `MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN`. `0` stops all research calls without affecting board snapshots or observation evidence; when unset, the cap is 16.

Model names are not duplicated in this feature. Runtime uses the actually configured model registry. Candidate capacity should shrink as model count grows so that adding a model does not silently skip that model for some candidates.

## Strategy v3 observation framework and v4 scoring correction

The pool first freezes the full available CSRC industry ranking for each post-close trading session as a `BoardObservation` snapshot, then selects up to 15 themes from that same source for discovery. Snapshot collection is independent of LLM report success, `risk_off`, and whether the day produces candidates; a real same-source full snapshot is useful observation evidence by itself. `theme_key` keeps readable semantics such as `industry:<name>`, while cross-day matching also checks `theme_code`, `theme_source`, `theme_taxonomy`, and `as_of` so same-name boards from different sources or changed codes are not mixed.

Cold start no longer rejects observation. With insufficient history, themes and stocks can remain in the observation pool with missing dates, source, window size, and rejection evidence. They may enter the analysis budget when stock, benchmark and liquidity evidence is complete; board-history maturity remains a soft score. Actual model calls still require the global three-healthy-session gate. A healthy zero-candidate run can count toward the three-day engineering observe gate because it proves the system completed discovery and found no eligible candidate. Upstream failures, untrusted calendars, pre-close sessions, future dates, genuinely missing trading days, and old name-only records cannot be counted as healthy days.

`risk_off` blocks plan creation and simulated entries, but no longer blocks all model analysis. In `paper` mode with a completed observe gate, new red-light candidates receive research-only analysis whose reports never create a DecisionSignal, a paper plan, or a primary-account confirmation. `observe` mode still collects evidence without calling models, and an `unavailable` regime (required data unavailable) still blocks all new analysis. Observation evidence is collected and displayed as usual. `observe` and `paper` remain distinct, and `paper` continues to require the observe gate, budgets, and execution risk controls.

Old history is not relabelled with the new source. Frozen snapshots remain audit evidence and can support same-sample comparisons. Missing full-snapshot sessions remain gaps; the system must not reconstruct old sessions from current data or turn name-only legacy reports into `csrc_tencent` healthy observations.

### Twenty-session strength affects actual selection

`opportunity-policy-v4` / `theme-policy-v4` closes the gap where `return_20d` was stored but never consumed. The `board-discovery-v2` discovery score adds daily rank (0–30), daily strength (0–20), five-session persistence (0–20), compounded five-session return clipped to −10…10, and compounded twenty-session return clipped to −20…20. Returns use percentage points: `5` means 5%. Complete negative returns subtract points.

Final theme scoring also multiplies the twenty-session component by 0.5 (at most ±10 points), adds it to the previous daily-ranking strength, and clips the result to 0–20. Breadth, liquidity, completeness and persistence weights remain unchanged, and the final score remains 0–100. Twenty-session evidence therefore affects both admission to the discovery budget and final theme ordering / a stock's primary theme.

The twenty-session window requires complete daily snapshots for twenty consecutive real trading sessions ending on the signal date, with matching source, taxonomy and code. A missing day or invalid daily return produces `null`, contributes zero, and retains status, available count and missing dates in `return_20d_evidence`. Missing history is distinct from complete negative performance and introduces no new hard waiting period. Theme API `score_components` exposes discovery components, twenty-session evidence and the final adjustment. `evidence_stage` continues to describe the five-session window; use `return_20d_evidence.status` for twenty-session maturity. New runs use the new policy key; existing batches remain frozen, and historical membership replays still earn no healthy-observation credit.

These weights are hypotheses to evaluate, not research-proven profit parameters. Acceptance should compare subsequent relative returns, actual fill rates, simulated returns after costs, and drawdowns. More candidates, fewer watch reports or hypothetical entry touches are insufficient evidence of profitability. A `watch` report listing reference entry prices requires explicit conditional-entry authorization before it can enter that execution path.

### Observation credit is separate from candidate data quality

`board-observation-health-v2` counts persisted complete board snapshots once per trading session, independently of report generation and stock screening. It verifies market, source, taxonomy, theme identities/count, snapshot hash presence, a trusted trading calendar, and same-session post-close collection time. Missing snapshots, pre-close/weekend/future dates, identity mismatches and historical membership replays cannot manufacture observation credit. Authentic existing snapshots can be revalidated read-only without fetching past board membership.

Missing stock data, rejected candidates, short board history and `risk_off` do not invalidate an already collected board observation. Incomplete candidates remain rejected; `risk_off` still blocks new entries (and allows only research-only analysis in `paper` mode). Paper mode still requires three authentic healthy sessions and never activates automatically.

`observe_gate.counting_basis` is `healthy_board_observation_days`. Existing `completed_trading_days`, `healthy_run_days` and `dates` remain compatible and refer to healthy snapshot sessions. Run API `provenance.observation_health` projects the current verified verdict; `recorded_observation_health` preserves the original frozen verdict without updating its database row. `observation_health.discovery_quality` separately exposes incomplete candidate/theme counts. Scope `discovery_candidates` covers all candidates checked by that discovery; `persisted_candidates` covers retained rows of an older run and is not its original full-market sample size.

## API

All endpoints live under `/api/v1/market-opportunities` and reuse the admin session:

- `GET /status`: mode, source, version, degraded flag, configured models, limits, and observe-gate status; `observe_gate.dates` contains the trading dates already counted by the gate and is displayed directly by the Web UI.
- `GET /runs?page=&limit=`: paginated runs with `total`.
- `GET /runs/{id}`: run detail with themes, candidates, and model jobs, plus `llm_submitted_job_count` (analysis jobs that actually reached the LLM; jobs that failed before submission are not counted).
- `GET /themes?page=&limit=`: paginated theme evidence.
- `GET /candidates?page=&limit=&model=`: paginated candidates, dynamic model states, and no-plan reasons. Each `model_states` entry carries `execution_intent` (`paper_candidate` / `research_only`), `research` (`kind` / `outcome` / `review` / `review_of`), and the report's `action` / `action_label`, so the page can separate "generated a paper plan" from "research only". A model filter projects only that model's jobs/states without hiding candidates whose job has not been created (`missing / job_not_created`). Totals count candidates rather than jobs; unrelated model jobs are not included.
- `GET /analysis-jobs?page=&limit=`: paginated model analysis jobs.
- `GET /evaluation?run_id=&windows=10,20,40`: read-only prospective evaluation that only uses bars after the candidate as-of date. The 10/20/40-session windows are an observation lens, not validated return evidence.
- `POST /discover`: discover candidates from the latest or specified market-review report only; it does not call LLMs and is rejected in `off` mode.
- `POST /runs/{id}/analyze`: analyze candidates for one specific run only; it is gated by `paper` mode, the observe gate, the actual configured models, and `MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN`. For a red-light run it creates research-only jobs only for selected candidates that have no job yet, within the daily research budget, and returns `blocked` with the research block reason when the budget is insufficient. A research-review run gains no new jobs and only continues its materialized review jobs. Primary-account confirmation follow-up runs under `risk_off` and runs whose regime is `unavailable` still return `blocked`.

List endpoints must use server-side pagination and return `total`; the Web UI must not silently truncate 500 rows and present that as the real total.

Runtime mode endpoints live under `/api/v1/market-opportunities/mode`:

- `GET`: return the effective mode, source, version, and update time.
- `PUT`: submit `mode` and required `expected_version`; optimistic locking prevents overwriting another recent mode change. Successful updates append an audit row and do not rewrite `.env`. A `paper` update returns `409 observe_gate_incomplete` without writing an override until the observe gate is complete. When no database override has ever existed, status returns `mode_version=0`; after a set-and-clear round trip, it returns the latest audit revision for that key, and the next write must submit that revision.
- `DELETE`: clear the database override with required `expected_version` so the system falls back to the `.env` default; the clear action remains audited.

## Web page

Route: `/market-opportunities`; the sidebar label is “Opportunities”. The page is ordered conclusions first, evidence second:

- A run summary at the top: trade date, run number, run status, start–end time (UTC+8), run mode, the day's market regime and its consequence (under `risk_off` it reads “research only, no paper plan”), data completeness, LLM calls actually made versus the limit (`llm_submitted_job_count` against `limits.max_llm_calls_per_run`), and the next run time from `MARKET_OPPORTUNITY_RUN_TIME` (shown as “with the scheduled analysis batch” when unset).
- A “N names sent to LLM analysis” conclusion table: only candidates that really created an analysis job, with columns for stock, theme, rule score, LLM advice (including whether it is bullish), result, and next step. Bullish rows come first, and expanding a row reveals the existing execution-evaluation evidence, missing evidence, legacy-vs-current strategy comparison, and per-model detail (job status, report ID, signal ID, paper-plan ID, execution intent, and research outcome). A plan that exists but is not executable is shown separately from no plan, together with two-session confirmation progress and the precise no-plan reason. Under a red light, research jobs read as “research only”.
- A “Pending review / confirmation” queue that collects research reviews and two-session confirmation obligations, sorted by due date, showing the due date, progress, and the spawned run number.
- Prospective evaluation: the 10/20/40 trading-session windows, model report/plan conversion, and realized paper-trading PnL summary; results without confirmed executions are labelled as proxy and are not mixed with realized trade PnL. Research jobs are counted separately as `research_jobs` / `research_reports` and are excluded from model report/plan conversion rates.
- The remaining candidates that were not sent to LLM analysis are collapsed by default, with a header showing “N others (observing N · rejected M)” plus per-reason Chinese counts; candidates that qualified but were not analysed are counted separately as “qualified, not analysed”. Expanding gives candidate detail with pagination and status filtering.
- “Settings and notes” is collapsed by default and holds mode switching, screening style, the evening run time, observe-gate rules, limits, field explanations, `provenance`, and run diagnostics. The observe gate lists the counted dates returned in `observe_gate.dates`, and an exposed empty array is shown as having no counted dates yet. In the run diagnostics, unavailable upstream data or missing market themes show as blocked and are not presented as a genuine zero-candidate result; a red-light run's `risk_blocked` means simulated execution is blocked, not that no research ran. English short reasons returned by the backend (including composite ones such as `candidate_rejected: a, b`) and run statuses are translated part by part, and the mode's English `message` is no longer rendered.

Timestamps are rendered in China time as `UTC+8`. The page has at most two levels of disclosure (conclusion rows, then the remaining candidates and settings) and no longer nests scrollable cards inside scrollable cards.

## Relationship to watchlist and paper trading

Opportunity candidates are not automatically written to `STOCK_LIST` or the permanent Web watchlist. A candidate that receives a buy-like model report may create a paper plan only after all existing paper-trading gates pass; a red-light research report never creates a paper plan, even when bullish. Opportunity scores are ranking and audit evidence only; they cannot bypass execution risk controls.

Before the LLM is called, the candidate's theme, rank, score components, quote source, snapshot date, and data quality are injected as a read-only `selection_context`. The prompt requires an independent review and a watch/avoid result when evidence is insufficient; selection is never treated as a buy instruction. A normal manual report from the same stock, model, and date is not reused for an opportunity job unless it contains the same opportunity candidate key.

All eligible stocks are ranked globally before per-theme and total caps are applied, preventing earlier constituent order from starving a higher-scoring later theme. Technical scoring windows consume the pool's effective trading style: by default it inherits `PAPER_TRADING_STYLE`; when `MARKET_OPPORTUNITY_STYLE` is set it becomes the pool base style and is frozen onto the candidate for later stock analyses and pool paper plans, without changing the main account's base style, watchlist plans, or existing holdings (exact `market:symbol` entries in `PAPER_TRADING_STYLE_OVERRIDES` still take precedence). For example, the position style includes its medium-term core moving-average windows instead of a fixed short-term template. The score is still a discovery filter only. The full stock analysis and execution risk layer own the action.

## Dates, retry, and two-session confirmation

Manual analysis validates old batches against an independently proven current session, never against their own stale dates. An unavailable calendar blocks new submissions. Scheduled discovery first globally recovers submitted work. Stored reports are located before expiry handling: expired reports retain audit linkage but cannot create current plans. Cross-session work without a report is closed. Unknown same-session submissions exceeding the existing 900-second claim window become terminal failures, not automatic duplicate LLM calls; only known synchronous failures use the existing bounded retry policy.

Same-day reuse requires a nonempty snapshot fingerprint, selection contract version, and identical frozen evidence (themes, score components, quotes and dates). A matching symbol or hash alone is insufficient. Transient run/job IDs may differ. The execution purpose and scope frozen at the reused report's first save must also match the current job (`paper_candidate/primary` for existing opportunity execution jobs), so a research-only report is never reused as an executable job result, and reuse never rewrites the original report's purpose. A new analysis stores `paper_candidate/primary` and the candidate snapshot date at first save; see the [paper trading guide](paper-trading_EN.md) for the authorization rules.

Scoring consumes existing style settings: short swing requests 80 bars, MA5/10/20 and 20-session return/volatility; position requests 180 bars, MA10/20/60 and 60-session return/volatility; trend requests 500 bars, MA20/60/120/250, 250-session return and 120-session volatility. Excess benchmark returns are normalized to a common 20-session score scale. Short-term price structure remains an auxiliary timing component. Insufficient style history cannot pass selection. This aligns the screening contract; it is not evidence of improved investment returns.

- Stock, benchmark, board snapshot, candidate `as_of`, and opportunity run must match the exact target session. A future `valid_until` does not authorize claiming an old run on a new session.
- Missing themes, upstream failures, or universally unavailable theme constituents create a `blocked` run with an explicit reason. The same idempotency key can refresh that run until a job has been submitted or completed. A healthy terminal zero-candidate run exists only when discovery completed and no stock passed.
- Observe-gate counting uses real trading sessions and real snapshots. A missing trading day cannot be skipped and later filled as healthy; a complete healthy zero-candidate snapshot can count toward the three-day engineering gate, but it is not evidence that the strategy has positive returns.
- If the first full analysis reaches only entry confirmation `1/2`, a durable next-session follow-up obligation is stored on the first job. On the next session the system must fetch current evidence and run the same model through the full analysis again; it does not reuse the first report or add the stock to the permanent watchlist.
- Follow-up run and job keys are deterministic, so scheduler retries do not duplicate work. The obligation is valid only on its specified next trading session; a missed session expires instead of replaying a stale signal later. A failed confirmation, watch/avoid result, or still non-executable plan keeps entry frozen. Stop, target, and T+1 protection for existing positions remain active.
- During scheduled single- or multi-model analysis, opportunity compensation and paper-protection ledgers are attempted independently before failures are aggregated. Failure of the first auxiliary does not skip the second. When `MARKET_OPPORTUNITY_RUN_TIME` is set, the batch runs only the paper-protection ledger and the pool moves to the evening background task; see [Limits](#limits).

## Red-light research and next-session review

- Admission: when the mode is `paper`, the observe gate is complete, the session regime is `risk_off`, configured models exist, and the research budget covers the whole model group, the session's selected candidates receive research-only jobs for every configured model. `observe` mode calls no model. An `unavailable` regime (required data unavailable) creates no research, and a due research review then ends with `market_regime_unavailable`.
- Authority: a research report is frozen as `research_only/primary` at its first save. It receives no real or paper position context, never creates a DecisionSignal, a paper plan, or a thesis, and never counts toward the primary account's two-session confirmation. The job evidence records reconciliation as `execution_not_authorized/research_only`. Classification follows the report's own action without score alignment: only buy/add are bullish; every other action (watch, hold, reduce, sell, avoid, alert) and unrecognized actions are non-bullish, so the program never turns a watch into a buy. Each research record freezes the report ID, model, prompt fingerprint, input and output hashes, candidate identity, and the report's conditional price points, containing only account-agnostic facts.
- Next-session review: a bullish initial result reserves one research-budget slot on the next real trading session and records a review obligation. On the due session the same model re-runs a full analysis on freshly fetched data. The result stays research-only, forms no primary-account confirmation, and never schedules another review. A review runs only on its due session; a missed session becomes `expired` and is never replayed or confirmed later. When the due session's budget is already full, the review is recorded as `skipped` (`research_review_budget_unavailable`).
- Budget: each market and trading session shares one research budget, and reviews take priority: review slots are reserved on the previous session, new candidates can only use the remaining slots, and admission is computed for the whole configured-model group, never for a subset of models. Every submitted model call, including retries, takes one slot; slots are never released, and the same attempt is not charged again after a restart. At call time the budget ledger is the hard cap: once retries exhaust it, the remaining research jobs end with `research_budget_exhausted`. The run's `provenance.research` records admission, and block reasons are `configured_models_missing`, `research_budget_zero`, `research_budget_insufficient_for_configured_models`, or `research_budget_exhausted`; a session with no selected candidates reports no budget block.
- Reuse and restarts: a research-only report is generated once per stock, model, and trading session. Initial research and review reuse each other's report, but a research report is never reused as an executable job result. When the review session rediscovers the same stock, the new initial research reuses that session's report and schedules its own review from the new signal date, so downstream consumers should deduplicate by report ID. A submitted research job whose report cannot be found after a restart stays uncertain within the existing 900-second claim window and is not resubmitted; it fails terminally only after the window. A research report recovered on a later session is archived only: it neither schedules nor completes a review.
- Distinguishing results: the post-market scheduler result adds `research_outcomes`, counting research jobs in this pass's discovery run and due review runs as `bullish`, `non_bullish` (watch and other non-bullish actions), `analysis_failed`, `budget_exhausted`, `data_unavailable` (required data unavailable), `expired` (missed session), and `in_progress`. Board or constituent source failures still appear as a `blocked` run with reasons such as `market_themes_missing` or `theme_constituents_unavailable`; a genuine zero-candidate result has `selected_candidate_count=0` and no block reason. The scheduler log line "大盘风口机会池完成" also prints `research_block` and `research_outcomes`.

## Automatic work and budget boundaries

- Eligible themes are scored before selection, then capped by the smaller user and regime limits. Over-cap themes retain rejection evidence instead of being labelled selected. Ties use source rank and theme key for stable ordering.
- Observation breadth and analysis breadth are separate: each day can send up to 15 themes into discovery and keep up to 30 candidates as observation evidence; only up to 3 themes and 8 candidates enter analysis, with at most 16 model calls per run. Red-light research is additionally capped by the shared daily research budget.
- `MARKET_OPPORTUNITY_MAX_LLM_CALLS_PER_RUN=0` preserves a zero model-call budget for newly created runs; because the research cap takes the smaller value, red-light research also calls no model. With selected candidates but insufficient budget for the complete configured-model group, the run is explicitly blocked, not silently completed with no jobs or a subset of models. Previously frozen run budgets are not retroactively rewritten by this fix.
- On closed days or untrusted calendars, the automatic entry point does not discover new candidates, materialize followups, or claim new model jobs. Already-submitted work may reconcile under the existing recovery rules. Manual research is not globally prohibited, but existing mode, date, and budget gates remain in force.
- When the market is `risk_off`, automatic work still collects board snapshots and observation evidence. In `paper` mode it advances only research-only analysis and due research reviews, not primary-account candidate analysis, primary-account two-session confirmation, report-to-plan conversion, or simulated entries.
- These fixes never switch `observe` to `paper` or lower the observation acceptance threshold.

## Board data sources and observation gate

Industry rankings prefer Eastmoney/BK. On failure, the independent `csrc_tencent` fallback combines current BaoStock CSRC membership with dated Tencent batch quotes and computes equally weighted member returns over that same membership. These are derived statistics, not official industry index returns; the report and model input disclose this distinction. New rankings no longer consume the stale Sina industry directory.

Codes such as `csrc_C27` preserve source, membership and session identity through the report and opportunity pool. They must not be aliased to similarly named Eastmoney, SW or Sina boards. Only industries with complete, valid, same-session member quotes qualify; missing members are not silently dropped to improve an average. Existing minimum-five-member, persistence, liquidity, observation and trading gates remain unchanged.

Before requesting industry members, the opportunity pool validates the ranking source/code pair. Missing or conflicting identities remain `source_identity_unavailable`; no name-only cross-provider membership lookup is allowed. This also covers later fallback providers returning names without identities: their rankings may appear in descriptive market reports but cannot drive stock selection. Concept themes retain their separate existing rules.

Classification runs in an isolated process with a single page capped at 10,000 rows (a full page is rejected), required-field and cursor-completeness checks, and a 60-second limit. The full snapshot has a 120-second budget. Real full classification queries took approximately 40–52 seconds, so a cold first fetch is slower than a cache hit. Process-local shared caching and single-flight construction prevent duplicate pulls within a process; failures have a 30-second cooldown and stale prior-date successes are not reused. Intraday freshness is still checked on cache reads.

Quote batches reuse one HTTP session. Connection errors and read timeouts allow at most one retry within the same total budget. Explicit rejections such as 403/429 are not retried; exhausted retries never cache a partially successful snapshot.

Legacy `hangye_*` / `gn_*` reports retain their original adapters; known `BK...` identities are not remapped by name. Legacy Sina industry members require independent CSRC validation, including native classification-code prefix parsing. Conflicting, missing, future or over-31-day-old classifications cannot pass selection. `akshare_sina` remains the historical ledger identifier, not a claim that current transport uses AKShare.

Derived industries accumulate genuine same-source observations rather than borrowing same-name Eastmoney history or fabricating earlier dates. A restored source may still await the existing persistence window; this differs from an upstream failure. Model analysis and simulated plans remain behind the existing observation gate: restoring data does not automatically create buy plans.

Theme persistence is not proven by LLM text such as "active for N days". Selection prefers same-source board-history bars; Sina constituents must not use same-name Eastmoney history. With current-day membership only, the system uses its same-source ledger: the current trading date plus the previous four real trading days that have healthy same-source snapshots. At least three positive days in a complete five-day window can confirm persistence; a complete window with fewer positive days is a mature negative/unconfirmed window, not cold start. Five- and twenty-day returns are returned only when the window is complete and every daily return is finite. Missing dates, changed board codes, source mismatches, or old name-only records keep the window incomplete. An operational fallback can therefore still yield zero candidates while history accumulates; failed runs do not count as valid observations.

## Frozen history and prospective evaluation

Historical snapshots are audit evidence, not a recomputable dataset. Frozen old samples can support same-sample old/new comparisons such as selected counts, rejection reasons, report/plan conversion, and later 10/20/40-session price behavior for the same candidate as-of dates. Missing sessions must not be backfilled from current APIs, and legacy records must not be upgraded to healthy `csrc_tencent` snapshots just because names match. Comparisons must state which samples have complete same-source evidence and which remain historical ledger rows only.

Prospective evaluation distinguishes two result classes. Candidates with paper plans and execution ledgers show real plan/execution outcomes. Candidates without confirmed executions can only be estimated from post-as-of prices and are labelled proxy. Proxy results are useful for seeing whether screening is too tight or missed obvious themes, but they are not realized trading returns and must not be used to claim validated performance.

Prospective evaluation reads only persisted `StockDaily` rows. When an observation candidate does not enter LLM analysis, the daily bars returned during discovery are not automatically available for later windows. Strategy v3 therefore adds a separate history-refresh loop: after the real market close, it scans recent-120-calendar-day `selected`, `observing`, and `score_components.strategy_comparison.legacy_selected` candidates, deduplicates by symbol, starts from the earliest candidate `as_of`, follows up to 40 real trading sessions after that signal date, and persists missing daily bars plus the CSI 300 benchmark as `000300.SH`. The refresh only stores market data. It does not create reports or fabricate executions; per-symbol failures are reported in the summary while other symbols continue. Stale dates, future dates, intraday sessions, and non-trading days do not fetch. Backfilled bars keep their `data_source`, but their price basis remains the provider's basis; same-sample comparisons must disclose the limitation if a provider later changes adjustment or historical-price policy.

## Rollback

Set:

```http
PUT /api/v1/market-opportunities/mode
{"mode":"off","expected_version": <mode_version from status>}
```

The next opportunity task will stop new discovery without a service restart. Do not delete opportunity history tables or edit the runtime database manually; they are evidence for observe rollout and prospective evaluation. To restore the bootstrap default, call `DELETE /api/v1/market-opportunities/mode` with the current `mode_version`; the audited endpoint clears the override before `.env` takes over again.

To stop only red-light research, set `MARKET_OPPORTUNITY_RESEARCH_DAILY_LLM_BUDGET=0` or switch the mode back to `observe`/`off`; no code rollback is needed, and existing research reports, the research budget ledger, and review records stay for audit. If you must roll back to a version that does not recognize research jobs, apply those settings first, and after the rollback do not call `POST /runs/{id}/analyze` manually on red-light or research-review runs: the older version does not recognize research job keys and may process an unfinished research review job as an ordinary candidate job. For the rollback limits of the report execution-purpose table, see the [paper trading guide](paper-trading_EN.md).

## Replay and data completeness

Board persistence is a soft score and a cold-start label, not an additional five-day analysis lock. Candidates with complete stock/benchmark/liquidity evidence may qualify for analysis; real model calls still require the global three healthy-session gate.

An explicit historical report replay may reuse its frozen membership only when market, date, source and board code match. Such runs carry `frozen_membership_replay` and earn no full-market observation-day credit. Price history is cut at the signal date; old and new decisions share the observed constituent sample.

CSI 300 uses an explicit index-history route preserving `sh000300`. Stock screening requires finite turnover in the latest 20 rows and falls through to another provider when that field is absent. Turnover is never estimated as volume times price. Missing JSON metrics are null.

Forward windows use exact exchange sessions; missing sessions are not replaced by later bars. The next-open proxy cannot report an executable same-day exit under A-share T+1. Proxy benchmark returns use the same opening basis and calendar dates. Paper net PnL is not charged fees twice, and repeated same-symbol/same-day runs are deduplicated to the latest run.

A nonempty, current, finite short price history remains ineligible for selection but is recorded as a normal history-length exclusion, so newly listed stocks do not alone invalidate an otherwise healthy observation day. Missing, stale or non-finite history remains a data failure.

Candidate pages order newer runs first, then ranked selected/observing rows before unranked rejections. The observation count comes from `provenance.observation_candidate_count`, and the analysis count from top-level `selected_candidate_count`; top-level `candidate_count` includes rejection audit rows and is not the observation-pool size.

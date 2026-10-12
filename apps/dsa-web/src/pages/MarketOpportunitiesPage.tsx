import type React from 'react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { getParsedApiError } from '../api/error';
import { marketOpportunitiesApi } from '../api/marketOpportunities';
import { systemConfigApi, SystemConfigConflictError } from '../api/systemConfig';
import type {
  MarketOpportunityCandidate,
  MarketOpportunityCandidateMetric,
  MarketOpportunityEvaluationResponse,
  MarketOpportunityModelState,
  MarketOpportunityRun,
  MarketOpportunityRunDetail,
  MarketOpportunityStatusResponse,
} from '../types/marketOpportunities';

const STATUS_TONE: Record<string, string> = {
  observing: 'border-sky-200 bg-sky-50 text-sky-700',
  selected: 'border-emerald-200 bg-emerald-50 text-emerald-700',
  completed: 'border-emerald-200 bg-emerald-50 text-emerald-700',
  healthy_empty: 'border-emerald-200 bg-emerald-50 text-emerald-700',
  observing_health: 'border-sky-200 bg-sky-50 text-sky-700',
  warming_up: 'border-sky-200 bg-sky-50 text-sky-700',
  risk_blocked: 'border-amber-200 bg-amber-50 text-amber-700',
  historical_replay: 'border-amber-200 bg-amber-50 text-amber-700',
  pending: 'border-amber-200 bg-amber-50 text-amber-700',
  failed: 'border-rose-200 bg-rose-50 text-rose-700',
  blocked: 'border-rose-200 bg-rose-50 text-rose-700',
  missing: 'border-slate-200 bg-slate-50 text-slate-600',
};

const RUN_PAGE_SIZE = 20;
const CANDIDATE_PAGE_SIZE = 20;
const MODE_OPTIONS = [
  { value: 'off', label: 'off · 关闭', description: '不发现候选，不分析，不生成模拟计划。' },
  { value: 'observe', label: 'observe · 观察', description: '只发现并冻结候选，不调用 LLM，不生成模拟计划。' },
  { value: 'paper', label: 'paper · 模拟', description: '观察门槛完成后，才允许候选进入模型分析和模拟计划链路。' },
] as const;

const STYLE_CONFIG_KEY = 'MARKET_OPPORTUNITY_STYLE';
const STYLE_OPTIONS = [
  { value: '', label: '继承 · 跟随模拟交易', description: '留空：机会池沿用 PAPER_TRADING_STYLE，与模拟交易主账户一致。' },
  { value: 'short_swing', label: '短线 · short_swing', description: '短线波段：历史门槛放宽到 80 根、趋势打分改用 MA5/10/20，弱市里更易选出短线标的。' },
  { value: 'position', label: '中线 · position', description: '中线持仓：历史门槛 180 根、趋势打分 MA10/20/60，默认的持仓周期口径。' },
  { value: 'trend', label: '长线 · trend', description: '趋势持有：历史门槛 500 根、趋势打分 MA20/60/120/250，偏长线。' },
] as const;
const STYLE_VALUES: readonly string[] = STYLE_OPTIONS.map((option) => option.value);

const RUN_TIME_CONFIG_KEY = 'MARKET_OPPORTUNITY_RUN_TIME';
// 与后端 config_registry 的校验一致：HH:MM，非法值按未配置处理。
const RUN_TIME_PATTERN = /^(?:[01]\d|2[0-3]):[0-5]\d$/;

const MODE_LABELS: Record<string, string> = {
  off: '关闭',
  observe: '观察模式',
  paper: '模拟模式',
};

const RUN_STATUS_LABELS: Record<string, string> = {
  discovering: '发现中',
  ready: '已完成发现',
  analyzing: '分析中',
  completed: '已完成',
  observing: '观察中',
  blocked: '已阻断',
  failed: '失败',
  pending: '等待中',
};

const REGIME_LABELS: Record<string, string> = {
  normal: '正常',
  cautious: '谨慎',
  risk_off: '风险偏高',
  unavailable: '状态不可用',
};

const ACTION_LABELS: Record<string, string> = {
  buy: '买入',
  add: '加仓',
  hold: '持有',
  reduce: '减仓',
  sell: '卖出',
  watch: '观望',
  avoid: '回避',
  alert: '预警',
};

const BULLISH_ACTIONS = new Set(['buy', 'add']);
const ACTIVE_JOB_STATUSES = new Set(['pending', 'claimed', 'submitted']);

const RESEARCH_OUTCOME_LABELS: Record<string, string> = {
  in_progress: '分析中',
  bullish: '看多',
  non_bullish: '不看多',
  analysis_failed: '分析失败',
  budget_exhausted: '研究预算已用尽',
  data_unavailable: '数据不可用',
  expired: '已过期',
};

// 未入选候选的折叠摘要按这几类聚合；其余原因回落到通用原因翻译。
const REST_REASON_GROUPS: Record<string, string> = {
  insufficient_daily_bars: '日线不足',
  daily_data_unavailable: '日线不足',
  low_20d_amount: '成交额偏低',
  st_or_delisting: 'ST',
};

const REASON_LABELS: Record<string, string> = {
  awaiting_entry_confirmation: '等待下一交易日确认，当前不会买入',
  entry_evidence_invalid: '买入证据不完整，已冻结入场',
  market_themes_missing: '大盘报告没有可用主题',
  theme_constituents_unavailable: '板块成分数据源不可用，可在后续任务重试',
  analysis_pending: '分析任务处理中',
  zero_candidates: '健康运行但未产生候选，可计入工程观察天数',
  no_selected_candidates: '健康运行但没有达到可分析资格的候选',
  risk_off: '风险市：继续观察，不允许新增模拟入场',
  'risk budget blocked': '风险预算阻断',
  historical_collection_not_supported: '历史采集暂不支持',
  market_regime_risk_off: '大盘风险状态偏高，仅保留观察',
  observation_health_unverified: '观察健康状态未验证',
  frozen_membership_no_observation_credit: '历史冻结成分回放，不计入观察天数',
  theme_history_warming_up: '题材历史样本累计中',
  insufficient_daily_bars: '日线样本不足',
  low_20d_amount: '20日成交额偏低',
  below_analysis_threshold: '低于分析阈值',
  entry_setup_unconfirmed: '入场形态未确认',
  insufficient_future_bars: '未来样本不足，等待窗口到期',
  missing_entry_price: '缺少入场价格',
  job_not_created: '未创建分析任务',
  analysis_jobs_missing: '未创建分析任务',
  risk_blocked: '风险市阻断模拟执行，只保留观察证据',
  source_unavailable: '数据源不可用',
  historical_replay: '历史回放',
  candidate_observing: '观察中：暂不生成模拟计划',
  analysis_candidate_budget: '达到基础条件，等待本批次分析名额',
  non_finite_metrics: '关键行情指标缺失或不可计算',
  history_not_ready: '历史样本尚未准备好',
  persistence: '持续性证据',
  amount: '成交额',
  ok: '正常',
  missing: '缺失',
  failed: '失败',
  completed: '已完成',
  observing: '观察中',
  selected: '达到分析条件',
  rejected: '已拒绝',
  warming_up: '观察累计中',
  observing_health: '观察健康',
  healthy_empty: '健康运行但零候选',
  excluded: '已排除',
  pending: '等待中',
  claimed: '已领取',
  submitted: '已提交',
  spawned: '已派生',
  non_executable: '不可执行',
  no_analysis_job: '未创建分析任务',
  completed_without_paper_plan: '已完成分析但未生成模拟计划',
  cannot_confirm_without_limit_price_or_minute_data: '缺少涨跌停价格或分钟线，无法确认',
  insufficient_data: '样本未到期',
  daily_open_proxy_unconfirmed: '日线开盘代理，未确认真实成交',
  next_session_open_daily_proxy_unconfirmed: '下一交易日开盘代理，未确认真实成交',
  signal_close_to_future_close_non_executable: '信号收盘到未来收盘，仅作观察，不代表可执行收益',
  healthy_observation_days: '健康观察日累计',
  healthy_board_observation_days: '健康完整板块快照累计',
  discovery_candidates: '发现候选',
  persisted_candidates: '已持久化候选',
  partial: '部分完整',
  complete: '完整',
  daily_data_unavailable: '当日日线缺失',
  st_or_delisting: 'ST 或退市风险',
  research_only: '仅研究，不生成模拟计划',
  execution_not_authorized: '未获执行授权',
  research_budget_exhausted: '当日研究预算已用尽',
  research_budget_zero: '研究预算为 0',
  research_budget_insufficient_for_configured_models: '研究预算不足以覆盖全部配置模型',
  llm_budget_zero: '本批 LLM 预算为 0',
  llm_budget_insufficient_for_configured_models: '本批 LLM 预算不足以覆盖全部配置模型',
  sync_analysis_exception: '分析调用异常',
  analysis_report_missing_after_sync_call: '分析完成但未找到报告',
  job_submission_claim_lost: '任务提交时领取权丢失',
  submitted_job_missing_query_id: '已提交任务缺少查询 ID',
  submitted_job_session_expired: '已提交任务会话过期',
  submitted_job_recovery_deadline_expired: '已提交任务超过恢复期限',
  submitted_job_session_expired_after_report: '报告生成后任务会话过期',
  candidate_missing: '候选记录缺失',
  market_regime_unavailable: '大盘状态不可用，暂停新分析',
  observe_gate_incomplete: '观察验收未完成',
  configured_models_missing: '未配置分析模型',
  no_theme_payload: '大盘报告没有主题数据',
  has_candidates: '有候选',
  no_executable_plan: '没有可执行计划',
  analysis_conditions_pending: '分析条件未满足',
  observation_data_incomplete: '观察数据不完整',
  no_themes: '没有主题',
  failed_retryable: '失败，可重试',
  research_review_budget_unavailable: '复核名额不足',
  followup_not_completed_by_expiry: '到期前未完成确认',
  research_review_not_completed_by_expiry: '到期前未完成复核',
  bullish_research_outcome: '研究结论看多',
  not_reviewed: '未复核',
  not_scheduled: '未排队',
  candidate_rejected: '未入选',
  candidate_not_selected: '未入选',
  scheduled: '已排队',
  expired: '已过期',
  bullish: '看多',
  non_bullish: '不看多',
  confirmed: '已确认',
  frozen: '已冻结',
  active: '生效中',
  superseded: '已被替代',
  triggered: '已触发',
  closed: '已关闭',
  analyzing: '分析中',
  ready: '已完成发现',
  blocked: '已阻断',
  discovering: '发现中',
  normal: '正常',
  cautious: '谨慎',
};

function translateReasonPart(part: string): string | null {
  if (REASON_LABELS[part]) {
    return REASON_LABELS[part];
  }
  const index = part.indexOf(':');
  if (index > 0) {
    const key = part.slice(0, index).trim();
    const detail = part.slice(index + 1).trim();
    const keyLabel = REASON_LABELS[key];
    if (keyLabel) {
      if (!detail) {
        return keyLabel;
      }
      const detailLabel = REASON_LABELS[detail] ?? detail.replace(/:/g, ' · ');
      return `${keyLabel}（${detailLabel}）`;
    }
  }
  return null;
}

// 组合原因（`candidate_rejected: a, b`、`sync_analysis_exception:Type:msg`）要逐段翻译，不能整串回显英文。
function reasonLabel(reason?: string | null): string {
  if (!reason) {
    return '--';
  }
  const raw = String(reason);
  if (REASON_LABELS[raw]) {
    return REASON_LABELS[raw];
  }
  const normalized = raw.replace(/^candidate_(rejected|observing):?\s*/, '').trim();
  if (!normalized) {
    return REASON_LABELS[raw.replace(/:$/, '')] ?? raw;
  }
  if (REASON_LABELS[normalized]) {
    return REASON_LABELS[normalized];
  }
  const parts = normalized.split(/[,，]/).map((part) => part.trim()).filter(Boolean);
  if (!parts.length) {
    return raw;
  }
  const translated = parts.map(translateReasonPart);
  if (translated.every((label) => label === null)) {
    return raw;
  }
  return translated.map((label, index) => label ?? parts[index]).join('、');
}

function statusClass(status?: string | null): string {
  return STATUS_TONE[String(status || '').toLowerCase()] ?? 'border-slate-200 bg-slate-50 text-slate-600';
}

function formatCount(value: unknown): string {
  if (value === null || value === undefined || value === '') {
    return '--';
  }
  return String(value);
}

// 后端时间戳是 UTC 无时区的 naive 字符串，缺少后缀时补 Z 再按中国时区渲染。
function parseServerTime(value?: string | null): Date | null {
  if (!value) {
    return null;
  }
  const source = value.includes('T') && !/[zZ]|[+-]\d{2}:?\d{2}$/.test(value) ? `${value}Z` : value;
  const date = new Date(source);
  return Number.isNaN(date.getTime()) ? null : date;
}

function formatChinaTime(value?: string | null): string {
  if (!value) {
    return '--';
  }
  const date = parseServerTime(value);
  if (!date) {
    return value;
  }
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date).replace(/\//g, '-');
}

function formatChinaClock(value?: string | null): string | null {
  const date = parseServerTime(value);
  if (!date) {
    return null;
  }
  return new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    hour: '2-digit',
    minute: '2-digit',
    hourCycle: 'h23',
  }).format(date);
}

function formatShortDate(value?: string | null): string {
  const matched = String(value ?? '').match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (matched) {
    return `${matched[2]}-${matched[3]}`;
  }
  return value ? String(value) : '--';
}

function formatScore(value?: number | null): string {
  return typeof value === 'number' && Number.isFinite(value) ? value.toFixed(1) : '--';
}

function stripThemePrefix(value?: string | null): string {
  const raw = String(value ?? '').trim();
  if (!raw) {
    return '--';
  }
  return raw.replace(/^[a-z_]+:/i, '') || '--';
}

function shortModelName(model: string): string {
  const parts = String(model || '').split('/');
  return parts[parts.length - 1] || model;
}

function modeLabel(mode?: string | null): string {
  const key = String(mode ?? '').toLowerCase();
  return MODE_LABELS[key] ?? (mode ? String(mode) : '--');
}

function runStatusLabel(status?: string | null): string {
  const key = String(status ?? '').toLowerCase();
  if (!key) {
    return '--';
  }
  return RUN_STATUS_LABELS[key] ?? reasonLabel(key);
}

function getWindowMetric(evaluation: MarketOpportunityEvaluationResponse | null, window: number, key: string): string {
  const windows = evaluation?.summary?.windows;
  if (!windows || typeof windows !== 'object') {
    return '--';
  }
  const metric = (windows as Record<string, Record<string, unknown>>)[String(window)];
  const value = metric?.[key];
  return value === null || value === undefined || value === '' ? '--' : String(value);
}

function firstNumber(payload: Record<string, unknown> | undefined, keys: string[]): number | null {
  if (!payload) {
    return null;
  }
  for (const key of keys) {
    const value = payload[key];
    if (typeof value === 'number' && Number.isFinite(value)) {
      return value;
    }
  }
  return null;
}

function firstStringArray(payload: Record<string, unknown> | undefined, keys: string[]): string[] {
  if (!payload) {
    return [];
  }
  for (const key of keys) {
    const value = payload[key];
    if (Array.isArray(value)) {
      return value.map(String).filter(Boolean);
    }
  }
  return [];
}

function stableStringify(value: unknown): string {
  if (!value || typeof value !== 'object') {
    return '--';
  }
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function toRecord(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : undefined;
}

function fieldValue(payload: Record<string, unknown> | undefined | null, camelKey: string, snakeKey: string = camelKey): unknown {
  return payload?.[camelKey] ?? payload?.[snakeKey];
}

function formatMetricValue(value: unknown): string {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return Number.isInteger(value) ? String(value) : String(Number(value.toFixed(4)));
  }
  return formatCount(value);
}

function displayValue(value: unknown): string {
  if (typeof value === 'string') {
    return reasonLabel(value);
  }
  if (typeof value === 'boolean') {
    return value ? '是' : '否';
  }
  return formatCount(value);
}

function toDisplayList(value: unknown): string[] {
  if (!Array.isArray(value)) {
    return [];
  }
  return value.map((item) => {
    if (item && typeof item === 'object') {
      const row = item as Record<string, unknown>;
      return [
        row.date,
        typeof row.field === 'string' ? reasonLabel(row.field) : row.field,
        row.source,
        typeof row.reason === 'string' ? reasonLabel(row.reason) : row.reason,
      ].filter(Boolean).join(' · ') || stableStringify(row);
    }
    return typeof item === 'string' ? reasonLabel(item) : String(item);
  }).filter(Boolean);
}

// 「阶段」列：阶段词 + 原始状态的翻译，避免英文状态码直接暴露给用户。
function stageLabel(candidate: MarketOpportunityCandidate): string {
  const status = String(candidate.status || '').toLowerCase();
  if (status === 'observing') {
    return '观察池 · 观察中';
  }
  if (status === 'selected') {
    return '可分析 · 达到分析条件';
  }
  if (status === 'rejected') {
    return '已拒绝 · 已拒绝';
  }
  return status ? `${status} · ${reasonLabel(status)}` : '未入池';
}

function stageDescription(candidate: MarketOpportunityCandidate, mode?: string | null, healthStatus?: string | null): string {
  const status = String(candidate.status || '').toLowerCase();
  const reasons = [
    candidate.noPlanReason,
    ...(candidate.rejectionReasons ?? []).filter((reason): reason is string => typeof reason === 'string'),
  ];
  if (String(healthStatus || '').toLowerCase() === 'risk_blocked') {
    return '风险市只允许保留观察证据，页面不把候选显示为可买。';
  }
  if (status === 'observing') {
    if (reasons.includes('analysis_candidate_budget')) {
      return '已达到基础条件，正在等待本批次分析名额；尚未进入模型分析。';
    }
    return '已进入观察池，但尚未达到模型分析资格。';
  }
  if (status === 'selected') {
    return mode === 'paper'
      ? '达到分析资格，仍需通过模式、健康门槛、风控和模拟计划约束。'
      : '达到分析资格；当前模式不会提交真实买入。';
  }
  return '未进入分析链路。';
}

function setupTypeLabel(value?: unknown): string {
  const labels: Record<string, string> = {
    trend_continuation: '趋势延续',
    pullback: '缩量回踩',
    unconfirmed: '未确认',
  };
  const key = String(value || '');
  return labels[key] ?? (key || '--');
}

function evidenceStageLabel(value?: unknown): string {
  const labels: Record<string, string> = {
    cold_start: '冷启动',
    established: '已建立',
  };
  const key = String(value || '');
  return labels[key] ?? (key || '--');
}

function formatStrategyComparison(payload?: Record<string, unknown> | null): string {
  if (!payload) {
    return '--';
  }
  const legacySelected = payload.legacySelected ?? payload.legacy_selected;
  const currentSelected = payload.currentSelected ?? payload.current_selected;
  const legacyScore = payload.legacyScore ?? payload.legacy_score;
  const policyVersion = payload.policyVersion ?? payload.policy_version;
  const legacy = legacySelected === true ? '旧策略会选' : legacySelected === false ? '旧策略不选' : '旧策略--';
  const current = currentSelected === true ? '新策略会选' : currentSelected === false ? '新策略不选' : '新策略--';
  const score = legacyScore == null ? '旧分--' : `旧分 ${legacyScore}`;
  const policy = policyVersion ? `策略 ${policyVersion}` : '';
  return [legacy, current, score, policy].filter(Boolean).join(' · ');
}

function isWindowMature(evaluation: MarketOpportunityEvaluationResponse | null, window: number): boolean {
  const windows = evaluation?.summary?.windows;
  const metric = windows && typeof windows === 'object'
    ? (windows as Record<string, Record<string, unknown>>)[String(window)]
    : undefined;
  const evaluated = metric?.evaluated;
  return typeof evaluated === 'number' && evaluated > 0;
}

function candidateMetricFor(
  evaluation: MarketOpportunityEvaluationResponse | null,
  candidate: MarketOpportunityCandidate,
): MarketOpportunityCandidateMetric | null {
  const metrics = evaluation?.candidateMetrics ?? [];
  return metrics.find((metric) => (
    metric.candidateId === candidate.id
    || metric.candidate_id === candidate.id
    || metric.symbol === candidate.symbol
    || metric.stockCode === candidate.stockCode
    || metric.stock_code === candidate.stockCode
  )) ?? null;
}

function metricWindowValue(metric: MarketOpportunityCandidateMetric | null, field: 'windows' | 'executableWindows' | 'executable_windows', window: number): Record<string, unknown> | undefined {
  const payload = metric as Record<string, unknown> | null;
  const windows = fieldValue(payload, field === 'executable_windows' ? 'executableWindows' : field, field === 'executableWindows' ? 'executable_windows' : field);
  return toRecord(windows)?.[String(window)] as Record<string, unknown> | undefined;
}

function paperExecution(metric: MarketOpportunityCandidateMetric | null): Record<string, unknown> | undefined {
  return toRecord(fieldValue(metric as Record<string, unknown> | null, 'paperExecution', 'paper_execution'));
}

function executableEntry(metric: MarketOpportunityCandidateMetric | null): Record<string, unknown> | undefined {
  return toRecord(fieldValue(metric as Record<string, unknown> | null, 'executableEntry', 'executable_entry'));
}

function planStateLabel(state: MarketOpportunityModelState): string {
  if (state.planExecutable === true) {
    return '计划已取得执行权';
  }
  if (state.paperPlanId) {
    return '计划已生成，但不可执行';
  }
  if (ACTIVE_JOB_STATUSES.has(String(state.status))) {
    return '分析处理中，尚未生成计划';
  }
  return '未生成计划';
}

// 只要某个模型真的创建过分析任务，该候选就算“送过 LLM 分析”。
function hasAnalysisJob(state: MarketOpportunityModelState): boolean {
  return state.jobId != null || (Boolean(state.status) && String(state.status) !== 'missing');
}

function isResearchState(state: MarketOpportunityModelState): boolean {
  return state.executionIntent === 'research_only' || Boolean(state.research);
}

function isBullishState(state: MarketOpportunityModelState): boolean {
  return BULLISH_ACTIONS.has(String(state.action ?? '')) || state.research?.outcome === 'bullish';
}

function isActiveJob(state: MarketOpportunityModelState): boolean {
  return ACTIVE_JOB_STATUSES.has(String(state.status));
}

function isReviewPending(state: MarketOpportunityModelState): boolean {
  const review = state.research?.review;
  return Boolean(review) && ['scheduled', 'spawned'].includes(String(review?.status ?? ''));
}

// 待确认 = 已有模拟计划但交易日确认未走完，且复核链路还在排队/已启动。
// 计划已执行、已关闭、已替代，或确认已完成时都不算待确认。
function pendingConfirmation(state: MarketOpportunityModelState): { current: number | null; required: number | null } | null {
  if (!state.paperPlanId || state.planExecutable === true) {
    return null;
  }
  if (['closed', 'superseded'].includes(String(state.planStatus ?? ''))) {
    return null;
  }
  if (state.followup && !['scheduled', 'spawned'].includes(String(state.followup.status ?? ''))) {
    return null;
  }
  const progress = state.confirmationProgress;
  const current = progress?.current ?? state.followup?.confirmationCurrent ?? null;
  const required = progress?.required ?? state.followup?.confirmationRequired ?? null;
  if (progress?.completed === true) {
    return null;
  }
  if (required == null) {
    return null;
  }
  if (current != null && current >= required) {
    return null;
  }
  return { current, required };
}

function obligationStatusText(obligation: { status?: string | null; result?: string | null; reason?: string | null; spawnedRunId?: number | null } | null | undefined): string {
  if (!obligation) {
    return '等待确认任务';
  }
  const status = String(obligation.status ?? '');
  const result = obligation.result ? reasonLabel(String(obligation.result)) : null;
  const reason = obligation.reason ? reasonLabel(String(obligation.reason)) : null;
  if (status === 'scheduled') {
    return '已排队';
  }
  if (status === 'spawned') {
    return obligation.spawnedRunId ? `已启动（运行 #${obligation.spawnedRunId}）` : '已启动';
  }
  if (status === 'completed') {
    return result ? `已完成（${result}）` : '已完成';
  }
  if (status === 'failed') {
    const head = result ?? '未完成';
    return reason ? `${head}（${reason}）` : head;
  }
  if (status === 'expired') {
    return result && String(obligation.result) !== 'expired' ? `已过期（${result}）` : '已过期';
  }
  if (status === 'skipped') {
    return reason ? `未排队（${reason}）` : '未排队';
  }
  return status ? displayValue(status) : '等待确认任务';
}

// 结果列的一句话结论：研究 / 计划 / 分析中 / 未分析。
function stateResult(state: MarketOpportunityModelState, riskOff: boolean): string {
  const outcome = String(state.research?.outcome ?? '');
  if (isResearchState(state)) {
    if (outcome === 'in_progress' || (!outcome && isActiveJob(state))) {
      return '仅研究 · 分析中';
    }
    if (outcome === 'bullish' || outcome === 'non_bullish') {
      if (state.research?.kind === 'review') {
        return '仅研究（复核）';
      }
      return outcome === 'bullish' && riskOff ? '仅研究（风险市）' : '仅研究';
    }
    const detail = state.failureReason
      ? reasonLabel(state.failureReason)
      : RESEARCH_OUTCOME_LABELS[outcome] ?? reasonLabel(outcome || state.status);
    return `仅研究 · ${detail}`;
  }
  if (state.planExecutable === true) {
    return `可执行 · 计划 #${state.paperPlanId ?? '--'}`;
  }
  const pending = pendingConfirmation(state);
  if (state.paperPlanId && pending) {
    return `已生成计划 #${state.paperPlanId} · 确认 ${pending.current ?? 0}/${pending.required ?? '--'}`;
  }
  if (state.paperPlanId) {
    return `已生成计划 #${state.paperPlanId}${state.planStatus ? ` · ${displayValue(state.planStatus)}` : ''}`;
  }
  if (isActiveJob(state)) {
    return '分析中';
  }
  if (!hasAnalysisJob(state)) {
    return reasonLabel(state.noPlanReason || 'job_not_created');
  }
  return reasonLabel(state.noPlanReason || state.failureReason || state.status);
}

function stateAdvice(state: MarketOpportunityModelState): string {
  const bullish = isBullishState(state);
  const label = state.actionLabel || ACTION_LABELS[String(state.action ?? '')] || String(state.action ?? '');
  if (label) {
    return bullish && !label.includes('看多') ? `${label}（看多）` : label;
  }
  if (state.research?.outcome === 'bullish') {
    return '看多';
  }
  if (state.research?.outcome === 'in_progress' || isActiveJob(state) || isResearchState(state)) {
    return state.research?.outcome === 'in_progress' ? '分析中' : '未分析';
  }
  if (String(state.status).startsWith('failed')) {
    return '分析失败';
  }
  if (!hasAnalysisJob(state)) {
    return '未分析';
  }
  return '—';
}

function stateNextStep(state: MarketOpportunityModelState): string {
  const review = state.research?.review;
  if (isReviewPending(state) && review?.dueDate) {
    return `${formatShortDate(review.dueDate)} 复核`;
  }
  const pending = pendingConfirmation(state);
  if (pending) {
    const due = state.followup && ['scheduled', 'spawned'].includes(String(state.followup.status ?? '')) && state.followup.dueDate
      ? `${formatShortDate(state.followup.dueDate)} `
      : '';
    return `${due}待确认 ${pending.current ?? 0}/${pending.required ?? '--'}`;
  }
  return '—';
}

interface CoreRow {
  candidate: MarketOpportunityCandidate;
  states: MarketOpportunityModelState[];
  bullish: boolean;
}

function buildCoreRows(candidates: MarketOpportunityCandidate[], modelFilter: string): CoreRow[] {
  const rows: CoreRow[] = [];
  for (const candidate of candidates) {
    const all = candidate.modelStates ?? [];
    if (!all.some(hasAnalysisJob) && !(candidate.analysisJobs ?? []).length) {
      continue;
    }
    const states = (modelFilter ? all.filter((state) => state.model === modelFilter) : all.filter(hasAnalysisJob));
    if (!states.length) {
      continue;
    }
    rows.push({ candidate, states, bullish: states.some(isBullishState) });
  }
  return [...rows.filter((row) => row.bullish), ...rows.filter((row) => !row.bullish)];
}

function stateCellLines(states: MarketOpportunityModelState[], render: (state: MarketOpportunityModelState) => string): string[] {
  const multi = states.length > 1;
  return states.map((state) => (multi ? `${shortModelName(state.model)}：${render(state)}` : render(state)));
}

interface QueueItem {
  id: string;
  label: string;
  status: string;
  dueDate: string | null;
}

function buildFollowupQueue(rows: CoreRow[]): QueueItem[] {
  const items: QueueItem[] = [];
  for (const row of rows) {
    const name = row.candidate.stockName || row.candidate.symbol;
    const multi = row.states.length > 1;
    for (const state of row.states) {
      const suffix = multi ? ` · ${shortModelName(state.model)}` : '';
      const review = state.research?.review;
      if (review) {
        const due = review.dueDate ? `${formatShortDate(review.dueDate)} ` : '';
        items.push({
          id: `${row.candidate.id}-${state.model}-review`,
          label: `${name}${suffix} · ${due}复核（仅研究，不生成计划）`,
          status: obligationStatusText(review),
          dueDate: review.dueDate ?? null,
        });
        continue;
      }
      const pending = pendingConfirmation(state);
      if (!pending) {
        continue;
      }
      const due = state.followup && ['scheduled', 'spawned'].includes(String(state.followup.status ?? '')) && state.followup.dueDate
        ? `${formatShortDate(state.followup.dueDate)} `
        : '';
      const progress = pending.required == null ? '确认' : `确认 ${pending.current ?? 0}/${pending.required}`;
      items.push({
        id: `${row.candidate.id}-${state.model}-followup`,
        label: `${name}${suffix} · ${due}${progress}（模拟计划 #${state.paperPlanId}）`,
        status: obligationStatusText(state.followup),
        dueDate: state.followup?.dueDate ?? null,
      });
    }
  }
  return items.sort((left, right) => {
    if (!left.dueDate && !right.dueDate) {
      return 0;
    }
    if (!left.dueDate) {
      return 1;
    }
    if (!right.dueDate) {
      return -1;
    }
    return left.dueDate < right.dueDate ? -1 : left.dueDate > right.dueDate ? 1 : 0;
  });
}

interface RestGroup {
  label: string;
  count: number;
}

// 未入选候选按中文原因聚合，每个候选每类只计一次。
function buildRestGroups(candidates: MarketOpportunityCandidate[]): RestGroup[] {
  const counts = new Map<string, number>();
  for (const candidate of candidates) {
    const seen = new Set<string>();
    for (const reason of candidate.rejectionReasons ?? []) {
      if (typeof reason !== 'string') {
        continue;
      }
      const key = reason.replace(/^candidate_(rejected|observing):?\s*/, '').trim();
      if (!key) {
        continue;
      }
      const label = REST_REASON_GROUPS[key] ?? reasonLabel(key);
      if (label === '--' || seen.has(label)) {
        continue;
      }
      seen.add(label);
      counts.set(label, (counts.get(label) ?? 0) + 1);
    }
  }
  return [...counts.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((left, right) => right.count - left.count);
}

interface CandidateDetailProps {
  candidate: MarketOpportunityCandidate;
  evaluation: MarketOpportunityEvaluationResponse | null;
  mode?: string | null;
  healthStatus?: string | null;
  riskBlocked: boolean;
  showBadge: boolean;
}

const CandidateDetail: React.FC<CandidateDetailProps> = ({
  candidate,
  evaluation,
  mode,
  healthStatus,
  riskBlocked,
  showBadge,
}) => {
  const metric = candidateMetricFor(evaluation, candidate);
  const entry = executableEntry(metric);
  const tradability = toRecord(fieldValue(entry, 'tradability'));
  const paper = paperExecution(metric);
  const signalWindow = metricWindowValue(metric, 'windows', 1);
  const executableWindow = metricWindowValue(metric, 'executableWindows', 1);
  const missingEvidence = toDisplayList(candidate.dataQuality?.missingEvidence ?? candidate.dataQuality?.missing_evidence);
  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <p className="text-xs text-secondary">
          {candidate.symbol} · {candidate.stockCode} · as-of {candidate.asOf ?? '--'} · valid until {candidate.validUntil ?? '--'}
        </p>
        {showBadge ? (
          <span className={`rounded-full border px-3 py-1 text-xs font-semibold ${statusClass(candidate.status)}`}>
            {stageLabel(candidate)}
          </span>
        ) : null}
      </div>
      <p className={`rounded-xl border px-3 py-2 text-xs ${riskBlocked ? 'border-amber-200 bg-amber-50 text-amber-700' : 'border-sky-200 bg-sky-50 text-sky-700'}`}>
        {stageDescription(candidate, mode, healthStatus)}
      </p>

      <dl className="grid grid-cols-2 gap-3 text-sm md:grid-cols-4">
        <div>
          <dt className="text-secondary">主题</dt>
          <dd className="font-medium text-primary">{stripThemePrefix(candidate.primaryThemeKey)}</dd>
        </div>
        <div>
          <dt className="text-secondary">排名</dt>
          <dd className="font-medium text-primary">{candidate.rank ?? '--'}</dd>
        </div>
        <div>
          <dt className="text-secondary">评分</dt>
          <dd className="font-medium text-primary">{candidate.score ?? '--'}</dd>
        </div>
        <div>
          <dt className="text-secondary">无计划原因</dt>
          <dd className="font-medium text-primary">{reasonLabel(candidate.noPlanReason)}</dd>
        </div>
        <div>
          <dt className="text-secondary">证据阶段</dt>
          <dd className="font-medium text-primary">{evidenceStageLabel(candidate.scoreComponents?.evidenceStage ?? candidate.scoreComponents?.evidence_stage)}</dd>
        </div>
        <div>
          <dt className="text-secondary">机会类型</dt>
          <dd className="font-medium text-primary">{setupTypeLabel(candidate.scoreComponents?.setupType ?? candidate.scoreComponents?.setup_type)}</dd>
        </div>
        <div className="md:col-span-2">
          <dt className="text-secondary">旧/新策略对照</dt>
          <dd className="font-medium text-primary">{formatStrategyComparison(toRecord(candidate.scoreComponents?.strategyComparison ?? candidate.scoreComponents?.strategy_comparison))}</dd>
        </div>
      </dl>

      {missingEvidence.length ? (
        <div className="rounded-xl border border-amber-200 bg-amber-50 p-3 text-xs text-amber-700">
          缺失证据：{missingEvidence.join(' / ')}
        </div>
      ) : null}
      {candidate.rejectionReasons?.length ? (
        <div className="rounded-xl border border-slate-200 bg-card p-3 text-xs text-secondary">
          拒绝原因：{candidate.rejectionReasons.map((reason) => typeof reason === 'string' ? reasonLabel(reason) : stableStringify(reason)).join(' / ')}
        </div>
      ) : null}

      <div className="rounded-xl border border-border bg-surface p-3 text-xs text-secondary">
        <p className="font-medium text-primary">执行评估证据</p>
        {metric ? (
          <div className="mt-2 grid gap-2 md:grid-cols-2">
            <p>收益口径 {displayValue(fieldValue(metric, 'returnBasis', 'return_basis'))}</p>
            <p>信号收盘 1日 {formatMetricValue(signalWindow?.returnPct ?? signalWindow?.return_pct)} · {displayValue(signalWindow?.status)}</p>
            <p>次日开盘代理 {displayValue(fieldValue(entry, 'status'))} · 入场 {formatCount(fieldValue(entry, 'entryDate', 'entry_date'))}</p>
            <p>T+1 最早卖出 {formatCount(fieldValue(entry, 'earliestSellDate', 'earliest_sell_date'))} · 价格 {formatMetricValue(fieldValue(entry, 'price'))}</p>
            <p>可成交确认 {displayValue(fieldValue(tradability, 'fillConfirmed', 'fill_confirmed'))}</p>
            <p>涨跌停可成交 {displayValue(fieldValue(tradability, 'limitUnfillableStatus', 'limit_unfillable_status'))}</p>
            <p>执行代理 1日 {formatMetricValue(executableWindow?.returnPct ?? executableWindow?.return_pct)} · {displayValue(executableWindow?.status)}</p>
            <p>执行口径 {displayValue(executableWindow?.returnBasis ?? executableWindow?.return_basis)}</p>
            <p>纸面仓位 {formatCount(fieldValue(paper, 'positions'))} · 成交事件 {formatCount(fieldValue(paper, 'tradeEvents', 'trade_events'))}</p>
            <p>费用 {formatMetricValue(fieldValue(paper, 'totalFees', 'total_fees'))} · 滑点 {formatMetricValue(fieldValue(paper, 'totalSlippageAmount', 'total_slippage_amount'))}</p>
            <p>佣金 {formatMetricValue(fieldValue(paper, 'totalCommission', 'total_commission'))} · 税费 {formatMetricValue(fieldValue(paper, 'totalTax', 'total_tax'))}</p>
            <p>净已实现盈亏 {formatMetricValue(fieldValue(paper, 'netRealizedPnl', 'net_realized_pnl'))}</p>
          </div>
        ) : (
          <p className="mt-2">暂无执行评估数据；等待评估接口返回候选指标。</p>
        )}
      </div>

      <div className="space-y-2">
        <p className="text-xs font-medium text-primary">各模型明细</p>
        {(candidate.modelStates ?? []).map((state) => (
          <div key={state.model} className="rounded-xl border border-border bg-surface p-3 text-sm">
            <div className="flex flex-wrap items-center justify-between gap-2">
              <span className="font-medium text-primary">{state.model}</span>
              <span className={`rounded-full border px-2 py-0.5 text-xs ${statusClass(state.status)}`}>
                {displayValue(state.status)}
              </span>
            </div>
            <p className="mt-2 text-xs text-secondary">
              报告 {state.analysisHistoryId ?? '--'} · 信号 {state.decisionSignalId ?? '--'} · 计划 {state.paperPlanId ?? '--'}
            </p>
            <p className="mt-1 text-xs text-secondary">
              {state.executionIntent ? `执行意图 ${reasonLabel(state.executionIntent)}` : null}
              {state.research?.kind ? `${state.executionIntent ? ' · ' : ''}研究类型 ${reasonLabel(state.research.kind)}` : null}
              {state.research?.outcome ? `${state.executionIntent || state.research?.kind ? ' · ' : ''}研究结论 ${RESEARCH_OUTCOME_LABELS[String(state.research.outcome)] ?? reasonLabel(String(state.research.outcome))}` : null}
            </p>
            <p className={`mt-1 text-xs font-medium ${state.planExecutable ? 'text-emerald-600' : 'text-amber-600'}`}>
              {planStateLabel(state)}
              {state.planStatus ? ` · ${displayValue(state.planStatus)}` : ''}
            </p>
            {state.confirmationProgress && (
              <p className="mt-1 text-xs text-secondary">
                交易日确认 {state.confirmationProgress.current ?? 0}/{state.confirmationProgress.required ?? '--'}
                {state.confirmationProgress.completed ? ' · 已完成' : ' · 未完成'}
              </p>
            )}
            {state.followup && (
              <p className="mt-1 text-xs text-sky-600">
                次交易日复核：{displayValue(state.followup.status)} · 到期 {state.followup.dueDate ?? '--'}
                {state.followup.spawnedRunId ? ` · run #${state.followup.spawnedRunId}` : ''}
              </p>
            )}
            {state.updatedAt && (
              <p className="mt-1 text-xs text-secondary">更新 {formatChinaTime(state.updatedAt)}（UTC+8）</p>
            )}
            {state.noPlanReason && (
              <p className="mt-1 text-xs text-amber-600">{reasonLabel(state.noPlanReason)}</p>
            )}
          </div>
        ))}
      </div>
    </div>
  );
};

const MarketOpportunitiesPage: React.FC = () => {
  const [status, setStatus] = useState<MarketOpportunityStatusResponse | null>(null);
  const [runs, setRuns] = useState<MarketOpportunityRun[]>([]);
  const [runTotal, setRunTotal] = useState(0);
  const [runPage, setRunPage] = useState(1);
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [runDetail, setRunDetail] = useState<MarketOpportunityRunDetail | null>(null);
  const [evaluation, setEvaluation] = useState<MarketOpportunityEvaluationResponse | null>(null);
  const [candidates, setCandidates] = useState<MarketOpportunityCandidate[]>([]);
  const [candidateTotal, setCandidateTotal] = useState(0);
  const [candidatePage, setCandidatePage] = useState(1);
  const [modelFilter, setModelFilter] = useState<string>('');
  const [candidateStatusFilter, setCandidateStatusFilter] = useState<string>('');
  const [isLoading, setIsLoading] = useState(true);
  const [isActing, setIsActing] = useState(false);
  const [isSavingMode, setIsSavingMode] = useState(false);
  const [modeDraft, setModeDraft] = useState<'off' | 'observe' | 'paper'>('off');
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [styleDraft, setStyleDraft] = useState<string>('');
  const [savedStyle, setSavedStyle] = useState<string>('');
  const [styleConfigVersion, setStyleConfigVersion] = useState<string | null>(null);
  const [styleAvailable, setStyleAvailable] = useState(true);
  const [isSavingStyle, setIsSavingStyle] = useState(false);
  const [settingsOpen, setSettingsOpen] = useState(false);
  const [runTimeSetting, setRunTimeSetting] = useState<string | null | undefined>(undefined);
  const settingsRef = useRef<HTMLDetailsElement>(null);

  useEffect(() => {
    const normalized = String(status?.mode ?? 'off').toLowerCase();
    if (normalized === 'off' || normalized === 'observe' || normalized === 'paper') {
      setModeDraft(normalized);
    }
  }, [status?.mode]);

  useEffect(() => {
    let cancelled = false;
    systemConfigApi.getConfig(false)
      .then((payload) => {
        if (cancelled) {
          return;
        }
        const item = payload.items.find((entry) => entry.key === STYLE_CONFIG_KEY);
        const raw = String(item?.value ?? '').trim();
        const normalized = STYLE_VALUES.includes(raw) ? raw : '';
        setSavedStyle(normalized);
        setStyleDraft(normalized);
        setStyleConfigVersion(payload.configVersion);
        setStyleAvailable(true);
        const runTimeItem = payload.items.find((entry) => entry.key === RUN_TIME_CONFIG_KEY);
        setRunTimeSetting(String(runTimeItem?.value ?? '').trim());
      })
      .catch(() => {
        if (!cancelled) {
          // 风格开关是增强项；配置接口不可用时隐藏下拉，不阻断机会池主视图。
          setStyleAvailable(false);
          setRunTimeSetting(null);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    Promise.all([
      marketOpportunitiesApi.getStatus(),
      marketOpportunitiesApi.getRuns({ page: runPage, limit: RUN_PAGE_SIZE }),
    ])
      .then(([statusPayload, runsPayload]) => {
        if (cancelled) {
          return;
        }
        setStatus(statusPayload);
        setRuns(runsPayload.items);
        setRunTotal(runsPayload.total);
        setSelectedRunId((current) => (
          current && runsPayload.items.some((run) => run.id === current)
            ? current
            : runsPayload.items[0]?.id ?? null
        ));
        setError(null);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : '机会池数据加载失败');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, [runPage]);

  useEffect(() => {
    if (!selectedRunId) {
      setRunDetail(null);
      setEvaluation(null);
      setCandidates([]);
      setCandidateTotal(0);
      return;
    }
    let cancelled = false;
    Promise.all([
      marketOpportunitiesApi.getRun(selectedRunId),
      marketOpportunitiesApi.getEvaluation({ runId: selectedRunId, windows: [1, 3, 5, 10, 20, 40] }),
      marketOpportunitiesApi.getCandidates({
        runId: selectedRunId,
        status: candidateStatusFilter || undefined,
        model: modelFilter || undefined,
        page: candidatePage,
        limit: CANDIDATE_PAGE_SIZE,
      }),
    ])
      .then(([detailPayload, evaluationPayload, candidatesPayload]) => {
        if (cancelled) {
          return;
        }
        setRunDetail(detailPayload);
        setEvaluation(evaluationPayload);
        setCandidates(candidatesPayload.items);
        setCandidateTotal(candidatesPayload.total);
        setError(null);
      })
      .catch((err: unknown) => {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : '机会池详情加载失败');
        }
      });
    return () => {
      cancelled = true;
    };
  }, [selectedRunId, candidateStatusFilter, modelFilter, candidatePage]);

  const configuredModels = useMemo(() => {
    const fromDetail = runDetail?.configuredModels ?? [];
    const fromStatus = status?.configuredModels ?? [];
    return Array.from(new Set([...fromDetail, ...fromStatus].filter(Boolean)));
  }, [runDetail?.configuredModels, status?.configuredModels]);

  const observeGate = status?.observeGate as Record<string, unknown> | undefined;
  const observeRequiredDays = firstNumber(observeGate, ['requiredTradingDays', 'required_trading_days', 'requiredDays', 'required_days']);
  const observeDateKeys = ['dates', 'observedTradingDates', 'observed_trading_dates', 'recordedDates', 'recorded_dates', 'windowDates', 'window_dates', 'persistenceWindowDates', 'persistence_window_dates'];
  const observeDates = firstStringArray(observeGate, observeDateKeys);
  const observeDatesExposed = observeDateKeys.some((key) => Array.isArray(observeGate?.[key]));
  const observePositiveDates = firstStringArray(observeGate, ['positiveTradingDates', 'positive_trading_dates', 'positiveDates', 'positive_dates', 'persistencePositiveDates', 'persistence_positive_dates']);
  const observeCompletedDays = firstNumber(observeGate, ['healthyRunDays', 'healthy_run_days', 'completedTradingDays', 'completed_trading_days', 'observedTradingDays', 'observed_trading_days', 'recordedTradingDays', 'recorded_trading_days', 'positiveTradingDays', 'positive_trading_days'])
    ?? (observePositiveDates.length || observeDates.length || (observeGate?.completed === true && observeRequiredDays ? observeRequiredDays : null));
  const observeTotalRunDays = firstNumber(observeGate, ['totalRunDays', 'total_run_days']);
  const observeCountingBasis = String(observeGate?.countingBasis ?? observeGate?.counting_basis ?? 'healthy_observation_days');
  const observeExcludedDates = Array.isArray(observeGate?.excludedDates)
    ? observeGate.excludedDates
    : Array.isArray(observeGate?.excluded_dates)
      ? observeGate.excluded_dates
      : [];
  const observeProgressLabel = observeRequiredDays == null
    ? formatCount(observeCompletedDays)
    : `${formatCount(observeCompletedDays)}/${formatCount(observeRequiredDays)}`;
  const paperGateIncomplete = modeDraft === 'paper' && observeGate?.completed !== true;
  const modeChanged = modeDraft !== (status?.mode ?? 'off');
  const modeVersion = typeof status?.modeVersion === 'number' ? status.modeVersion : null;
  const modeControlDisabled = isSavingMode || modeVersion === null || status?.modeDegraded === true;
  const styleChanged = styleDraft !== savedStyle;
  const styleControlDisabled = isSavingStyle || styleConfigVersion === null;
  const observationHealth = toRecord(runDetail?.provenance?.observationHealth ?? runDetail?.provenance?.observation_health);
  const runProvenance = toRecord(runDetail?.provenance);
  const recordedObservationHealth = toRecord(runProvenance?.recordedObservationHealth ?? runProvenance?.recorded_observation_health);
  const discoveryQuality = toRecord(observationHealth?.discoveryQuality ?? observationHealth?.discovery_quality);
  const observationCandidateCount = firstNumber(runProvenance, ['observationCandidateCount', 'observation_candidate_count']);
  const selectedCandidateCount = typeof runDetail?.selectedCandidateCount === 'number'
    ? runDetail.selectedCandidateCount
    : firstNumber(runProvenance, ['selectedCandidateCount', 'selected_candidate_count']);
  const discoveryCandidateCount = firstNumber(discoveryQuality, ['candidateCount', 'candidate_count']);
  const discoveryCandidateIncompleteCount = firstNumber(discoveryQuality, ['candidateDataIncompleteCount', 'candidate_data_incomplete_count']);
  const discoveryThemeIncompleteCount = firstNumber(discoveryQuality, ['themeDataIncompleteCount', 'theme_data_incomplete_count']);
  const discoveryQualityStatus = String(discoveryQuality?.status ?? '').toLowerCase();
  const discoveryQualityScope = String(discoveryQuality?.scope ?? '');
  const hasPartialDiscoveryQuality = discoveryQualityStatus === 'partial'
    || (discoveryCandidateIncompleteCount != null && discoveryCandidateIncompleteCount > 0)
    || (discoveryThemeIncompleteCount != null && discoveryThemeIncompleteCount > 0);
  const observationHealthReason = observationHealth
    ? observationHealth.reason ?? observationHealth.healthReason ?? observationHealth.health_reason ?? ''
    : runDetail?.healthReason ?? '';
  const effectiveHealthStatus = runDetail?.healthStatus ?? runDetail?.status ?? null;
  const isRiskBlockedRun = String(effectiveHealthStatus || '').toLowerCase() === 'risk_blocked'
    || String(runDetail?.marketRegime || '').toLowerCase() === 'risk_off';

  const coreRows = useMemo(
    () => buildCoreRows(runDetail?.candidates ?? [], modelFilter),
    [runDetail?.candidates, modelFilter],
  );
  const queueItems = useMemo(() => buildFollowupQueue(coreRows), [coreRows]);
  const coreCandidateIds = useMemo(() => new Set(coreRows.map((row) => row.candidate.id)), [coreRows]);
  const restCandidates = useMemo(
    () => (runDetail?.candidates ?? []).filter((candidate) => !coreCandidateIds.has(candidate.id)),
    [runDetail?.candidates, coreCandidateIds],
  );
  const restGroups = useMemo(() => buildRestGroups(restCandidates), [restCandidates]);
  const restObservingCount = restCandidates.filter((candidate) => String(candidate.status) === 'observing').length;
  const restRejectedCount = restCandidates.filter((candidate) => String(candidate.status) === 'rejected').length;
  const restSelectedCount = restCandidates.filter((candidate) => String(candidate.status) === 'selected').length;

  const runMode = String(runDetail?.mode ?? status?.mode ?? '').toLowerCase();
  const runRegime = String(runDetail?.marketRegime ?? '').toLowerCase();
  const runStatusKey = String(runDetail?.status ?? '').toLowerCase();
  const analysisBlockReason = String(runProvenance?.analysisBlockReason ?? runProvenance?.analysis_block_reason ?? '');
  const researchBudget = toRecord(runDetail?.provenance?.research);
  const researchBlockReason = String(researchBudget?.blockReason ?? researchBudget?.block_reason ?? '');
  const llmSubmittedCount = typeof runDetail?.llmSubmittedJobCount === 'number' ? runDetail.llmSubmittedJobCount : null;
  const llmLimit = firstNumber(status?.limits, ['maxLlmCallsPerRun', 'max_llm_calls_per_run'])
    ?? firstNumber(runDetail?.configSnapshot, ['maxLlmCallsPerRun', 'max_llm_calls_per_run'])
    ?? (typeof runDetail?.llmBudget === 'number' ? runDetail.llmBudget : null);
  const runStartClock = formatChinaClock(runDetail?.startedAt);
  const runEndClock = formatChinaClock(runDetail?.completedAt);
  const runTimeRange = runStartClock && runEndClock
    ? `${runStartClock}–${runEndClock}`
    : runStartClock
      ? `${runStartClock} 开始`
      : '';
  const runHasFailureDetail = ['blocked', 'failed'].includes(runStatusKey) && Boolean(runDetail?.errorMessage);
  const runHeadline = runDetail
    ? [
      formatShortDate(runDetail.tradeDate),
      `运行 #${runDetail.id}`,
      `${runStatusLabel(runDetail.status)}${runHasFailureDetail ? `（${reasonLabel(runDetail.errorMessage)}）` : ''}${runTimeRange ? ` ${runTimeRange}` : ''}`,
      modeLabel(runDetail.mode),
    ].join(' · ')
    : '';

  let regimeConsequence = '达标候选送 LLM 分析，可生成模拟计划';
  if (runMode === 'observe') {
    regimeConsequence = '观察模式只发现候选，不调用 LLM';
  } else if (runMode && runMode !== 'paper') {
    regimeConsequence = '当前模式不调用 LLM';
  } else if (runRegime === 'risk_off') {
    if (researchBudget && researchBlockReason) {
      regimeConsequence = `只做研究，但研究未启动（${reasonLabel(researchBlockReason)}）`;
    } else if (researchBudget) {
      regimeConsequence = '只做研究，不生成模拟计划';
    } else {
      regimeConsequence = '本次不送 LLM 分析，也不生成模拟计划';
    }
  } else if (runRegime === 'unavailable' || analysisBlockReason === 'market_regime_unavailable') {
    regimeConsequence = '本次不送 LLM 分析';
  } else if (analysisBlockReason === 'observe_gate_incomplete') {
    regimeConsequence = '观察验收未完成，只发现候选，不调用 LLM';
  } else if (runHasFailureDetail) {
    regimeConsequence = '本次未送 LLM 分析';
  }

  const discoveryReasonCounts = toRecord(discoveryQuality?.reasonCounts ?? discoveryQuality?.reason_counts);
  const discoveryDailyUnavailable = firstNumber(discoveryReasonCounts, ['dailyDataUnavailable', 'daily_data_unavailable']);
  const discoverySampleCount = discoveryCandidateCount ?? runDetail?.candidateCount ?? null;
  let dataCompletenessLine = `数据：样本 ${formatCount(runDetail?.candidateCount)} 只，未记录数据完整度`;
  if (discoveryQuality) {
    const head = `数据：样本 ${formatCount(discoverySampleCount)} 只`;
    if (discoveryDailyUnavailable != null && discoveryDailyUnavailable > 0) {
      dataCompletenessLine = `${head}，${discoveryDailyUnavailable} 只缺当日日线 ⚠`;
    } else if (discoveryCandidateIncompleteCount != null && discoveryCandidateIncompleteCount > 0) {
      dataCompletenessLine = `${head}，${discoveryCandidateIncompleteCount} 只数据不完整 ⚠`;
    } else {
      dataCompletenessLine = `${head}，数据完整`;
    }
    if (discoveryThemeIncompleteCount) {
      dataCompletenessLine += `，主题 ${discoveryThemeIncompleteCount} 个数据不完整`;
    }
  }

  let nextRunLine = '下一次：随定时分析批次';
  if (String(status?.mode ?? '').toLowerCase() === 'off') {
    nextRunLine = '下一次：机会池已关闭';
  } else if (runTimeSetting === undefined) {
    nextRunLine = '下一次：--';
  } else if (runTimeSetting === null) {
    nextRunLine = '下一次：--（系统配置不可读）';
  } else if (RUN_TIME_PATTERN.test(runTimeSetting)) {
    nextRunLine = `下一次：交易日 ${runTimeSetting} 后（晚间运行）`;
  }

  const emptyCoreReason = (): string => {
    if (runMode === 'observe') {
      return '观察模式不调用 LLM';
    }
    if (runMode !== 'paper') {
      return '当前模式不调用 LLM';
    }
    if (analysisBlockReason === 'observe_gate_incomplete') {
      return '观察验收未完成';
    }
    if (runRegime === 'unavailable' || analysisBlockReason === 'market_regime_unavailable') {
      return '大盘状态不可用';
    }
    if (runMode === 'paper' && researchBudget && researchBlockReason) {
      return `研究未启动：${reasonLabel(researchBlockReason)}`;
    }
    if (runRegime === 'risk_off') {
      return '风险市只做研究，本次没有候选通过研究门槛';
    }
    if (runHasFailureDetail) {
      return reasonLabel(runDetail?.errorMessage);
    }
    if (!runDetail?.selectedCandidateCount) {
      return '没有候选达到分析条件';
    }
    return '达标候选尚未创建分析任务';
  };

  const saveMode = async () => {
    if (modeVersion === null) {
      setError('当前模式版本不可用，不能保存。请刷新状态，若仍不可用说明运行时设置存储处于 degraded。');
      return;
    }
    if (paperGateIncomplete) {
      setError(`paper 模式需要先完成观察门槛：当前 ${observeProgressLabel}。`);
      return;
    }
    setIsSavingMode(true);
    setNotice(null);
    try {
      const payload = await marketOpportunitiesApi.updateMode({
        mode: modeDraft,
        expectedVersion: modeVersion,
        reason: 'web market opportunity mode switch',
      });
      setStatus(payload);
      setModeDraft(payload.mode as 'off' | 'observe' | 'paper');
      setNotice(`机会池模式已切换为 ${payload.mode}，来源 ${payload.modeSource ?? 'unknown'}，下一次状态检查或任务执行生效，无需重启容器。`);
      await refreshCurrent();
    } catch (err) {
      const parsed = getParsedApiError(err);
      if (parsed.status === 409) {
        const latest = await refreshCurrent();
        setError(`机会池模式保存失败：状态已被其他入口更新，已刷新为 ${latest?.mode ?? '最新状态'}，请确认后重试。`);
      } else {
        setError(parsed.message || '机会池模式保存失败；如果提示版本冲突，请刷新后重试。');
      }
    } finally {
      setIsSavingMode(false);
    }
  };

  const clearModeOverride = async () => {
    if (modeVersion === null) {
      setError('当前模式版本不可用，不能清除覆盖值。请刷新状态，若仍不可用说明运行时设置存储处于 degraded。');
      return;
    }
    setIsSavingMode(true);
    setNotice(null);
    try {
      const payload = await marketOpportunitiesApi.clearMode({
        expectedVersion: modeVersion,
        reason: 'web clear market opportunity mode override',
      });
      setStatus(payload);
      setModeDraft(payload.mode as 'off' | 'observe' | 'paper');
      setNotice(`已清除 DB 覆盖值，当前回退到 ${payload.modeSource ?? 'environment'}=${payload.mode}，下一次状态检查或任务执行生效。`);
      await refreshCurrent();
    } catch (err) {
      const parsed = getParsedApiError(err);
      if (parsed.status === 409) {
        const latest = await refreshCurrent();
        setError(`清除覆盖值失败：状态已被其他入口更新，已刷新为 ${latest?.mode ?? '最新状态'}，请确认后重试。`);
      } else {
        setError(parsed.message || '清除机会池模式覆盖值失败；如果提示版本冲突，请刷新后重试。');
      }
    } finally {
      setIsSavingMode(false);
    }
  };

  const saveStyle = async () => {
    if (styleConfigVersion === null) {
      setError('当前配置版本不可用，不能保存风格。请刷新页面后重试。');
      return;
    }
    setIsSavingStyle(true);
    setNotice(null);
    try {
      const payload = await systemConfigApi.update({
        configVersion: styleConfigVersion,
        items: [{ key: STYLE_CONFIG_KEY, value: styleDraft }],
      });
      setSavedStyle(styleDraft);
      setStyleConfigVersion(payload.configVersion);
      const label = STYLE_OPTIONS.find((option) => option.value === styleDraft)?.label ?? styleDraft;
      setNotice(`机会池筛选风格已保存为「${styleDraft ? label : '继承（跟随模拟交易）'}」，下一次机会池运行生效，不影响模拟交易主账户及其现有持仓。`);
    } catch (err) {
      if (err instanceof SystemConfigConflictError) {
        try {
          const latest = await systemConfigApi.getConfig(false);
          const item = latest.items.find((entry) => entry.key === STYLE_CONFIG_KEY);
          const raw = String(item?.value ?? '').trim();
          const normalized = STYLE_VALUES.includes(raw) ? raw : '';
          setSavedStyle(normalized);
          setStyleDraft(normalized);
          setStyleConfigVersion(latest.configVersion);
          const runTimeItem = latest.items.find((entry) => entry.key === RUN_TIME_CONFIG_KEY);
          setRunTimeSetting(String(runTimeItem?.value ?? '').trim());
          setError(`风格保存失败：配置已被其他入口更新，已刷新为「${normalized || '继承'}」，请确认后重试。`);
        } catch {
          setError('风格保存失败：配置版本冲突且刷新失败，请手动刷新页面后重试。');
        }
      } else {
        setError(getParsedApiError(err).message || '机会池筛选风格保存失败，请重试。');
      }
    } finally {
      setIsSavingStyle(false);
    }
  };

  const refreshCurrent = async (): Promise<MarketOpportunityStatusResponse | null> => {
    setIsLoading(true);
    try {
      const [statusPayload, runsPayload] = await Promise.all([
        marketOpportunitiesApi.getStatus(),
        marketOpportunitiesApi.getRuns({ page: runPage, limit: RUN_PAGE_SIZE }),
      ]);
      setStatus(statusPayload);
      const normalized = String(statusPayload.mode ?? 'off').toLowerCase();
      if (normalized === 'off' || normalized === 'observe' || normalized === 'paper') {
        setModeDraft(normalized);
      }
      setRuns(runsPayload.items);
      setRunTotal(runsPayload.total);
      const nextRunId = selectedRunId ?? runsPayload.items[0]?.id ?? null;
      setSelectedRunId(nextRunId);
      if (nextRunId) {
        const [detailPayload, evaluationPayload, candidatesPayload] = await Promise.all([
          marketOpportunitiesApi.getRun(nextRunId),
          marketOpportunitiesApi.getEvaluation({ runId: nextRunId, windows: [1, 3, 5, 10, 20, 40] }),
          marketOpportunitiesApi.getCandidates({
            runId: nextRunId,
            status: candidateStatusFilter || undefined,
            model: modelFilter || undefined,
            page: candidatePage,
            limit: CANDIDATE_PAGE_SIZE,
          }),
        ]);
        setRunDetail(detailPayload);
        setEvaluation(evaluationPayload);
        setCandidates(candidatesPayload.items);
        setCandidateTotal(candidatesPayload.total);
      }
      setError(null);
      return statusPayload;
    } catch (err) {
      setError(getParsedApiError(err).message || '机会池刷新失败');
      return null;
    } finally {
      setIsLoading(false);
    }
  };

  const runAction = async (kind: 'discover' | 'analyze') => {
    if (kind === 'analyze' && !selectedRunId) {
      setError('请先选择一个机会池运行。');
      return;
    }
    setIsActing(true);
    setNotice(null);
    try {
      const response = kind === 'discover'
        ? await marketOpportunitiesApi.discover({ market: 'cn' })
        : await marketOpportunitiesApi.analyzeRun(selectedRunId as number);
      setNotice(`${kind === 'discover' ? '仅发现' : '分析候选（模拟链路）'}已提交：${response.status}`);
      await refreshCurrent();
    } catch (err) {
      setError(getParsedApiError(err).message || '机会池操作失败');
    } finally {
      setIsActing(false);
    }
  };

  const openSettings = () => {
    setSettingsOpen(true);
    settingsRef.current?.scrollIntoView?.({ block: 'start' });
  };

  const restSummary = `其余 ${restCandidates.length} 只（观察 ${restObservingCount} · 淘汰 ${restRejectedCount}${restSelectedCount ? ` · 达标未分析 ${restSelectedCount}` : ''}）${restGroups.length ? `：${restGroups.map((group) => `${group.label} ${group.count}`).join(' · ')}` : ''}`;

  return (
    <main className="page-container space-y-6">
      <header className="rounded-3xl border border-sky-200 bg-card p-6 shadow-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div>
            <p className="text-sm font-semibold uppercase tracking-[0.24em] text-sky-500">Market Opportunity Pool</p>
            <h1 className="mt-2 text-3xl font-bold text-primary">大盘风口机会池</h1>
            <p className="mt-3 max-w-3xl text-sm leading-6 text-secondary">
              从盘后大盘报告提取今日风口，先进入观察池，再按健康观察天数、模型资格和模拟风控逐层推进。
            </p>
            <p className="mt-2 text-sm leading-6 text-secondary">
              健康 3 日是工程验收：确认系统累计收集了可审计观察，不代表收益已经被验证。候选与自选分离：本页没有“加入自选”或真实交易按钮。
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-3">
            <button
              type="button"
              className="btn-secondary"
              aria-label="展开设置与说明"
              aria-controls="market-opportunity-settings"
              aria-expanded={settingsOpen}
              onClick={openSettings}
            >
              设置与说明
            </button>
            <select
              className="input-field min-w-56"
              aria-label="选择机会池运行"
              value={selectedRunId ?? ''}
              onChange={(event) => {
                setSelectedRunId(event.target.value ? Number(event.target.value) : null);
                setCandidatePage(1);
              }}
            >
              {runs.length === 0 && <option value="">暂无运行</option>}
              {runs.map((run) => (
                <option key={run.id} value={run.id}>
                  #{run.id} · {run.tradeDate ?? '--'} · {runStatusLabel(run.status)}
                </option>
              ))}
            </select>
            <select
              className="input-field min-w-52"
              aria-label="按模型筛选"
              value={modelFilter}
              onChange={(event) => {
                setModelFilter(event.target.value);
                setCandidatePage(1);
              }}
            >
              <option value="">全部模型</option>
              {configuredModels.map((model) => (
                <option key={model} value={model}>{model}</option>
              ))}
            </select>
            <button
              type="button"
              className="btn-secondary"
              disabled={isActing || status?.mode === 'off'}
              onClick={() => runAction('discover')}
              aria-label="只从大盘报告发现候选，不调用 LLM"
            >
              仅发现
            </button>
            <button
              type="button"
              className="btn-primary"
              disabled={isActing || status?.mode !== 'paper' || !selectedRunId}
              onClick={() => runAction('analyze')}
              aria-label="仅在 paper 模式分析当前 run 的候选，仍是模拟链路"
            >
              分析候选（模拟链路）
            </button>
          </div>
        </div>

        <div className="mt-5 border-l-4 border-sky-300 pl-4">
          {!runDetail ? (
            <p className="text-sm text-secondary">{isLoading ? '运行概览加载中…' : '暂无运行记录。'}</p>
          ) : (
            <div className="space-y-1 text-sm text-secondary">
              <p className="text-base font-semibold text-primary">{runHeadline}</p>
              <p>
                大盘：{REGIME_LABELS[runRegime] ?? (runDetail.marketRegime || '未记录')} → {regimeConsequence}
              </p>
              <p>{dataCompletenessLine}
                {`     LLM：${llmSubmittedCount == null ? '--' : llmSubmittedCount} 次 / 上限 ${formatCount(llmLimit)}`}
              </p>
              <p>{nextRunLine}</p>
            </div>
          )}
        </div>

        <details
          id="market-opportunity-settings"
          ref={settingsRef}
          className="mt-5 rounded-2xl border border-border bg-surface p-4"
          open={settingsOpen}
          onToggle={(event: React.SyntheticEvent<HTMLDetailsElement>) => setSettingsOpen(event.currentTarget.open)}
        >
          <summary className="cursor-pointer text-sm font-semibold text-primary">
            设置与说明
            <span className="ml-2 text-xs font-normal text-secondary">运行模式、筛选风格、晚间运行时间、观察验收规则、运行诊断</span>
          </summary>
          <div className="mt-4 space-y-4">
            <div className="rounded-2xl border border-sky-200 bg-sky-50 p-3 text-sm text-sky-900">
              <label className="block text-xs font-semibold uppercase tracking-[0.18em] text-sky-600" htmlFor="market-opportunity-mode">
                运行模式（热切换）
              </label>
              <div className="mt-2 flex flex-wrap items-center gap-2">
                <select
                  id="market-opportunity-mode"
                  className="input-field min-w-56"
                  value={modeDraft}
                  onChange={(event) => setModeDraft(event.target.value as 'off' | 'observe' | 'paper')}
                >
                  {MODE_OPTIONS.map((option) => (
                    <option key={option.value} value={option.value}>{option.label}</option>
                  ))}
                </select>
                <button
                  type="button"
                  className="btn-primary !px-3 !py-2 text-sm"
                  disabled={modeControlDisabled || !modeChanged || paperGateIncomplete}
                  onClick={saveMode}
                >
                  保存模式
                </button>
                <button
                  type="button"
                  className="btn-secondary !px-3 !py-2 text-sm"
                  disabled={modeControlDisabled || status?.modeSource !== 'database'}
                  onClick={clearModeOverride}
                  aria-label="清除数据库覆盖值并回退到环境值"
                >
                  回退环境值
                </button>
              </div>
              <p className="mt-2 text-xs leading-5 text-sky-800">
                {MODE_OPTIONS.find((option) => option.value === modeDraft)?.description}
                当前来源 {status?.modeSource ?? '--'}，版本 {status?.modeVersion ?? '--'}，
                更新时间 {formatChinaTime(status?.modeUpdatedAt)}，更新人 {status?.modeUpdatedBy ?? '--'}。
                保存后下一次状态检查或任务执行生效，无需重启容器。
              </p>
              {status?.modeDegraded && (
                <p className="mt-1 text-xs font-medium text-rose-700">
                  运行时配置存储 degraded：系统已 fail-closed 到 off，暂不允许从页面切换。
                </p>
              )}
              {paperGateIncomplete && (
                <p className="mt-1 text-xs font-medium text-amber-700">
                  paper 仍受观察门槛保护：未完成观察验收前页面不会提交，后端也会阻止候选分析。
                </p>
              )}
            </div>

            {styleAvailable && (
              <div className="rounded-2xl border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">
                <label className="block text-xs font-semibold uppercase tracking-[0.18em] text-emerald-600" htmlFor="market-opportunity-style">
                  筛选风格（中/短/长）
                </label>
                <div className="mt-2 flex flex-wrap items-center gap-2">
                  <select
                    id="market-opportunity-style"
                    className="input-field min-w-56"
                    value={styleDraft}
                    disabled={styleControlDisabled}
                    onChange={(event) => setStyleDraft(event.target.value)}
                  >
                    {STYLE_OPTIONS.map((option) => (
                      <option key={option.value || 'inherit'} value={option.value}>{option.label}</option>
                    ))}
                  </select>
                  <button
                    type="button"
                    className="btn-primary !px-3 !py-2 text-sm"
                    disabled={styleControlDisabled || !styleChanged}
                    onClick={saveStyle}
                  >
                    保存风格
                  </button>
                </div>
                <p className="mt-2 text-xs leading-5 text-emerald-800">
                  {STYLE_OPTIONS.find((option) => option.value === styleDraft)?.description}
                  仅改机会池筛选/研究基础风格，不影响模拟交易主账户及其现有持仓；保存后下一次机会池运行即可生效，不用重启服务。
                </p>
              </div>
            )}

            <p className="text-xs leading-5 text-secondary">
              {runTimeSetting === undefined
                ? '晚间运行时间：读取中…'
                : runTimeSetting === null
                  ? '晚间运行时间：系统配置不可读。'
                  : RUN_TIME_PATTERN.test(runTimeSetting)
                    ? `晚间运行时间：交易日 ${runTimeSetting} 后运行一次（等待当日大盘复盘完成、且没有分析批次在跑）。在系统设置中修改 MARKET_OPPORTUNITY_RUN_TIME。`
                    : runTimeSetting
                      ? `晚间运行时间：「${runTimeSetting}」不是 HH:MM，按未配置处理，随定时分析批次运行。`
                      : '晚间运行时间：未配置，机会池随定时分析批次结束后立即运行。在系统设置中修改 MARKET_OPPORTUNITY_RUN_TIME。'}
            </p>
            <p className="text-sm leading-6 text-secondary">
              风险偏高时只做研究、不生成模拟计划；研究看多的候选会排进后续交易日复核一次，复核同样只研究、不生成计划。
            </p>
            <p className="text-xs leading-5 text-secondary">
              provenance：{stableStringify(status?.modeProvenance)}
            </p>

            <div className="rounded-2xl border border-border bg-card p-3 text-xs text-secondary">
              <p className="font-medium text-primary">运行诊断</p>
              {runDetail ? (
                <>
                  <p className="mt-2">
                    当前 run #{runDetail.id} · {runDetail.market} · {runDetail.tradeDate ?? '--'} ·
                    创建 {formatChinaTime(runDetail.createdAt)} · 更新 {formatChinaTime(runDetail.updatedAt)} ·
                    健康状态 {runStatusLabel(runDetail.healthStatus ?? runDetail.status)}
                    {runDetail.healthReason ? `（${reasonLabel(runDetail.healthReason)}）` : ''}
                  </p>
                  {runDetail.provenance?.frozenMembershipReplay === true && (
                    <p className="mt-1 text-amber-600">
                      历史成分回放：复用当日已冻结的板块成分，行情截到当日；不计入健康观测天数。
                    </p>
                  )}
                  <p className="mt-1">
                    观察健康 {observationHealth?.healthy === true ? '健康' : observationHealth?.healthy === false ? '已排除' : '--'} ·
                    原因 {reasonLabel(String(observationHealthReason))} ·
                    快照 {formatCount(observationHealth?.sourceSnapshotId ?? observationHealth?.source_snapshot_id)} ·
                    口径 {displayValue(observationHealth?.countingBasis ?? observationHealth?.counting_basis)}
                  </p>
                  {recordedObservationHealth && (
                    <p className="mt-1">
                      原始运行健康 {recordedObservationHealth.healthy === true ? '健康' : recordedObservationHealth.healthy === false ? '已排除' : '--'} ·
                      原因 {reasonLabel(String(recordedObservationHealth.reason ?? recordedObservationHealth.healthReason ?? recordedObservationHealth.health_reason ?? ''))} ·
                      快照 {formatCount(recordedObservationHealth.sourceSnapshotId ?? recordedObservationHealth.source_snapshot_id)}
                    </p>
                  )}
                </>
              ) : (
                <p className="mt-2">{isLoading ? '运行诊断加载中…' : '暂无运行记录。'}</p>
              )}
            </div>
          </div>
        </details>
      </header>

      {notice && (
        <section className="rounded-2xl border border-emerald-200 bg-emerald-50 p-4 text-sm text-emerald-700">
          {notice}
        </section>
      )}

      {error && (
        <section className="rounded-2xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700">
          {error}
        </section>
      )}

      <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <div className="rounded-2xl border border-border bg-card p-5">
          <p className="text-sm text-secondary">运行模式</p>
          <p className="mt-2 text-2xl font-semibold text-primary">{status ? modeLabel(status.mode) : '--'}</p>
          <p className="mt-2 text-xs text-secondary">
            保存后下一次状态检查或任务执行生效。切换入口在「设置与说明」。
          </p>
        </div>
        <div className="rounded-2xl border border-border bg-card p-5">
          <p className="text-sm text-secondary">配置模型数</p>
          <p className="mt-2 text-2xl font-semibold text-primary">
            {configuredModels.length ? `${configuredModels.length} 个` : '--'}
          </p>
          <p className="mt-2 text-xs text-secondary">{configuredModels.join(' / ') || '未配置'}</p>
        </div>
        <div className="rounded-2xl border border-border bg-card p-5">
          <p className="text-sm text-secondary">观察验收天数</p>
          <p className="mt-2 text-2xl font-semibold text-primary">
            {observeProgressLabel}
          </p>
          <p className="mt-2 text-xs text-secondary">
            {observeRequiredDays == null ? '后端未暴露要求天数；' : `要求 ${observeRequiredDays} 个交易日；`}
            计数口径 {displayValue(observeCountingBasis)}。
          </p>
          <p className="mt-1 text-xs text-secondary">
            总运行日 {formatCount(observeTotalRunDays)}，健康运行日 {formatCount(observeCompletedDays)}。真实健康完整板块快照按交易日累计一次；零候选和候选局部失败不覆盖健康快照，板块全量快照采集失败不计入。
          </p>
          <p className="mt-1 text-xs text-secondary">
            {observeDates.length
              ? `观察日期 ${observeDates.join(' / ')}。`
              : observeDatesExposed
                ? '尚无计入观察门禁的日期。'
                : '观察日期未暴露。'}
          </p>
          <p className="mt-1 text-xs text-secondary">
            {observePositiveDates.length ? `正向日期 ${observePositiveDates.join(' / ')}。` : null}
            运行记录 {formatCount(runs.length)} / {formatCount(runTotal)}，第 {runPage} 页加载最近 {RUN_PAGE_SIZE} 条。
          </p>
          {observeExcludedDates.length ? (
            <p className="mt-1 text-xs text-amber-700">
              排除日期：{toDisplayList(observeExcludedDates).join(' / ')}
            </p>
          ) : null}
        </div>
        <div className="rounded-2xl border border-border bg-card p-5">
          <p className="text-sm text-secondary">候选容量</p>
          <p className="mt-2 text-2xl font-semibold text-primary">
            {formatCount(status?.limits?.maxObservationCandidates ?? status?.limits?.max_observation_candidates ?? status?.limits?.maxTotalCandidates)}
          </p>
          <p className="mt-2 text-xs text-secondary">
            {observationCandidateCount != null || selectedCandidateCount != null
              ? `当前批次 观察候选 ${formatCount(observationCandidateCount)} · 达到分析条件 ${formatCount(selectedCandidateCount)}。`
              : null}
            {discoveryQuality
              ? `发现质量 ${displayValue(discoveryQualityStatus)} · ${displayValue(discoveryQualityScope)} ${formatCount(discoveryCandidateCount)}；候选数据不完整 ${formatCount(discoveryCandidateIncompleteCount)}，主题数据不完整 ${formatCount(discoveryThemeIncompleteCount)}。`
              : null}
            观察池上限 {formatCount(status?.limits?.maxObservationCandidates ?? status?.limits?.max_observation_candidates)}；
            可分析上限 {formatCount(status?.limits?.maxTotalCandidates ?? status?.limits?.max_total_candidates)}；
            主题发现 {formatCount(status?.limits?.discoveryMaxThemes ?? status?.limits?.discovery_max_themes)}。
          </p>
          {hasPartialDiscoveryQuality ? (
            <p className="mt-2 text-xs text-amber-700">
              候选数据部分不完整：候选 {formatCount(discoveryCandidateIncompleteCount)} 个、主题 {formatCount(discoveryThemeIncompleteCount)} 个；这些候选保持拒绝，不会授权买入。
            </p>
          ) : null}
        </div>
      </section>

      {runDetail && (
        <section className="rounded-3xl border border-border bg-card p-5">
          <div className="flex flex-col gap-2 border-b border-border pb-4 md:flex-row md:items-end md:justify-between">
            <div>
              <h2 className="text-xl font-semibold text-primary">送 LLM 分析的 {coreRows.length} 只</h2>
              <p className="mt-1 text-sm text-secondary">
                看多的排在前面；点一行展开执行评估、缺失证据、新旧策略对照和各模型明细。
              </p>
            </div>
            <span className="text-sm text-secondary">全部候选 {formatCount(runDetail.candidateCount)} 只</span>
          </div>
          {coreRows.length === 0 ? (
            <p className="py-8 text-sm text-secondary">本次没有候选送 LLM 分析（{emptyCoreReason()}）。</p>
          ) : (
            <div className="mt-4 space-y-2">
              <div className="hidden gap-3 px-3 text-xs font-semibold uppercase tracking-[0.12em] text-secondary md:grid md:grid-cols-[1.2fr_1.1fr_0.5fr_1fr_1.2fr_0.9fr]">
                <span>股票</span>
                <span>主题</span>
                <span>规则分</span>
                <span>LLM 建议</span>
                <span>结果</span>
                <span>下一步</span>
              </div>
              {coreRows.map((row) => {
                const name = row.candidate.stockName || row.candidate.symbol;
                const advice = stateCellLines(row.states, stateAdvice);
                const result = stateCellLines(row.states, (state) => stateResult(state, isRiskBlockedRun));
                const nextStep = stateCellLines(row.states, stateNextStep);
                return (
                  <details
                    key={row.candidate.id}
                    data-testid="opportunity-core-row"
                    className="rounded-2xl border border-border bg-surface p-3"
                  >
                    <summary className="grid cursor-pointer gap-3 md:grid-cols-[1.2fr_1.1fr_0.5fr_1fr_1.2fr_0.9fr]">
                      <span className={`font-medium ${row.bullish ? 'text-emerald-700' : 'text-primary'}`}>{name}</span>
                      <span className="text-sm text-secondary">{stripThemePrefix(row.candidate.primaryThemeKey)}</span>
                      <span className="text-sm text-secondary">{formatScore(row.candidate.score)}</span>
                      <span className="text-sm text-secondary">
                        {advice.map((line) => <span key={line} className="block">{line}</span>)}
                      </span>
                      <span className="text-sm text-secondary">
                        {result.map((line) => <span key={line} className="block">{line}</span>)}
                      </span>
                      <span className="text-sm text-secondary">
                        {nextStep.map((line) => <span key={line} className="block">{line}</span>)}
                      </span>
                    </summary>
                    <div className="mt-3 border-t border-border pt-3">
                      <CandidateDetail
                        candidate={row.candidate}
                        evaluation={evaluation}
                        mode={status?.mode}
                        healthStatus={effectiveHealthStatus}
                        riskBlocked={isRiskBlockedRun}
                        showBadge
                      />
                    </div>
                  </details>
                );
              })}
            </div>
          )}
        </section>
      )}

      {runDetail && (
        <section className="rounded-3xl border border-border bg-card p-5">
          <h2 className="text-xl font-semibold text-primary">待复核 / 待确认</h2>
          <p className="mt-1 text-sm text-secondary">
            复核和确认都只发生在后续交易日，复核本身只做研究、不生成模拟计划。
          </p>
          {queueItems.length === 0 ? (
            <p className="py-6 text-sm text-secondary">暂无待复核或待确认的候选。</p>
          ) : (
            <ul className="mt-4 space-y-2">
              {queueItems.map((item) => (
                <li key={item.id} className="flex flex-wrap items-center justify-between gap-2 rounded-xl border border-border bg-surface px-3 py-2 text-sm">
                  <span className="font-medium text-primary">{item.label}</span>
                  <span className="text-xs text-secondary">{item.status}</span>
                </li>
              ))}
            </ul>
          )}
        </section>
      )}

      <section className="rounded-3xl border border-border bg-card p-5">
        <div className="flex flex-col gap-2 border-b border-border pb-4 md:flex-row md:items-end md:justify-between">
          <div>
            <h2 className="text-xl font-semibold text-primary">前瞻评估</h2>
            <p className="mt-1 text-sm text-secondary">
              只使用候选 as-of 之后的 K 线，分开观察选股表现、模型报告转化和模拟计划转化。时间按中国时间（UTC+8）展示。
            </p>
          </div>
          <span className="text-sm text-secondary">样本 {formatCount(evaluation?.candidateCount)}</span>
        </div>
        <div className="mt-4 grid gap-3 md:grid-cols-3 xl:grid-cols-6">
          {[1, 3, 5, 10, 20, 40].map((window) => (
            <div key={window} className="rounded-2xl border border-border bg-surface p-4">
              <p className="text-sm font-medium text-primary">{window} 日窗口</p>
              <p className="mt-2 text-xs text-secondary">已评估 {getWindowMetric(evaluation, window, 'evaluated')}</p>
              <p className="mt-1 text-xs text-secondary">命中率 {getWindowMetric(evaluation, window, 'hitRate')}</p>
              <p className="mt-1 text-xs text-secondary">平均收益 {getWindowMetric(evaluation, window, 'avgReturnPct')}</p>
              <p className="mt-1 text-xs text-secondary">
                {isWindowMature(evaluation, window) ? '已有到期样本' : window >= 20 ? '尚未成熟，等待足够交易日' : '等待样本'}
              </p>
            </div>
          ))}
        </div>
        <div className="mt-4 rounded-2xl border border-border bg-surface p-4 text-xs text-secondary">
          {Object.keys(evaluation?.modelExecutionSummary ?? {}).length === 0 ? (
            <p>暂无模型转化数据。</p>
          ) : (
            <div className="grid gap-2 md:grid-cols-2">
              {Object.entries(evaluation?.modelExecutionSummary ?? {}).map(([model, payload]) => {
                const row = payload as Record<string, unknown>;
                return (
                  <div key={model} className="rounded-xl border border-border bg-card p-3">
                    <p className="font-medium text-primary">{model}</p>
                    <p className="mt-1">报告 {formatCount(row.reports)} / 任务 {formatCount(row.jobs)}</p>
                    <p className="mt-1">计划 {formatCount(row.plans)} · 已实现盈亏 {formatCount(row.realizedPnl)}</p>
                  </div>
                );
              })}
            </div>
          )}
        </div>
      </section>

      <details data-testid="opportunity-rest" className="rounded-3xl border border-border bg-card p-5">
        <summary className="cursor-pointer text-sm font-semibold text-primary">{restSummary}</summary>
        <div className="mt-4 border-t border-border pt-4">
          <div className="flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
            <p className="text-sm text-secondary">
              只展示冻结在机会池中的候选；无计划原因来自分析任务、决策信号和模拟计划链路。
              当前加载 {candidates.length} / {candidateTotal} 条。
            </p>
            <div className="flex flex-wrap items-center gap-2">
              <button
                type="button"
                className="btn-secondary !px-3 !py-2 text-sm"
                disabled={runPage <= 1}
                onClick={() => setRunPage((page) => Math.max(1, page - 1))}
              >
                上一页运行
              </button>
              <span className="text-xs text-secondary">运行第 {runPage} 页</span>
              <button
                type="button"
                className="btn-secondary !px-3 !py-2 text-sm"
                disabled={runPage * RUN_PAGE_SIZE >= runTotal}
                onClick={() => setRunPage((page) => page + 1)}
              >
                下一页运行
              </button>
              <select
                className="input-field min-w-40"
                aria-label="按候选状态筛选"
                value={candidateStatusFilter}
                onChange={(event) => {
                  setCandidateStatusFilter(event.target.value);
                  setCandidatePage(1);
                }}
              >
                <option value="">全部状态</option>
                <option value="observing">观察中</option>
                <option value="selected">达到分析条件</option>
                <option value="rejected">已拒绝</option>
              </select>
            </div>
          </div>
          {isLoading ? (
            <div className="py-12 text-center text-secondary">加载中...</div>
          ) : candidates.length === 0 ? (
            <div className="py-12 text-center text-secondary">
              暂无候选。健康零候选会计入工程观察天数，但不会伪造成选股或买入信号。
            </div>
          ) : (
            <div className="mt-4 grid gap-3 xl:grid-cols-2">
              {candidates.map((item) => {
                if (coreCandidateIds.has(item.id)) {
                  return (
                    <p key={item.id} className="rounded-xl border border-border bg-surface px-3 py-2 text-sm text-secondary">
                      {item.stockName || item.symbol} · 已在「送 LLM 分析」表中
                    </p>
                  );
                }
                const name = item.stockName || item.symbol;
                return (
                  <details key={item.id} className="rounded-xl border border-border bg-surface p-3">
                    <summary className="cursor-pointer text-sm">
                      <span className="font-medium text-primary">{name}</span>
                      {item.stockName ? <span className="ml-2 text-xs text-secondary">{item.symbol}</span> : null}
                      <span className={`ml-2 rounded-full border px-2 py-0.5 text-xs ${statusClass(item.status)}`}>
                        {stageLabel(item)}
                      </span>
                      <span className="ml-2 text-xs text-secondary">{reasonLabel(item.noPlanReason)}</span>
                    </summary>
                    <div className="mt-3 border-t border-border pt-3">
                      <CandidateDetail
                        candidate={item}
                        evaluation={evaluation}
                        mode={status?.mode}
                        healthStatus={effectiveHealthStatus}
                        riskBlocked={isRiskBlockedRun}
                        showBadge={false}
                      />
                    </div>
                  </details>
                );
              })}
            </div>
          )}
          <div className="mt-5 flex flex-wrap items-center justify-between gap-3 border-t border-border pt-4 text-sm text-secondary">
            <span>候选第 {candidatePage} 页 · 当前加载 {candidates.length} / 总数 {candidateTotal}</span>
            <div className="flex items-center gap-2">
              <button
                type="button"
                className="btn-secondary !px-3 !py-2 text-sm"
                disabled={candidatePage <= 1}
                onClick={() => setCandidatePage((page) => Math.max(1, page - 1))}
              >
                上一页候选
              </button>
              <button
                type="button"
                className="btn-secondary !px-3 !py-2 text-sm"
                disabled={candidatePage * CANDIDATE_PAGE_SIZE >= candidateTotal}
                onClick={() => setCandidatePage((page) => page + 1)}
              >
                下一页候选
              </button>
            </div>
          </div>
        </div>
      </details>
    </main>
  );
};

export default MarketOpportunitiesPage;

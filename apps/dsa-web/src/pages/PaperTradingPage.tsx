import type React from 'react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ChevronDown, FlaskConical, Play, RefreshCw, WalletCards } from 'lucide-react';
import { analysisApi } from '../api/analysis';
import { getParsedApiError, type ParsedApiError } from '../api/error';
import { paperTradingApi } from '../api/paperTrading';
import { portfolioApi } from '../api/portfolio';
import { ApiErrorAlert, Badge, Card, EmptyState } from '../components/common';
import { PaperAccountOverview } from '../components/paperTrading/PaperAccountOverview';
import { latestPlanSession, marketGateFacts } from '../components/paperTrading/paperFacts';
import { PaperLotsTable } from '../components/paperTrading/PaperLotsTable';
import { PaperNoTradeFacts } from '../components/paperTrading/PaperNoTradeFacts';
import { PaperPlansTable } from '../components/paperTrading/PaperPlansTable';
import { label } from '../components/paperTrading/labels';
import { useUiLanguage } from '../contexts/UiLanguageContext';
import type { PaperAuctionObservation, PaperCapitalAccount, PaperCapitalAdjustment, PaperDispatch, PaperEvaluation, PaperHolding, PaperMonitorStatus, PaperObservation, PaperOperation, PaperPerformance, PaperPlan, PaperPosition, PaperReanalysisRequest, PaperReportPlanReconciliation, PaperResolution, PaperRiskEvent, PaperRiskLock, PaperRiskRevision, PaperRiskSnapshot, PaperTradeEvent, PaperTradingScope } from '../types/paperTrading';
import type { PortfolioAccountItem } from '../types/portfolio';

const money = (value?: number | null) => value == null ? '--' : value.toFixed(2);
const pct = (value?: number | null) => value == null ? '--' : `${value.toFixed(2)}%`;
const fractionPct = (value?: number | null) => value == null ? '--' : `${(value * 100).toFixed(0)}%`;
const currencyMoney = (value?: number | null, currency?: string | null) => (
  value == null ? '--' : `${currency ? `${currency} ` : ''}${value.toFixed(2)}`
);
const marketCurrencyMoney = (
  value: number | null | undefined,
  currency: string | null | undefined,
  complete: boolean | null | undefined,
  zh: boolean,
) => (complete === false ? (zh ? '不完整 / --' : 'Incomplete / --') : currencyMoney(value, currency));
const PAGE_SIZE = 50;
// 顶部核心卡片单独取数，不随下方审计表分页变化：未平仓批次、估值与资金账户取满后端单页上限，最新计划与最近平仓只取最近一段。
const MAIN_VIEW_FULL_LIMIT = 500;
const LATEST_PLAN_LIMIT = 100;
const RECENT_CLOSED_LIMIT = 20;
const AUTO_REFRESH_MS = 60_000;

const formatChinaTime = (value?: string | null) => {
  if (!value) return '--';
  const normalized = /(?:Z|[+-]\d{2}:\d{2})$/i.test(value) ? value : `${value}Z`;
  const date = new Date(normalized);
  if (Number.isNaN(date.getTime())) return value;
  const parts = new Intl.DateTimeFormat('zh-CN', {
    timeZone: 'Asia/Shanghai',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return `${values.year}-${values.month}-${values.day} ${values.hour}:${values.minute}:${values.second} (UTC+8)`;
};

type PageKey = 'accounts' | 'adjustments' | 'holdings' | 'operations' | 'plans' | 'positions' | 'events' | 'resolutions' | 'riskRevisions' | 'dispatches' | 'evaluations' | 'observations' | 'reanalysisRequests' | 'reportPlanReconciliations' | 'auctionObservations' | 'riskSnapshots' | 'riskLocks' | 'riskEvents';
type PageState = Record<PageKey, number>;
type TotalState = Record<PageKey, number>;

const initialPages: PageState = { accounts: 1, adjustments: 1, holdings: 1, operations: 1, plans: 1, positions: 1, events: 1, resolutions: 1, riskRevisions: 1, dispatches: 1, evaluations: 1, observations: 1, reanalysisRequests: 1, reportPlanReconciliations: 1, auctionObservations: 1, riskSnapshots: 1, riskLocks: 1, riskEvents: 1 };
const initialTotals: TotalState = { accounts: 0, adjustments: 0, holdings: 0, operations: 0, plans: 0, positions: 0, events: 0, resolutions: 0, riskRevisions: 0, dispatches: 0, evaluations: 0, observations: 0, reanalysisRequests: 0, reportPlanReconciliations: 0, auctionObservations: 0, riskSnapshots: 0, riskLocks: 0, riskEvents: 0 };
const fallbackMarkets = ['cn', 'hk', 'us'];
const makeIdempotencyKey = () => `web-${Date.now()}-${Math.random().toString(16).slice(2)}`;
const stableKeyPart = (value: unknown) => String(value ?? '').trim().replace(/[^a-zA-Z0-9_.:-]+/g, '_');
type FundForm = { adjustmentType: 'deposit' | 'withdrawal'; amount: string; reason: string; idempotencyKey?: string; fingerprint?: string };
type RiskManualForm = { reason: string; idempotencyKey?: string; fingerprint?: string };
type PlanScope = 'active' | 'all';
const emptyFundForm = (): FundForm => ({ adjustmentType: 'deposit', amount: '', reason: '' });
const fundFingerprint = (accountId: number, form: FundForm) => `${accountId}|${form.adjustmentType}|${Number(form.amount)}|${form.reason.trim()}`;
const riskManualFingerprint = (accountId: number, reason: string, action: string) => `${accountId}|${action}|${reason.trim()}`;

const asRecord = (value: unknown): Record<string, unknown> => (
  value && typeof value === 'object' && !Array.isArray(value) ? value as Record<string, unknown> : {}
);

const asArray = (value: unknown): Record<string, unknown>[] => (
  Array.isArray(value) ? value.map(asRecord) : []
);

const factorBadgeVariant = (factor: Record<string, unknown>) => {
  if (!factor.available) return 'warning';
  return factor.passed ? 'success' : 'default';
};

const eventQuantity = (event: PaperTradeEvent) => {
  if (typeof event.quantity === 'number') return event.quantity;
  const details = asRecord(event.details);
  const value = details.quantity ?? details.executedQuantity ?? details.executed_quantity;
  return typeof value === 'number' ? value : null;
};

const eventActionLabel = (eventType: string, zh: boolean) => {
  const type = eventType.toLowerCase();
  if (type.includes('open')) return zh ? '模拟买入' : 'Simulated buy';
  if (type.includes('close')) return zh ? '模拟卖出' : 'Simulated sell';
  if (type.includes('reduce')) return zh ? '模拟减仓' : 'Simulated reduce';
  if (type.includes('pending')) return zh ? '模拟待卖出' : 'Simulated sell pending';
  return eventType;
};

const compactJson = (value: unknown) => {
  if (value == null) return '--';
  if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return '--';
  }
};

const intentSummary = (items: Array<Record<string, unknown>> | undefined, zh: boolean) => {
  if (!items?.length) return zh ? '无' : 'None';
  return items.slice(0, 3).map((item) => String(item.intent ?? item.kind ?? item.reason ?? '--')).join(' / ');
};

const valuationWarning = (
  stale: boolean | null | undefined,
  observedAt: string | null | undefined,
  zh: boolean,
) => {
  if (!stale) return null;
  return `${zh ? '估值价已陈旧（不可用于交易）' : 'Valuation mark is stale (not for trading)'} · ${formatChinaTime(observedAt)}`;
};

const unpricedValuationText = (
  holdingCount: number | null | undefined,
  positionCount: number | null | undefined,
  zh: boolean,
) => {
  const holdings = holdingCount ?? positionCount ?? 0;
  const positions = positionCount ?? 0;
  if (zh) {
    return `估值不完整：${holdings} 只缺价持仓${positions > holdings ? `，涉及 ${positions} 批` : ''}`;
  }
  return `Incomplete valuation: ${holdings} unpriced holding(s)${positions > holdings ? ` across ${positions} lot(s)` : ''}`;
};

const lifecycleLabel = (operation: PaperOperation, zh: boolean) => {
  const state = operation.operationalState || operation.stage || operation.status || 'unknown';
  const labels: Record<string, { zh: string; en: string }> = {
    waiting_entry: { zh: '等待入场', en: 'Waiting entry' },
    entry_and_position_monitoring: { zh: '入场 + 持仓监控', en: 'Entry + position monitoring' },
    position_monitoring: { zh: '持仓监控', en: 'Position monitoring' },
    reanalysis_blocked: { zh: '复盘重分析阻塞', en: 'Reanalysis blocked' },
    pending_exit: { zh: 'T+1 待卖出', en: 'T+1 pending exit' },
    t1_pending_exit: { zh: 'T+1 待卖出', en: 'T+1 pending exit' },
    closed: { zh: '已关闭', en: 'Closed' },
    history: { zh: '历史', en: 'History' },
  };
  return labels[state]?.[zh ? 'zh' : 'en'] || state;
};

const lifecycleVariant = (operation: PaperOperation): 'success' | 'warning' | 'danger' | 'default' => {
  const state = operation.operationalState || operation.stage || operation.status || '';
  if (state.includes('blocked')) return 'danger';
  if (state.includes('pending')) return 'warning';
  return operation.isOperational === false ? 'default' : 'success';
};

const strategyActionLabel = (action: string | null | undefined, zh: boolean) => {
  const key = String(action || '').toLowerCase();
  const labels: Record<string, { zh: string; en: string }> = {
    buy: { zh: '买入', en: 'Buy' },
    add: { zh: '加仓', en: 'Add' },
    hold: { zh: '持有', en: 'Hold' },
    watch: { zh: '观望', en: 'Watch' },
    reduce: { zh: '减仓', en: 'Reduce' },
    sell: { zh: '卖出', en: 'Sell' },
    avoid: { zh: '回避', en: 'Avoid' },
    alert: { zh: '警戒', en: 'Alert' },
  };
  return labels[key]?.[zh ? 'zh' : 'en'] || action || '--';
};

const strategyConfirmationText = (operation: PaperOperation, zh: boolean) => {
  const count = operation.strategyThesisConfirmationCount;
  const required = operation.strategyThesisConfirmationRequired;
  if (count == null && required == null) return zh ? '未记录' : 'Not recorded';
  return `${count ?? 0}/${required ?? '--'} ${zh ? '个独立交易日' : 'distinct sessions'}`;
};

const idList = (value: Array<number | string> | undefined) => value?.length ? value.join(', ') : '--';

const reanalysisBlockingStatuses = new Set(['pending', 'claimed', 'submitted']);
const currentRiskLockStates = new Set(['warning', 'reduce_only', 'cooldown', 'manual_hold', 'halted', 'entry_suspended']);

const isCurrentRiskLock = (item: PaperRiskLock) => {
  const state = String(item.state || item.status || '').toLowerCase();
  return currentRiskLockStates.has(state) || item.entryAllowed === false || item.addAllowed === false;
};

const latestSnapshotByAccountId = (snapshots: PaperRiskSnapshot[]) => {
  const map = new Map<number, PaperRiskSnapshot>();
  snapshots.forEach((item) => {
    const current = map.get(item.accountId);
    if (!current || String(item.computedAt || item.bucketStartedAt || '') > String(current.computedAt || current.bucketStartedAt || '')) {
      map.set(item.accountId, item);
    }
  });
  return map;
};

const statusVariant = (status?: string | null): 'success' | 'warning' | 'danger' | 'default' => {
  const value = String(status || '').toLowerCase();
  if (value.includes('fail') || value.includes('reject') || value.includes('halt') || value.includes('blocked')) return 'danger';
  if (value.includes('pending') || value.includes('claimed') || value.includes('submitted') || value.includes('warning') || value.includes('cooldown') || value.includes('manual')) return 'warning';
  if (value.includes('complete') || value.includes('normal') || value.includes('allowed') || value.includes('pass')) return 'success';
  return 'default';
};

const dependencyText = (name: string, zh: boolean) => (
  zh
    ? `${name} 后端契约未就绪或请求失败；新开仓/加仓必须视为 fail-closed，已有止损、止盈、T+1 和减仓/卖出仍应继续。`
    : `${name} backend contract is unavailable or failed; new open/add must be treated as fail-closed while existing stops, targets, T+1 exits, reductions and closes remain enabled.`
);

const capacityText = (value: unknown, zh: boolean) => {
  const item = asRecord(value);
  const remaining = item.remainingCapacity ?? item.remaining_capacity ?? item.remaining;
  const cap = item.capPct ?? item.cap_pct ?? item.cap;
  const utilization = item.utilizationPct ?? item.utilization_pct ?? item.utilization;
  const status = item.status ?? item.reason ?? item.bucket ?? item.name;
  if (!Object.keys(item).length) return zh ? '未知容量：未提供' : 'Unknown capacity: not provided';
  return `${status ? `${String(status)} · ` : ''}${zh ? '剩余' : 'remaining'} ${money(typeof remaining === 'number' ? remaining : null)} · ${zh ? '上限' : 'cap'} ${typeof cap === 'number' ? pct(cap) : '--'} · ${zh ? '使用率' : 'util'} ${typeof utilization === 'number' ? pct(utilization) : '--'}`;
};

const permissionBadge = (allowed: boolean | null | undefined, label: string, zh: boolean) => (
  <Badge variant={allowed ? 'success' : 'danger'}>{label}: {allowed ? (zh ? '允许' : 'allowed') : (zh ? '禁止' : 'blocked')}</Badge>
);

const formatNumber = (value: unknown, suffix = '') => (
  typeof value === 'number' && Number.isFinite(value) ? `${value}${suffix}` : '--'
);

const formatList = (value: unknown) => (
  Array.isArray(value) && value.length ? value.join('/') : (value == null || value === '' ? '--' : String(value))
);

const boolText = (value: unknown, zh: boolean) => (
  value === true ? (zh ? '是' : 'yes') : value === false ? (zh ? '否' : 'no') : '--'
);

const heartbeatTaskRows = (status: PaperMonitorStatus | null) => {
  const tasks = asRecord(status?.schedulerTaskStates);
  const summaries = asRecord(status?.schedulerLastRunSummaries);
  return Object.entries(tasks)
    .filter((entry): entry is [string, Record<string, unknown>] => Boolean(entry[1]) && typeof entry[1] === 'object' && !Array.isArray(entry[1]))
    .map(([name, value]) => {
      const heartbeat = asRecord(value.heartbeat);
      const lease = asRecord(value.lease);
      const heartbeatDetails = asRecord(heartbeat.details);
      const leaseDetails = asRecord(lease.details);
      const summary = asRecord(summaries[name]);
      const heartbeatSummary = asRecord(heartbeatDetails.lastRunSummary);
      const leaseSummary = asRecord(leaseDetails.lastRunSummary);
      return {
        name,
        heartbeat,
        lease,
        summary: Object.keys(summary).length ? summary : Object.keys(heartbeatSummary).length ? heartbeatSummary : leaseSummary,
      };
    });
};

const styleSettingsRows = (settings: Record<string, unknown> | null | undefined, zh: boolean) => {
  const row = (labelZh: string, labelEn: string, value: unknown) => ({ label: zh ? labelZh : labelEn, value });
  return [
    row('数据窗口', 'Data window', formatNumber(settings?.analysisWindowSessions, zh ? ' 个交易日' : ' sessions')),
    row('持有期', 'Holding horizon', `${formatList(settings?.expectedHoldingSessionsRange)} ${zh ? '交易日' : 'sessions'} · max ${formatNumber(settings?.maxHoldingSessions)}`),
    row('腿数 / 加仓', 'Entry legs / adds', `${formatNumber(settings?.entryLegLimit)} / ${formatNumber(settings?.addLimit)}`),
    row('风险倍率', 'Risk multiplier', formatNumber(settings?.riskMultiplier)),
    row('事件 LLM 冷却', 'Event LLM cooldown', formatNumber(settings?.llmEventCooldownMinutes, zh ? ' 分钟' : ' min')),
    row('每日 LLM 上限', 'Daily LLM cap', formatNumber(settings?.llmDailyCap)),
    row('刷新节奏', 'Refresh cadence', formatNumber(settings?.planRefreshCadenceSessions, zh ? ' 个交易日' : ' sessions')),
  ];
};

const frozenStyleSummary = (
  style?: string | null,
  settings?: Record<string, unknown> | null,
  zh?: boolean,
) => {
  if (!style && !settings) return zh ? '未冻结/后端未提供' : 'Not frozen / not exposed';
  return `${style || '--'} · window ${formatNumber(settings?.analysisWindowSessions)} · hold ${formatList(settings?.expectedHoldingSessionsRange)} · max ${formatNumber(settings?.maxHoldingSessions)} · legs/adds ${formatNumber(settings?.entryLegLimit)}/${formatNumber(settings?.addLimit)} · risk ${formatNumber(settings?.riskMultiplier)} · LLM ${formatNumber(settings?.llmEventCooldownMinutes)}m/${formatNumber(settings?.llmDailyCap)}d · refresh ${formatNumber(settings?.planRefreshCadenceSessions)}`;
};

const marketRiskReasons = (risk: Record<string, unknown>) => (
  Array.isArray(risk.reasons) ? risk.reasons.map(String).filter(Boolean) : []
);

const marketRiskId = (risk: Record<string, unknown>) => (
  risk.contextId ?? risk.marketContextId ?? risk.marketContextID
);

type StrategyAuditSource = {
  tradingStyle?: string | null;
  tradingStylePolicyVersion?: string | null;
  tradingStyleSettings?: Record<string, unknown> | null;
  tradingStyleEvidence?: Record<string, unknown> | null;
  historySufficient?: boolean | null;
  frozenEvidenceValid?: boolean | null;
  marketRegime?: string | null;
  marketRegimePolicyVersion?: string | null;
  marketRegimeContextId?: number | null;
  marketRegimeProvenance?: Record<string, unknown> | null;
  marketRegimeFresh?: boolean | null;
  marketRegimeEntryAllowed?: boolean | null;
  marketRegimeAllocationMultiplier?: number | null;
  marketRegimeBlockNewRisk?: boolean | null;
  marketRegimeReasons?: string[];
};

const historySufficiencyText = (item: StrategyAuditSource, zh: boolean) => {
  const evidence = asRecord(item.tradingStyleEvidence);
  const sufficient = item.historySufficient ?? item.frozenEvidenceValid ?? evidence.historySufficient ?? evidence.frozenEvidenceValid;
  return boolText(sufficient, zh);
};

const marketGateText = (item: StrategyAuditSource, zh: boolean) => {
  const reasons = item.marketRegimeReasons?.length ? item.marketRegimeReasons : marketRiskReasons(asRecord(item.marketRegimeProvenance));
  return `${zh ? '新开/加仓' : 'open/add'} ${item.marketRegimeEntryAllowed === false || item.marketRegimeBlockNewRisk ? (zh ? '阻塞' : 'blocked') : item.marketRegimeEntryAllowed === true ? (zh ? '允许' : 'allowed') : '--'} · ${zh ? '新鲜' : 'fresh'} ${boolText(item.marketRegimeFresh, zh)} · ${zh ? '倍率' : 'multiplier'} ${formatNumber(item.marketRegimeAllocationMultiplier)} · ${zh ? '原因' : 'reasons'} ${reasons.length ? reasons.join(', ') : '--'}`;
};

const assetExposureCapsText = (status: PaperMonitorStatus, zh: boolean) => {
  const caps = asRecord(status.assetExposureCaps);
  const valueFor = (snake: string, camel: string) => {
    const value = caps[snake] ?? caps[camel];
    return typeof value === 'number' ? pct(Math.min(status.maxSymbolExposurePct ?? value, value)) : '--';
  };
  const overrideCount = Object.keys(asRecord(status.assetClassOverrides)).length;
  const parts = [
    `${zh ? '个股' : 'stock'} ${valueFor('individual_stock', 'individualStock')}`,
    `${zh ? '题材ETF' : 'theme ETF'} ${valueFor('theme_etf', 'themeEtf')}`,
    `${zh ? '宽基ETF' : 'broad ETF'} ${valueFor('broad_etf', 'broadEtf')}`,
    `${zh ? '未知' : 'unknown'} ${valueFor('unknown', 'unknown')}`,
  ];
  return `${parts.join(' / ')} · ${zh ? '显式分类覆盖' : 'explicit class overrides'} ${overrideCount}`;
};

const reconciliationReport = (item: PaperReportPlanReconciliation) => asRecord(item.report);
const reconciliationDetails = (item: PaperReportPlanReconciliation) => asRecord(item.details);
const reconciliationStockLabel = (item: PaperReportPlanReconciliation) => {
  const report = reconciliationReport(item);
  return String(report.name || report.code || `report #${item.sourceReportId ?? item.id}`);
};
const reconciliationReportMeta = (item: PaperReportPlanReconciliation, zh: boolean) => {
  const report = reconciliationReport(item);
  const code = String(report.code || `report #${item.sourceReportId ?? item.id}`);
  const market = report.market ? String(report.market) : (zh ? 'market 未暴露' : 'market not exposed');
  return `${code} · ${market}`;
};
const reconciliationTriggerSource = (item: PaperReportPlanReconciliation) => {
  const details = reconciliationDetails(item);
  return String(details.triggerSource || 'trigger source not exposed');
};
const reconciliationError = (item: PaperReportPlanReconciliation) => item.lastError || 'no error';
const reconciliationTime = (item: PaperReportPlanReconciliation) => (
  item.lastAttemptAt || item.nextRetryAt || item.updatedAt || item.createdAt
);

const nextCheckText = (operation: PaperOperation, zh: boolean) => {
  const next = operation.nextCheckObligation;
  if (!next) return zh ? '未排程 / 不可用' : 'Not scheduled / unavailable';
  const when = next.nextCheckTime || next.scheduledAt || next.dueAt;
  return `${next.status || (zh ? '已排程' : 'scheduled')} · ${formatChinaTime(when)}`;
};

const reanalysisText = (operation: PaperOperation, zh: boolean) => {
  const requests = operation.reanalysisRequests || [];
  if (!requests.length) return zh ? '未排程 / 数据不可用' : 'Not scheduled / unavailable';
  return requests.map((item) => `${item.status || '--'} · ${item.triggerType || item.source || (zh ? '来源受限' : 'provenance limited')}`).join(' / ');
};

const lowPayoffCorrectionText = (operation: PaperOperation, zh: boolean) => {
  const request = (operation.reanalysisRequests || []).find((item) => item.triggerType === 'plan_contract_low_payoff');
  if (!request) return null;
  const evidence = asRecord(request.evidence);
  const rawReasons = Array.isArray(evidence.reasons) ? evidence.reasons : [];
  const reasons = rawReasons.length ? rawReasons.map(String).join(', ') : request.failureReason;
  return `${zh ? '低盈亏比纠偏' : 'Low-payoff correction'}: ${request.status || '--'}${request.replacementReportId ? ` · #${request.replacementReportId}` : ''}${reasons ? ` · ${reasons}` : ''}`;
};

const exitLegText = (operation: PaperOperation, zh: boolean) => {
  const legs = operation.exitLegs || [];
  if (!legs.length) return null;
  const executed = new Set((operation.executedExitLegIds || []).map(String));
  return (
    <div className="mt-2 space-y-1">
      <div className="text-xs font-medium text-primary-text">
        {zh ? '分段止盈' : 'Staged exits'} · {zh ? '加权目标' : 'weighted target'} {money(operation.weightedTargetPrice)} · RR {money(operation.weightedRiskReward)}
      </div>
      {legs.map((leg) => {
        const id = String(leg.legId ?? leg.sequence ?? '--');
        return (
          <div key={id} className="rounded-lg border border-border px-2 py-1 text-xs">
            <span className="font-medium">{id}</span>
            <span className="ml-2">{money(leg.targetPrice)} / {fractionPct(leg.allocationFraction)}</span>
            <Badge variant={executed.has(id) ? 'success' : 'warning'}>{executed.has(id) ? (zh ? '已执行' : 'done') : (zh ? '待执行' : 'pending')}</Badge>
          </div>
        );
      })}
    </div>
  );
};

const Pager = ({ page, total, onPage, zh, label }: { page: number; total: number; onPage: (page: number) => void; zh: boolean; label?: string }) => {
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  if (total <= PAGE_SIZE) return null;
  return (
    <div role="navigation" aria-label={label || (zh ? '分页' : 'Pagination')} className="flex items-center justify-end gap-3 border-t border-border px-4 py-3 text-xs text-secondary-text">
      <span>{zh ? `共 ${total} 条 · 第 ${page}/${pages} 页` : `${total} total · Page ${page}/${pages}`}</span>
      <button type="button" className="btn-secondary px-3 py-1.5" disabled={page <= 1} onClick={() => onPage(page - 1)}>{zh ? '上一页' : 'Previous'}</button>
      <button type="button" className="btn-secondary px-3 py-1.5" disabled={page >= pages} onClick={() => onPage(page + 1)}>{zh ? '下一页' : 'Next'}</button>
    </div>
  );
};

const PaperTradingPage: React.FC = () => {
  const { language } = useUiLanguage();
  const zh = language === 'zh';
  const [model, setModel] = useState('');
  const [models, setModels] = useState<string[]>([]);
  const [scopeId, setScopeId] = useState('primary');
  const [scopes, setScopes] = useState<PaperTradingScope[]>([]);
  const [markets, setMarkets] = useState<string[]>(fallbackMarkets);
  const [operations, setOperations] = useState<PaperOperation[]>([]);
  const [plans, setPlans] = useState<PaperPlan[]>([]);
  const [latestPlanRows, setLatestPlanRows] = useState<{ total: number; items: PaperPlan[] }>({ total: 0, items: [] });
  const [openLotRows, setOpenLotRows] = useState<{ total: number; items: PaperPosition[] }>({ total: 0, items: [] });
  const [recentClosedLotRows, setRecentClosedLotRows] = useState<{ total: number; items: PaperPosition[] }>({ total: 0, items: [] });
  const [markHoldings, setMarkHoldings] = useState<PaperHolding[]>([]);
  const [mainViewAccounts, setMainViewAccounts] = useState<PaperCapitalAccount[]>([]);
  const [mainViewRiskSnapshots, setMainViewRiskSnapshots] = useState<PaperRiskSnapshot[]>([]);
  const [mainViewRiskLocks, setMainViewRiskLocks] = useState<PaperRiskLock[]>([]);
  const [planScope, setPlanScope] = useState<PlanScope>('active');
  const [holdings, setHoldings] = useState<PaperHolding[]>([]);
  const [positions, setPositions] = useState<PaperPosition[]>([]);
  const [events, setEvents] = useState<PaperTradeEvent[]>([]);
  const [resolutions, setResolutions] = useState<PaperResolution[]>([]);
  const [riskRevisions, setRiskRevisions] = useState<PaperRiskRevision[]>([]);
  const [accounts, setAccounts] = useState<PaperCapitalAccount[]>([]);
  const [portfolioAccounts, setPortfolioAccounts] = useState<PortfolioAccountItem[]>([]);
  const [adjustments, setAdjustments] = useState<PaperCapitalAdjustment[]>([]);
  const [dispatches, setDispatches] = useState<PaperDispatch[]>([]);
  const [evaluations, setEvaluations] = useState<PaperEvaluation[]>([]);
  const [observations, setObservations] = useState<PaperObservation[]>([]);
  const [reanalysisRequests, setReanalysisRequests] = useState<PaperReanalysisRequest[]>([]);
  const [reportPlanReconciliations, setReportPlanReconciliations] = useState<PaperReportPlanReconciliation[]>([]);
  const [auctionObservations, setAuctionObservations] = useState<PaperAuctionObservation[]>([]);
  const [riskSnapshots, setRiskSnapshots] = useState<PaperRiskSnapshot[]>([]);
  const [riskLocks, setRiskLocks] = useState<PaperRiskLock[]>([]);
  const [riskEvents, setRiskEvents] = useState<PaperRiskEvent[]>([]);
  const [contractErrors, setContractErrors] = useState<Record<string, string>>({});
  const [status, setStatus] = useState<PaperMonitorStatus | null>(null);
  const [pages, setPages] = useState<PageState>(initialPages);
  const [totals, setTotals] = useState<TotalState>(initialTotals);
  const [performance, setPerformance] = useState<PaperPerformance>({});
  const [loading, setLoading] = useState(true);
  const [running, setRunning] = useState(false);
  const [fundSubmitting, setFundSubmitting] = useState(false);
  const fundSubmitLock = useRef(false);
  const autoRefreshInFlight = useRef(false);
  const [error, setError] = useState<ParsedApiError | null>(null);
  const [fundMessage, setFundMessage] = useState<string | null>(null);
  const [riskMessage, setRiskMessage] = useState<string | null>(null);
  const [newAccount, setNewAccount] = useState({ sourceModel: '', market: 'cn', initialCapital: '' });
  const [fundForms, setFundForms] = useState<Record<number, FundForm>>({});
  const [riskForms, setRiskForms] = useState<Record<number, RiskManualForm>>({});

  const load = useCallback(async (silent = false) => {
    if (!silent) setLoading(true);
    setError(null);
    try {
      const [mainViewAccountResult, latestPlanResult, openLotResult, recentClosedLotResult, markHoldingResult, configured, monitorStatus, portfolioAccountRows, accountRows, adjustmentRows, holdingRows, operationRows, planRows, positionRows, eventRows, resolutionRows, riskRevisionRows, dispatchRows, evaluationRows, observationRows, metrics] = await Promise.all([
        paperTradingApi.getAccounts(model || undefined, { page: 1, limit: MAIN_VIEW_FULL_LIMIT, scopeId }),
        paperTradingApi.getPlans(model || undefined, { page: 1, limit: LATEST_PLAN_LIMIT, scopeId }),
        paperTradingApi.getPositions(model || undefined, { page: 1, limit: MAIN_VIEW_FULL_LIMIT, scopeId, status: 'open' }),
        paperTradingApi.getPositions(model || undefined, { page: 1, limit: RECENT_CLOSED_LIMIT, scopeId, status: 'closed' }),
        paperTradingApi.getHoldings(model || undefined, { page: 1, limit: MAIN_VIEW_FULL_LIMIT, scopeId }),
        analysisApi.getModels(),
        paperTradingApi.getStatus(),
        portfolioApi.getAccounts(false),
        paperTradingApi.getAccounts(model || undefined, { page: pages.accounts, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getAdjustments(model || undefined, { page: pages.adjustments, limit: PAGE_SIZE }),
        paperTradingApi.getHoldings(model || undefined, { page: pages.holdings, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getOperations(model || undefined, { page: pages.operations, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getPlans(model || undefined, { page: pages.plans, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getPositions(model || undefined, { page: pages.positions, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getEvents(model || undefined, { page: pages.events, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getResolutions(model || undefined, { page: pages.resolutions, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getRiskRevisions(model || undefined, { page: pages.riskRevisions, limit: PAGE_SIZE, scopeId }),
        paperTradingApi.getDispatches(model || undefined, { page: pages.dispatches, limit: PAGE_SIZE }),
        paperTradingApi.getEvaluations(model || undefined, { page: pages.evaluations, limit: PAGE_SIZE }),
        paperTradingApi.getObservations(model || undefined, { page: pages.observations, limit: PAGE_SIZE }),
        paperTradingApi.getPerformance(model || undefined),
      ]);
      const dependencyErrors: Record<string, string> = {};
      const loadContract = async <T,>(key: string, loader: () => Promise<{ total: number; items: T[] }>) => {
        try {
          return await loader();
        } catch (caught) {
          const parsed = getParsedApiError(caught);
          dependencyErrors[key] = parsed.message || parsed.title || 'backend contract unavailable';
          return { total: 0, items: [] as T[] };
        }
      };
      const [reanalysisRows, reconciliationRows, auctionRows, riskSnapshotRows, riskLockRows, riskEventRows] = await Promise.all([
        loadContract('reanalysisRequests', () => paperTradingApi.getReanalysisRequests(model || undefined, { page: pages.reanalysisRequests, limit: PAGE_SIZE })),
        loadContract('reportPlanReconciliations', () => paperTradingApi.getReportPlanReconciliations(model || undefined, { page: pages.reportPlanReconciliations, limit: PAGE_SIZE })),
        loadContract('auctionObservations', () => paperTradingApi.getAuctionObservations(model || undefined, { page: pages.auctionObservations, limit: PAGE_SIZE })),
        loadContract('riskSnapshots', () => paperTradingApi.getRiskSnapshots(model || undefined, { page: pages.riskSnapshots, limit: PAGE_SIZE })),
        loadContract('riskLocks', () => paperTradingApi.getRiskLocks(model || undefined, { page: pages.riskLocks, limit: PAGE_SIZE })),
        loadContract('riskEvents', () => paperTradingApi.getRiskEvents(model || undefined, { page: pages.riskEvents, limit: PAGE_SIZE })),
      ]);
      // 顶部卡片只看最新一页的风险快照与风险锁：审计表停在第一页时直接复用，翻到其他页时另取第一页。
      const [mainViewRiskSnapshotRows, mainViewRiskLockRows] = await Promise.all([
        pages.riskSnapshots === 1 ? riskSnapshotRows : loadContract('riskSnapshots', () => paperTradingApi.getRiskSnapshots(model || undefined, { page: 1, limit: PAGE_SIZE })),
        pages.riskLocks === 1 ? riskLockRows : loadContract('riskLocks', () => paperTradingApi.getRiskLocks(model || undefined, { page: 1, limit: PAGE_SIZE })),
      ]);
      try {
        setScopes(await paperTradingApi.getScopes());
      } catch (caught) {
        // scopes 是只读对照的增强项；实验注册表缺失时静默回退到仅主账户视图。
        dependencyErrors.scopes = getParsedApiError(caught).message || 'scope registry unavailable';
        setScopes([]);
      }
      const configuredModels = configured.models.map((item) => item.model);
      const configuredMarkets = (configured.supportedMarkets?.length ? configured.supportedMarkets : fallbackMarkets)
        .map((item) => item.toLowerCase())
        .filter((item, index, all) => item && all.indexOf(item) === index);
      setModels(configuredModels);
      setMarkets(configuredMarkets.length ? configuredMarkets : fallbackMarkets);
      setPortfolioAccounts(portfolioAccountRows.accounts || []);
      setNewAccount((current) => ({
        ...current,
        sourceModel: current.sourceModel || configuredModels[0] || '',
        market: configuredMarkets.includes(current.market) ? current.market : (configuredMarkets[0] || fallbackMarkets[0]),
      }));
      setStatus(monitorStatus);
      setAccounts(accountRows.items);
      setMainViewAccounts(mainViewAccountResult.items);
      setAdjustments(adjustmentRows.items);
      setHoldings(holdingRows.items);
      setOperations(operationRows.items);
      setPlans(planRows.items);
      setLatestPlanRows(latestPlanResult);
      setOpenLotRows(openLotResult);
      setRecentClosedLotRows(recentClosedLotResult);
      setMarkHoldings(markHoldingResult.items);
      setPositions(positionRows.items);
      setEvents(eventRows.items);
      setResolutions(resolutionRows.items);
      setRiskRevisions(riskRevisionRows.items);
      setDispatches(dispatchRows.items);
      setEvaluations(evaluationRows.items);
      setObservations(observationRows.items);
      setReanalysisRequests(reanalysisRows.items);
      setReportPlanReconciliations(reconciliationRows.items);
      setAuctionObservations(auctionRows.items);
      setRiskSnapshots(riskSnapshotRows.items);
      setRiskLocks(riskLockRows.items);
      setMainViewRiskSnapshots(mainViewRiskSnapshotRows.items);
      setMainViewRiskLocks(mainViewRiskLockRows.items);
      setRiskEvents(riskEventRows.items);
      setContractErrors(dependencyErrors);
      setTotals({ accounts: accountRows.total, adjustments: adjustmentRows.total, holdings: holdingRows.total, operations: operationRows.total, plans: planRows.total, positions: positionRows.total, events: eventRows.total, resolutions: resolutionRows.total, riskRevisions: riskRevisionRows.total, dispatches: dispatchRows.total, evaluations: evaluationRows.total, observations: observationRows.total, reanalysisRequests: reanalysisRows.total, reportPlanReconciliations: reconciliationRows.total, auctionObservations: auctionRows.total, riskSnapshots: riskSnapshotRows.total, riskLocks: riskLockRows.total, riskEvents: riskEventRows.total });
      setPerformance(metrics);
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      if (!silent) setLoading(false);
    }
  }, [model, pages, scopeId]);

  useEffect(() => {
    void load();

    const refreshWhenVisible = async () => {
      if (document.visibilityState !== 'visible' || autoRefreshInFlight.current) return;
      autoRefreshInFlight.current = true;
      try {
        await load(true);
      } finally {
        autoRefreshInFlight.current = false;
      }
    };

    const timer = window.setInterval(() => { void refreshWhenVisible(); }, AUTO_REFRESH_MS);
    const handleVisibilityChange = () => {
      if (document.visibilityState === 'visible') void refreshWhenVisible();
    };
    document.addEventListener('visibilitychange', handleVisibilityChange);

    return () => {
      window.clearInterval(timer);
      document.removeEventListener('visibilitychange', handleVisibilityChange);
    };
  }, [load]);

  const selectModel = (nextModel: string) => {
    setModel(nextModel);
    setPages(initialPages);
  };

  const selectScope = (nextScope: string) => {
    setScopeId(nextScope || 'primary');
    setPages(initialPages);
  };

  const isExperimentScope = scopeId !== 'primary';
  const activeScope = useMemo(() => scopes.find((item) => item.scopeId === scopeId) ?? null, [scopes, scopeId]);

  const setPage = (key: PageKey, page: number) => setPages((current) => ({ ...current, [key]: page }));

  const selectPlanScope = (nextScope: PlanScope) => {
    setPlanScope(nextScope);
    setPages((current) => ({ ...current, operations: 1, plans: 1 }));
  };

  const summary = useMemo(() => {
    const rows = Array.isArray(performance.models) ? performance.models : [];
    return model ? (rows.find((row) => row.model === model) ?? performance) : performance;
  }, [model, performance]);
  const normalizedReturn = (
    typeof summary.realizedReturnPct === 'number'
      ? summary.realizedReturnPct
      : null
  );
  const schedulerRows = useMemo(() => heartbeatTaskRows(status), [status]);
  const intradayRunSummary = asRecord(status?.intradayMonitorLastRunSummary);
  const hasIntradayRunSummary = Object.keys(intradayRunSummary).length > 0;
  const styleRows = useMemo(() => styleSettingsRows(status?.tradingStyleSettings, zh), [status?.tradingStyleSettings, zh]);
  const marketContextRisk = useMemo(() => asRecord(status?.marketContextRisk), [status]);
  const marketContextScope = useMemo(() => asRecord(marketContextRisk.scope), [marketContextRisk]);
  const marketContextReasons = useMemo(() => marketRiskReasons(marketContextRisk), [marketContextRisk]);

  const dispatchByEventId = useMemo(() => {
    const map = new Map<number, PaperDispatch>();
    dispatches.forEach((item) => {
      if (!map.has(item.eventId)) map.set(item.eventId, item);
    });
    return map;
  }, [dispatches]);

  const accountTotalsByCurrency = useMemo(() => {
    const totals = new Map<string, {
      currency: string;
      initialCapital: number;
      bookEquity: number;
      externalCashFlow: number;
      cash: number;
      investedCapital: number;
      realizedPnl: number;
      unrealizedPnl: number;
      totalPnl: number;
      marketValue: number;
      exposure: number;
      marketEquity: number;
      valuationComplete: boolean;
      unpricedPositionCount: number;
      unpricedHoldingCount: number;
      valuationObservedAt: string | null;
      valuationStale: boolean;
      openPositionCount: number;
    }>();
    mainViewAccounts.forEach((item) => {
      const currency = item.currency || '--';
      const current = totals.get(currency) ?? {
        currency,
        initialCapital: 0,
        bookEquity: 0,
        externalCashFlow: 0,
        cash: 0,
        investedCapital: 0,
        realizedPnl: 0,
        unrealizedPnl: 0,
        totalPnl: 0,
        marketValue: 0,
        exposure: 0,
        marketEquity: 0,
        valuationComplete: true,
        unpricedPositionCount: 0,
        unpricedHoldingCount: 0,
        valuationObservedAt: null,
        valuationStale: false,
        openPositionCount: 0,
      };
      const complete = item.valuationComplete !== false;
      current.initialCapital += item.initialCapital || 0;
      current.bookEquity += item.bookEquity ?? ((item.cash || 0) + (item.investedCapital || 0));
      current.externalCashFlow += item.externalCashFlow || 0;
      current.cash += item.cash || 0;
      current.investedCapital += item.investedCapital || 0;
      current.realizedPnl += item.realizedPnl || 0;
      current.valuationComplete = current.valuationComplete && complete;
      current.unpricedPositionCount += item.unpricedPositionCount || 0;
      current.unpricedHoldingCount += item.unpricedHoldingCount ?? item.unpricedPositionCount ?? 0;
      if (complete) {
        current.unrealizedPnl += item.unrealizedPnl || 0;
        current.totalPnl += item.totalPnl ?? ((item.realizedPnl || 0) + (item.unrealizedPnl || 0));
        current.marketValue += item.marketValue || 0;
        current.marketEquity += item.marketEquity ?? ((item.cash || 0) + (item.marketValue || 0));
      }
      current.valuationStale = current.valuationStale || Boolean(item.valuationStale);
      if (item.valuationObservedAt && (!current.valuationObservedAt || item.valuationObservedAt > current.valuationObservedAt)) {
        current.valuationObservedAt = item.valuationObservedAt;
      }
      current.exposure += item.exposure || 0;
      current.openPositionCount += item.openPositionCount || 0;
      totals.set(currency, current);
    });
    return Array.from(totals.values()).sort((left, right) => left.currency.localeCompare(right.currency));
  }, [mainViewAccounts]);

  const currentReanalysisRequests = useMemo(
    () => reanalysisRequests.filter((item) => reanalysisBlockingStatuses.has(String(item.status || '').toLowerCase())),
    [reanalysisRequests],
  );
  const historicalReanalysisRequests = useMemo(
    () => reanalysisRequests.filter((item) => !reanalysisBlockingStatuses.has(String(item.status || '').toLowerCase())),
    [reanalysisRequests],
  );
  const currentRiskLocks = useMemo(() => riskLocks.filter(isCurrentRiskLock), [riskLocks]);
  const mainViewCurrentRiskLocks = useMemo(() => mainViewRiskLocks.filter(isCurrentRiskLock), [mainViewRiskLocks]);
  const riskSnapshotByAccountId = useMemo(() => latestSnapshotByAccountId(riskSnapshots), [riskSnapshots]);

  // 同一币种下只有唯一账户时，账户级风险快照才能落到币种口径上；多账户不合并（回撤不可相加）。
  // 实验分组的账户来自独立数据库，而风险快照只读主库，按账户 id 关联会串库，因此不取。
  const riskSnapshotByCurrency = useMemo(() => {
    const map = new Map<string, PaperRiskSnapshot>();
    if (isExperimentScope) return map;
    const snapshotByAccountId = latestSnapshotByAccountId(mainViewRiskSnapshots);
    const accountIdsByCurrency = new Map<string, number[]>();
    mainViewAccounts.forEach((item) => {
      const currency = item.currency || '--';
      accountIdsByCurrency.set(currency, [...(accountIdsByCurrency.get(currency) || []), item.id]);
    });
    accountIdsByCurrency.forEach((ids, currency) => {
      const snapshot = ids.length === 1 ? snapshotByAccountId.get(ids[0]) : undefined;
      if (snapshot) map.set(currency, snapshot);
    });
    return map;
  }, [isExperimentScope, mainViewAccounts, mainViewRiskSnapshots]);

  const locale: 'zh' | 'en' = zh ? 'zh' : 'en';

  // 服务端已按状态过滤；批次状态只有 open / closed，这里按状态再取一次，保证两类不互相混入。
  const openLots = useMemo(() => openLotRows.items.filter((item) => String(item.status || '').toLowerCase() === 'open'), [openLotRows]);
  const recentClosedLots = useMemo(() => recentClosedLotRows.items.filter((item) => String(item.status || '').toLowerCase() === 'closed'), [recentClosedLotRows]);
  const latestSession = useMemo(() => latestPlanSession(latestPlanRows.items, latestPlanRows.total), [latestPlanRows]);

  // 大盘门控的中文/英文一句话，供概览与“为什么没有交易”共用，避免两处口径漂移。
  const marketGateSummary = useMemo(() => {
    if (isExperimentScope) return zh ? '实验分组不读取主库大盘门控，无法判断。' : 'Experiment scope does not read the primary-DB market gate.';
    const gate = marketGateFacts(marketContextRisk);
    if (!gate.controlEnabled) return zh ? '大盘风险控制未开启，不按大盘判断拦截新开仓。' : 'Market risk control is off, so no market-based entry block applies.';
    const basis = gate.sessionDate ? (zh ? `依据 ${gate.sessionDate} 交易日收盘判断` : `based on the ${gate.sessionDate} session close`) : (zh ? '缺少判断依据的交易日' : 'no basis session recorded');
    if (!gate.entryAllowed) {
      const reasonText = gate.reasons.length
        ? gate.reasons.map((item) => label('marketReason', item, locale)).join('、')
        : `${zh ? '大盘状态' : 'market regime'} ${label('regime', gate.regime, locale)}`;
      return zh
        ? `新开仓 / 加仓暂停（${reasonText}；${basis}），止损 / 止盈 / 减仓照常。`
        : `New open / add paused (${reasonText}; ${basis}); stops, targets and reductions continue.`;
    }
    const multiplierText = gate.multiplier != null && gate.multiplier < 1
      ? (zh ? `，仓位系数 ${gate.multiplier}` : `, allocation multiplier ${gate.multiplier}`)
      : '';
    return zh
      ? `新开仓 / 加仓允许（大盘${label('regime', gate.regime, locale)}；${basis}${multiplierText}）。`
      : `New open / add allowed (market ${label('regime', gate.regime, locale)}; ${basis}${multiplierText}).`;
  }, [isExperimentScope, locale, marketContextRisk, zh]);

  const runOnce = async (mode: 'eod' | 'intraday') => {
    if (isExperimentScope) return;
    setRunning(true);
    setError(null);
    try {
      await paperTradingApi.run(mode);
      await load();
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      setRunning(false);
    }
  };

  const syncPaperPositionToPortfolio = async (position: PaperPosition) => {
    if (isExperimentScope) return;
    const candidates = portfolioAccounts.filter((account) => account.market === position.market);
    const defaultAccount = candidates[0] ?? portfolioAccounts[0];
    if (!defaultAccount) {
      setFundMessage(zh ? '请先在持仓页创建同市场账户。' : 'Create a matching portfolio account first.');
      return;
    }
    const defaultSide = position.status === 'closed' ? 'sell' : 'buy';
    const accountInput = window.prompt(
      zh ? `持仓账户 ID（默认：${defaultAccount.name}）` : `Portfolio account ID (default: ${defaultAccount.name})`,
      String(defaultAccount.id),
    );
    if (accountInput == null) return;
    const sideInput = window.prompt(zh ? '方向 buy/sell' : 'Side buy/sell', defaultSide);
    if (sideInput == null) return;
    const quantityInput = window.prompt(zh ? '成交数量' : 'Execution quantity', String(position.quantity));
    if (quantityInput == null) return;
    const priceInput = window.prompt(zh ? '成交价格' : 'Execution price', String(position.closedPrice ?? position.openedPrice));
    if (priceInput == null) return;
    const dateInput = window.prompt(zh ? '成交日期 YYYY-MM-DD' : 'Execution date YYYY-MM-DD', position.closedOn ?? position.openedOn);
    if (dateInput == null) return;
    const feeInput = window.prompt(zh ? '费用' : 'Fee', '0');
    if (feeInput == null) return;
    const side = sideInput.trim().toLowerCase();
    const portfolioAccountId = Number(accountInput);
    const quantity = Number(quantityInput);
    const price = Number(priceInput);
    const fee = Number(feeInput);
    if (!Number.isFinite(portfolioAccountId) || !Number.isFinite(quantity) || !Number.isFinite(price) || !Number.isFinite(fee) || (side !== 'buy' && side !== 'sell')) {
      setFundMessage(zh ? '账户、方向、数量、价格或费用填写有误。' : 'Account, side, quantity, price, or fee is invalid.');
      return;
    }
    const confirmed = window.confirm(
      zh
        ? `确认把 ${position.stockCode} 的模拟${side === 'buy' ? '买入' : '卖出'}登记到真实持仓流水？不会连接券商。`
        : `Record this paper ${side} for ${position.stockCode} into portfolio trades? No broker will be contacted.`
    );
    if (!confirmed) return;
    setRunning(true);
    setError(null);
    try {
      await paperTradingApi.syncToPortfolio({
        paperPositionId: position.id,
        portfolioAccountId,
        side: side as 'buy' | 'sell',
        quantity,
        price,
        tradeDate: dateInput,
        fee,
        idempotencyKey: [
          'paper-to-portfolio',
          position.id,
          portfolioAccountId,
          side,
          dateInput,
          quantity,
          price,
          fee,
        ].map(stableKeyPart).join('-').slice(0, 128),
      });
      await load();
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      setRunning(false);
    }
  };

  const createCapitalAccount = async () => {
    if (isExperimentScope) return;
    if (fundSubmitLock.current) return;
    const initialCapital = newAccount.initialCapital.trim() ? Number(newAccount.initialCapital) : undefined;
    if (!newAccount.sourceModel || !newAccount.market || (initialCapital !== undefined && (!Number.isFinite(initialCapital) || initialCapital <= 0))) {
      setFundMessage(zh ? '请选择模型、市场；如填写初始本金，必须大于 0。' : 'Select a model and market; if initial capital is provided, it must be greater than 0.');
      return;
    }
    if (!window.confirm(zh ? '确认初始化一个仅用于模拟交易的资金账户？' : 'Initialize this paper-only capital account?')) return;
    fundSubmitLock.current = true;
    setFundSubmitting(true);
    setError(null);
    setFundMessage(null);
    try {
      const result = await paperTradingApi.createAccount({
        sourceModel: newAccount.sourceModel,
        market: newAccount.market,
        ...(initialCapital === undefined ? {} : { initialCapital }),
      });
      setFundMessage(result.created ? (zh ? '模拟资金账户已初始化。' : 'Paper capital account initialized.') : (zh ? '账户已存在，已刷新现有账户。' : 'Account already existed; refreshed current account.'));
      await load();
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      fundSubmitLock.current = false;
      setFundSubmitting(false);
    }
  };

  const updateFundForm = (accountId: number, patch: Partial<FundForm>) => {
    setFundForms((current) => ({
      ...current,
      [accountId]: {
        ...(current[accountId] || emptyFundForm()),
        ...patch,
        idempotencyKey: undefined,
        fingerprint: undefined,
      },
    }));
  };

  const submitFundAdjustment = async (account: PaperCapitalAccount) => {
    if (isExperimentScope) return;
    if (fundSubmitLock.current) return;
    const form = fundForms[account.id] || emptyFundForm();
    const amount = Number(form.amount);
    const reason = form.reason.trim();
    if (!Number.isFinite(amount) || amount <= 0) {
      setFundMessage(zh ? '入金/出金金额必须大于 0。' : 'Deposit/withdrawal amount must be greater than 0.');
      return;
    }
    if (!reason) {
      setFundMessage(zh ? '资金调整原因必填，便于后续审计。' : 'A fund-adjustment reason is required for audit.');
      return;
    }
    const label = form.adjustmentType === 'deposit' ? (zh ? '入金' : 'deposit') : (zh ? '出金' : 'withdrawal');
    if (!window.confirm(zh ? `确认对 ${account.accountKey} 做模拟${label} ${amount.toFixed(2)} ${account.currency}？` : `Confirm paper-only ${label} of ${amount.toFixed(2)} ${account.currency} for ${account.accountKey}?`)) return;
    fundSubmitLock.current = true;
    const fingerprint = fundFingerprint(account.id, form);
    const idempotencyKey = form.fingerprint === fingerprint && form.idempotencyKey ? form.idempotencyKey : makeIdempotencyKey();
    setFundForms((current) => ({ ...current, [account.id]: { ...form, idempotencyKey, fingerprint } }));
    setFundSubmitting(true);
    setError(null);
    setFundMessage(null);
    try {
      await paperTradingApi.adjustAccount(account.id, {
        adjustmentType: form.adjustmentType,
        amount,
        reason,
        idempotencyKey,
      });
      setFundForms((current) => ({ ...current, [account.id]: { adjustmentType: form.adjustmentType, amount: '', reason: '' } }));
      setPages((current) => ({ ...current, accounts: 1, adjustments: 1 }));
      setFundMessage(zh ? '模拟资金流水已记录。' : 'Paper fund adjustment recorded.');
      await load();
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      fundSubmitLock.current = false;
      setFundSubmitting(false);
    }
  };

  const updateRiskForm = (accountId: number, patch: Partial<RiskManualForm>) => {
    setRiskForms((current) => ({
      ...current,
      [accountId]: {
        ...(current[accountId] || { reason: '' }),
        ...patch,
        idempotencyKey: undefined,
        fingerprint: undefined,
      },
    }));
  };

  const submitManualRiskAction = async (account: PaperCapitalAccount, action: 'hold' | 'resume') => {
    if (isExperimentScope) return;
    const form = riskForms[account.id] || { reason: '' };
    const reason = form.reason.trim();
    if (!reason) {
      setRiskMessage(zh ? '人工风险操作必须填写原因。' : 'Manual risk actions require a reason.');
      return;
    }
    const label = action === 'hold' ? (zh ? '人工暂停新开仓/加仓' : 'manual hold') : (zh ? '人工恢复' : 'manual resume');
    if (!window.confirm(zh ? `确认对 ${account.accountKey} 执行${label}？` : `Confirm ${label} for ${account.accountKey}?`)) return;
    const fingerprint = riskManualFingerprint(account.id, reason, action);
    const idempotencyKey = form.fingerprint === fingerprint && form.idempotencyKey ? form.idempotencyKey : makeIdempotencyKey();
    setRiskForms((current) => ({ ...current, [account.id]: { ...form, idempotencyKey, fingerprint } }));
    setRunning(true);
    setError(null);
    setRiskMessage(null);
    try {
      if (action === 'hold') {
        await paperTradingApi.manualHold({ accountId: account.id, reason, idempotencyKey });
      } else {
        await paperTradingApi.manualResume({ accountId: account.id, reason, idempotencyKey });
      }
      setRiskMessage(action === 'hold' ? (zh ? '人工暂停事件已写入风险审计。' : 'Manual hold event recorded.') : (zh ? '人工恢复请求已写入风险审计。' : 'Manual resume request recorded.'));
      setRiskForms((current) => ({ ...current, [account.id]: { reason: '' } }));
      setPages((current) => ({ ...current, riskLocks: 1, riskEvents: 1, riskSnapshots: 1 }));
      await load();
    } catch (caught) {
      setError(getParsedApiError(caught));
    } finally {
      setRunning(false);
    }
  };

  return (
    <div className="mx-auto flex w-full max-w-[1680px] flex-col gap-5 px-4 py-6 sm:px-6 lg:px-8">
      <div className="flex flex-col gap-4 xl:flex-row xl:items-center xl:justify-between">
        <div>
          <div className="flex items-center gap-2">
            <FlaskConical className="h-6 w-6 text-primary" />
            <h1 className="text-2xl font-semibold text-foreground">{zh ? '混合模拟交易监控' : 'Hybrid Paper Trading Monitor'}</h1>
          </div>
          <p className="mt-2 text-sm text-secondary-text">
            {zh ? '按报告中的价格、放量、突破和均线条件监控；仅模拟，不连接券商。' : 'Monitor report-derived price, volume, breakout, and MA rules. Simulation only; no broker connection.'}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          {scopes.length > 1 ? (
            <select
              className="input-surface h-10 rounded-xl border bg-transparent px-3 text-sm"
              value={scopeId}
              onChange={(event) => selectScope(event.target.value)}
              aria-label={zh ? '执行范围（只读对照）' : 'Execution scope (read-only comparison)'}
            >
              {scopes.map((item) => <option key={item.scopeId} value={item.scopeId}>{item.label}</option>)}
            </select>
          ) : null}
          <select className="input-surface h-10 rounded-xl border bg-transparent px-3 text-sm" value={model} onChange={(event) => selectModel(event.target.value)}>
            <option value="">{zh ? '全部已配置模型' : 'All configured models'}</option>
            {models.map((item) => <option key={item} value={item}>{item}</option>)}
          </select>
          <button type="button" className="btn-secondary inline-flex h-10 items-center gap-2 px-4" onClick={() => void load()} disabled={loading}>
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />{zh ? '刷新' : 'Refresh'}
          </button>
          <button type="button" className="btn-primary inline-flex h-10 items-center gap-2 px-4" onClick={() => void runOnce('eod')} disabled={running || isExperimentScope}>
            <Play className="h-4 w-4" />{running ? (zh ? '运行中' : 'Running') : (zh ? '按收盘数据运行' : 'Run on daily bars')}
          </button>
          <button type="button" className="btn-secondary inline-flex h-10 items-center gap-2 px-4" onClick={() => void runOnce('intraday')} disabled={running || isExperimentScope}>
            <Play className="h-4 w-4" />{zh ? '按实时行情运行' : 'Run on realtime'}
          </button>
        </div>
      </div>

      <div className="rounded-2xl border border-warning/30 bg-warning/10 px-4 py-3 text-sm text-foreground">
        <strong>{zh ? '模拟环境：' : 'Paper environment: '}</strong>
        {zh ? '不会发送真实订单，也不会修改持仓页录入数据；盘后路径使用完整日 K，盘中路径使用实时行情与多因子确认。轮询可能错过两次检查之间的触价，模拟成交不代表券商可保证成交。' : 'No real order is sent, and manually entered portfolio holdings are not changed. EOD uses complete daily bars; intraday paper fills use realtime quotes plus multi-factor confirmation. Polling can miss touches between checks, and paper fills are not guaranteed broker fills.'}
      </div>
      {isExperimentScope ? (
        <div className="rounded-2xl border border-primary/30 bg-primary/10 px-4 py-3 text-sm text-foreground">
          <strong>{zh ? '只读对照：' : 'Read-only comparison: '}</strong>
          {zh
            ? `当前查看实验分组 ${activeScope?.label ?? scopeId} 的隔离账本，数据来自独立数据库，只读展示，不能在此运行监控或修改账户。切回“主账户”后可执行操作。`
            : `Viewing the isolated ledger for experiment arm ${activeScope?.label ?? scopeId} from a separate database. It is read-only; monitor runs and account mutations are disabled here. Switch back to the primary account to operate.`}
        </div>
      ) : null}
      {error ? <ApiErrorAlert error={error} actionLabel={zh ? '重试' : 'Retry'} onAction={() => void load()} /> : null}

      <PaperAccountOverview
        locale={zh ? 'zh' : 'en'}
        totals={accountTotalsByCurrency}
        riskSnapshotByCurrency={riskSnapshotByCurrency}
        experimentScope={isExperimentScope}
      />
      <PaperNoTradeFacts
        locale={zh ? 'zh' : 'en'}
        session={latestSession}
        openLots={openLots}
        openLotTotal={openLotRows.total}
        recentClosedLots={recentClosedLots}
        riskLockAccounts={mainViewCurrentRiskLocks}
        marketRisk={marketContextRisk}
        marketGateText={marketGateSummary}
        experimentScope={isExperimentScope}
      />
      <PaperLotsTable
        locale={zh ? 'zh' : 'en'}
        openLots={openLots}
        openTotal={openLotRows.total}
        closedLots={recentClosedLots}
        closedTotal={recentClosedLotRows.total}
        holdings={markHoldings}
      />
      <PaperPlansTable locale={zh ? 'zh' : 'en'} session={latestSession} />

      <div className="order-[5]">
      <Card className="order-[90]" title={zh ? '最近审计事件' : 'Recent audit events'}>
        <div className="space-y-2">{events.map((item) => {
          const dispatch = item.notificationDispatch ?? dispatchByEventId.get(item.id);
          const quantity = eventQuantity(item);
          return (
            <div key={item.id} className="rounded-xl bg-surface-2 px-3 py-2 text-sm">
              <div className="flex flex-wrap items-center justify-between gap-2">
                <span>{item.eventDate} · {item.stockName || item.stockCode} · {eventActionLabel(item.eventType, zh)}</span>
                <span className="text-secondary-text">{item.sourceModel} · {money(item.price)} · {item.reason}</span>
              </div>
              <div className="mt-2 flex flex-wrap items-center gap-2 text-xs text-secondary-text">
                <span>{zh ? '数量' : 'Qty'}: {money(quantity)}</span>
                <span>{zh ? '请求/执行/未成' : 'Req/exec/unfilled'}: {money(item.requestedQuantity)} / {money(item.executedQuantity)} / {money(item.unfilledQuantity)}</span>
                <span>{zh ? '计划' : 'Plan'}: #{item.planId ?? '--'}</span>
                <span>{zh ? '报告' : 'Report'}: #{item.sourceReportId}</span>
                <span>{zh ? '批次' : 'Lot'}: #{item.positionId ?? '--'}</span>
                <span>{zh ? '原因' : 'Reason'}: {item.reason || '--'}</span>
                <span>{zh ? '成交证据' : 'Fill evidence'}: {item.fillEvidenceGrade || '--'}</span>
                {item.fillEvidenceGrade === 'quote_based_unverified' ? <Badge variant="warning">{zh ? '仅行情价模拟，未验证盘口' : 'Quote-based unverified'}</Badge> : null}
                <span>{zh ? '订单键' : 'Order key'}: {item.logicalOrderKey || '--'}</span>
                <span>{zh ? '分配本金' : 'Allocated'}: {currencyMoney(item.allocatedCapital, item.currency)}</span>
                <span>{zh ? '剩余现金' : 'Cash after'}: {currencyMoney(item.cashAfter, item.currency)}</span>
                <span>{zh ? '敞口' : 'Exposure'}: {currencyMoney(item.exposureAfter, item.currency)}</span>
                <span>{zh ? '已实现盈亏' : 'Realized PnL'}: {currencyMoney(item.realizedPnlAfter, item.currency)}</span>
                <Badge variant={dispatch?.status === 'sent' ? 'success' : dispatch?.status === 'failed' ? 'danger' : dispatch ? 'warning' : 'default'}>
                  {dispatch ? `${dispatch.channel} ${dispatch.status} · ${zh ? '尝试' : 'attempts'} ${dispatch.attemptCount}` : (zh ? '查看投递记录' : 'See dispatch table')}
                </Badge>
                {dispatch?.sentAt ? <span>{zh ? '发送' : 'Sent'}: {formatChinaTime(dispatch.sentAt)}</span> : null}
                {dispatch?.lastError ? <span className="text-danger">{dispatch.lastError}</span> : null}
              </div>
            </div>
          );
        })}{events.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无事件' : 'No events'}</p> : null}</div>
        <Pager page={pages.events} total={totals.events} onPage={(page) => setPage('events', page)} zh={zh} />
      </Card>
      </div>

      <details className="group order-[6]">
        <summary className="flex cursor-pointer select-none items-center gap-2 px-4 py-3 text-sm font-semibold text-foreground">
          <ChevronDown className="h-4 w-4 shrink-0 text-secondary-text transition-transform group-open:rotate-180" aria-hidden="true" />
          <span>{zh ? '风控明细（风险锁、风险快照与容量、人工暂停 / 恢复、风险事件）' : 'Risk detail (locks, risk snapshots and capacity, manual hold / resume, risk events)'}</span>
        </summary>
        <div className="space-y-4 border-t border-border p-4">
        <Card className="order-[90]" title={zh ? '账户级风险控制' : 'Account-level risk control'} padding="none">
          <div className="border-b border-border px-4 py-3 text-xs text-secondary-text">
            {zh ? '权威口径：账户风险锁只拦截新开仓/加仓；止损、止盈、T+1、减仓和平仓仍按普通执行可行性继续。估值缺失/陈旧、硬阈值未恢复或人工暂停不得显示为健康。' : 'Authority: account risk locks block only new open/add; stops, targets, T+1 exits, reductions and closes continue under ordinary execution feasibility. Missing/stale valuation, unrecovered hard metrics, or manual holds must not render as healthy.'}
          </div>
          {['riskSnapshots', 'riskLocks', 'riskEvents'].map((key) => contractErrors[key] ? (
            <div key={key} className="border-b border-border bg-danger/10 px-4 py-3 text-sm text-danger">{dependencyText(key, zh)}<div className="mt-1 text-xs">{contractErrors[key]}</div></div>
          ) : null)}
          <div className="grid gap-4 p-4 xl:grid-cols-2">
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? '当前风险权限 / 锁' : 'Current risk permissions / locks'}</div>
              <div className="space-y-3 p-3">
                {currentRiskLocks.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无活动风险锁；若快照接口缺失或估值不完整，新开仓/加仓仍应 fail-closed。' : 'No active risk locks; if snapshot APIs are missing or valuation is incomplete, new open/add should still fail closed.'}</p> : null}
                {currentRiskLocks.map((item) => (
                  <div key={item.id} className="rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={statusVariant(item.state || item.status)}>{item.state || item.status || '--'}</Badge>
                      <span className="font-medium">{item.accountKey || `#${item.accountId}`}</span>
                      <span className="text-secondary-text">{item.sourceModel} · {item.market}/{item.currency}</span>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2 text-xs">
                      {permissionBadge(item.entryAllowed, zh ? '开仓' : 'Entry', zh)}
                      {permissionBadge(item.addAllowed, zh ? '加仓' : 'Add', zh)}
                      {permissionBadge(item.reduceAllowed ?? true, zh ? '减仓' : 'Reduce', zh)}
                      {permissionBadge(item.closeAllowed ?? true, zh ? '平仓' : 'Close', zh)}
                      <Badge variant={(item.exitsRemainEnabled ?? true) ? 'success' : 'danger'}>{zh ? '退出仍允许' : 'Exits remain enabled'}</Badge>
                    </div>
                    <div className="mt-2 grid gap-1 text-xs text-secondary-text md:grid-cols-2">
                      <span>{zh ? '原因' : 'Reasons'}: {(item.activeReasons || []).join(', ') || '--'}</span>
                      <span>{zh ? '恢复条件' : 'Recovery'}: {item.recoveryCondition || '--'}</span>
                      <span>{zh ? '熔断范围' : 'Scope'}: {item.haltScope || '--'}</span>
                      <span>{zh ? '冷却至' : 'Cooldown until'}: {formatChinaTime(item.cooldownUntil)}</span>
                      <span>{zh ? '指标/阈值' : 'Metric/threshold'}: {money(item.metric)} / {money(item.threshold)}</span>
                      <span>{zh ? '快照/事件' : 'Snapshot/event'}: #{item.snapshotId ?? '--'} / #{item.lastEventId ?? '--'}</span>
                    </div>
                  </div>
                ))}
              </div>
              <Pager page={pages.riskLocks} total={totals.riskLocks} onPage={(page) => setPage('riskLocks', page)} zh={zh} />
            </div>
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? '最新风险快照与容量' : 'Latest risk snapshots and capacity'}</div>
              <div className="space-y-3 p-3">
                {riskSnapshots.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无账户风险快照；不能把账户状态视为健康。' : 'No account risk snapshots; account state cannot be treated as healthy.'}</p> : null}
                {riskSnapshots.map((item) => (
                  <div key={item.id} className="rounded-lg border border-border bg-surface p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={item.valuationComplete === false || item.valuationStale ? 'danger' : 'success'}>{item.valuationComplete === false ? (zh ? '估值不完整' : 'Incomplete valuation') : item.valuationStale ? (zh ? '估值陈旧' : 'Stale valuation') : (zh ? '估值可用' : 'Valuation available')}</Badge>
                      <span className="font-medium">{item.accountKey || `#${item.accountId}`}</span>
                      <span className="text-secondary-text">{item.sourceModel} · {item.market}/{item.currency}</span>
                    </div>
                    <div className="mt-2 grid gap-1 text-xs text-secondary-text md:grid-cols-3">
                      <span>{zh ? '计算' : 'Computed'}: {formatChinaTime(item.computedAt || item.bucketStartedAt)}</span>
                      <span>{zh ? '日盈亏' : 'Daily PnL'}: {currencyMoney(item.dailyPnl, item.currency)} / {pct(item.dailyLossPct)}</span>
                      <span>{zh ? '回撤' : 'Drawdown'}: {pct(item.drawdownPct)}</span>
                      <span>{zh ? '连续亏损' : 'Loss streak'}: {item.losingStreak ?? '--'}</span>
                      <span>{zh ? '交易次数' : 'Trades'}: {item.dailyTradeCount ?? '--'}</span>
                      <span>{zh ? '开仓/加仓' : 'Open/add'}: {item.dailyOpenAddCount ?? '--'}</span>
                      <span>{zh ? '总敞口' : 'Total exposure'}: {pct(item.totalExposurePct)}</span>
                      <span>{zh ? '同股容量' : 'Symbol capacity'}: {pct(item.symbolUtilizationPct)}</span>
                      <span>{zh ? '行业容量' : 'Sector capacity'}: {pct(item.sectorUtilizationPct)}</span>
                      <span>{zh ? '相关性容量' : 'Correlation capacity'}: {pct(item.correlationUtilizationPct)}</span>
                      <span>{zh ? '未知桶容量' : 'Unknown capacity'}: {pct(item.unknownUtilizationPct)}</span>
                      <span>{zh ? '策略版本' : 'Policy'}: {item.policyVersion || '--'}</span>
                    </div>
                    <div className="mt-2 space-y-1 text-xs text-warning">
                      <div>{capacityText(item.unknownCapacity || asRecord(item.capacities).unknown, zh)}</div>
                      {(item.sectorCapacities || []).slice(0, 2).map((cap, index) => <div key={`sector-${index}`}>{zh ? '行业' : 'Sector'}: {capacityText(cap, zh)}</div>)}
                      {(item.correlationClusters || []).slice(0, 2).map((cap, index) => <div key={`corr-${index}`}>{zh ? '相关性' : 'Correlation'}: {capacityText(cap, zh)}</div>)}
                    </div>
                  </div>
                ))}
              </div>
              <Pager page={pages.riskSnapshots} total={totals.riskSnapshots} onPage={(page) => setPage('riskSnapshots', page)} zh={zh} />
            </div>
          </div>
          <div className="border-t border-border p-4">
            <div className="mb-3 text-sm font-semibold">{zh ? '人工暂停 / 恢复（纸面风险账本）' : 'Manual hold / resume (paper risk ledger)'}</div>
            {riskMessage ? <p className="mb-3 text-xs text-warning">{riskMessage}</p> : null}
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {accounts.map((account) => {
                const form = riskForms[account.id] || { reason: '' };
                const snapshot = riskSnapshotByAccountId.get(account.id);
                return (
                  <div key={account.id} className="rounded-xl border border-border bg-surface-2 p-3 text-sm">
                    <div className="font-medium">{account.accountKey}</div>
                    <div className="text-xs text-secondary-text">{account.sourceModel} · {account.market}/{account.currency} · snapshot #{snapshot?.id ?? '--'}</div>
                    <input className="input-surface mt-3 h-9 w-full rounded-lg border bg-transparent px-2 text-xs" placeholder={zh ? '人工操作原因（必填）' : 'Manual action reason (required)'} value={form.reason} onChange={(event) => updateRiskForm(account.id, { reason: event.target.value })} />
                    <div className="mt-2 grid grid-cols-2 gap-2">
                      <button type="button" className="btn-secondary h-9 px-3 text-xs" disabled={running} onClick={() => void submitManualRiskAction(account, 'hold')}>{zh ? '人工暂停' : 'Manual hold'}</button>
                      <button type="button" className="btn-primary h-9 px-3 text-xs" disabled={running} onClick={() => void submitManualRiskAction(account, 'resume')}>{zh ? '请求恢复' : 'Request resume'}</button>
                    </div>
                    <p className="mt-2 text-xs text-secondary-text">{zh ? '恢复不会覆盖未恢复的硬阈值；重复相同原因复用幂等键，冲突由后端 409 拒绝。' : 'Resume cannot override unrecovered hard metrics; identical retries reuse an idempotency key and conflicts are rejected by backend 409.'}</p>
                  </div>
                );
              })}
            </div>
          </div>
          <div className="border-t border-border px-4 py-3">
            <details>
              <summary className="cursor-pointer text-sm font-semibold text-primary">{zh ? '风险事件历史审计' : 'Risk event history audit'}</summary>
              <div className="mt-3 space-y-2">
                {riskEvents.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无风险事件。' : 'No risk events.'}</p> : null}
                {riskEvents.map((item) => (
                  <div key={item.id} className="rounded-lg bg-surface-2 px-3 py-2 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={statusVariant(item.newState || item.status || item.eventType)}>{label('eventType', item.eventType, (zh ? 'zh' : 'en'))}</Badge>
                      <span>{item.accountKey || `#${item.accountId}`}</span>
                      <span className="text-secondary-text">{item.oldState || '--'} → {item.newState || '--'} · {formatChinaTime(item.createdAt)}</span>
                    </div>
                    <div className="mt-1 text-xs text-secondary-text">{zh ? '原因' : 'Reason'}: {item.reason || (item.activeReasons || []).join(', ') || '--'} · {zh ? '恢复' : 'Recovery'}: {item.recoveryCondition || '--'} · {zh ? '幂等键' : 'Idempotency'}: {item.idempotencyKey || item.eventKey || '--'}</div>
                  </div>
                ))}
              </div>
              <Pager page={pages.riskEvents} total={totals.riskEvents} onPage={(page) => setPage('riskEvents', page)} zh={zh} />
            </details>
          </div>
        </Card>
        </div>
      </details>

      <details className="group order-[7]">
        <summary className="flex cursor-pointer select-none items-center gap-2 px-4 py-3 text-sm font-semibold text-foreground">
          <ChevronDown className="h-4 w-4 shrink-0 text-secondary-text transition-transform group-open:rotate-180" aria-hidden="true" />
          <span>{zh ? '资金与模型表现（入金/出金、资金流水、模型并排表现）' : 'Funds and model performance (deposits/withdrawals, journal, side-by-side models)'}</span>
        </summary>
        <div className="space-y-4 border-t border-border p-4">
        <Card className="order-[20]" title={zh ? '模拟资金管理' : 'Paper fund management'} padding="none">
          <div className="border-b border-border p-4">
            <div className="mb-3 flex items-center gap-2 text-sm font-semibold text-foreground">
              <WalletCards className="h-4 w-4 text-primary" />
              {zh ? '初始化模型资金账户' : 'Initialize model capital account'}
            </div>
            <div className="grid gap-3 text-sm md:grid-cols-[minmax(220px,1.4fr)_120px_160px_auto]">
              <select className="input-surface h-10 rounded-xl border bg-transparent px-3" value={newAccount.sourceModel} onChange={(event) => setNewAccount((current) => ({ ...current, sourceModel: event.target.value }))}>
                {models.map((item) => <option key={item} value={item}>{item}</option>)}
              </select>
              <select className="input-surface h-10 rounded-xl border bg-transparent px-3" value={newAccount.market} onChange={(event) => setNewAccount((current) => ({ ...current, market: event.target.value }))}>
                {markets.map((item) => <option key={item} value={item}>{item.toUpperCase()}</option>)}
              </select>
              <input className="input-surface h-10 rounded-xl border bg-transparent px-3" type="number" min="0" step="1000" value={newAccount.initialCapital} placeholder={zh ? '初始本金' : 'Initial capital'} onChange={(event) => setNewAccount((current) => ({ ...current, initialCapital: event.target.value }))} />
              <button type="button" className="btn-primary h-10 px-4" disabled={fundSubmitting || !models.length} onClick={() => void createCapitalAccount()}>
                {zh ? '初始化账户' : 'Initialize'}
              </button>
            </div>
            {fundMessage ? <p className="mt-2 text-xs text-warning">{fundMessage}</p> : null}
            <p className="mt-2 text-xs text-secondary-text">
              {zh ? '只管理模拟账户现金。入金/出金会写入不可变流水，不会修改已投入资金、持仓成本、敞口或已实现盈亏。' : 'Manages paper cash only. Deposits and withdrawals create immutable journal rows and never edit invested capital, position cost, exposure, or realized PnL.'}
            </p>
          </div>
          <div className="border-b border-border px-4 py-3 text-sm font-semibold text-foreground">
            {zh ? '资金账户' : 'Capital accounts'}
          </div>
          {accounts.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无资金账户' : 'No capital accounts'} description={zh ? '产生模拟成交后，会按模型、市场和币种建立独立资金账户。' : 'Paper fills create isolated accounts by model, market, and currency.'} /></div> : (
            <>
              <div className="grid gap-3 border-b border-border p-4 text-sm md:grid-cols-2 xl:grid-cols-3">
                {accountTotalsByCurrency.map((item) => (
                  <div key={item.currency} className="rounded-xl border border-border bg-surface-2 p-3">
                    <p className="text-xs font-semibold text-secondary-text">{zh ? '币种汇总' : 'Currency total'} · {item.currency}</p>
                    <div className="mt-2 grid grid-cols-3 gap-2 text-xs text-secondary-text">
                      <span>{zh ? '初始' : 'Initial'}<strong className="mt-1 block text-foreground">{currencyMoney(item.initialCapital, item.currency)}</strong></span>
                      <span>{zh ? '现金' : 'Cash'}<strong className="mt-1 block text-foreground">{currencyMoney(item.cash, item.currency)}</strong></span>
                      <span>{zh ? '成本口径权益' : 'Book equity'}<strong className="mt-1 block text-foreground">{currencyMoney(item.bookEquity, item.currency)}</strong></span>
                      <span>{zh ? '市值口径权益' : 'Market equity'}<strong className="mt-1 block text-foreground">{marketCurrencyMoney(item.marketEquity, item.currency, item.valuationComplete, zh)}</strong></span>
                      <span>{zh ? '持仓市值' : 'Market value'}<strong className="mt-1 block text-foreground">{marketCurrencyMoney(item.marketValue, item.currency, item.valuationComplete, zh)}</strong></span>
                      <span>{zh ? '已实现盈亏' : 'Realized PnL'}<strong className={`mt-1 block ${item.realizedPnl >= 0 ? 'text-success' : 'text-danger'}`}>{currencyMoney(item.realizedPnl, item.currency)}</strong></span>
                      <span>{zh ? '未实现盈亏' : 'Unrealized PnL'}<strong className={`mt-1 block ${(item.unrealizedPnl || 0) >= 0 ? 'text-success' : 'text-danger'}`}>{marketCurrencyMoney(item.unrealizedPnl, item.currency, item.valuationComplete, zh)}</strong></span>
                      <span>{zh ? '总盈亏' : 'Total PnL'}<strong className={`mt-1 block ${(item.totalPnl || 0) >= 0 ? 'text-success' : 'text-danger'}`}>{marketCurrencyMoney(item.totalPnl, item.currency, item.valuationComplete, zh)}</strong></span>
                      <span>{zh ? '持仓批次' : 'Open lots'}<strong className="mt-1 block text-foreground">{item.openPositionCount}</strong></span>
                    </div>
                    {!item.valuationComplete ? <p className="mt-2 text-xs text-warning">{unpricedValuationText(item.unpricedHoldingCount, item.unpricedPositionCount, zh)}</p> : null}
                    {valuationWarning(item.valuationStale, item.valuationObservedAt, zh) ? <p className="mt-1 text-xs text-warning">{valuationWarning(item.valuationStale, item.valuationObservedAt, zh)}</p> : null}
                  </div>
                ))}
              </div>
              <div className="overflow-x-auto"><table className="w-full min-w-[1680px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '模型 / 账户' : 'Model / account'}</th><th>{zh ? '市场 / 币种' : 'Market / currency'}</th><th>{zh ? '初始 / 外部流水' : 'Initial / flows'}</th><th>{zh ? '可用现金' : 'Cash'}</th><th>{zh ? '权益口径' : 'Equity bases'}</th><th>{zh ? '盈亏拆分' : 'PnL split'}</th><th>{zh ? '持仓市值 / 估值状态' : 'Market value / valuation'}</th><th>{zh ? '敞口 / 持仓批次' : 'Exposure / open lots'}</th><th>{zh ? '风控上限' : 'Risk limits'}</th><th>{zh ? '资金调整' : 'Fund adjustment'}</th><th>{zh ? '更新时间' : 'Updated'}</th></tr></thead><tbody>{accounts.map((item) => {
                const form = fundForms[item.id] || { adjustmentType: 'deposit' as const, amount: '', reason: '' };
                return <tr key={item.id} className="border-b border-border/60 align-top"><td className="p-4 font-medium">{item.sourceModel}<div className="text-xs text-secondary-text">{item.accountKey}</div></td><td>{item.market}<div className="text-xs text-secondary-text">{item.currency}</div></td><td>{currencyMoney(item.initialCapital, item.currency)}<div className="text-xs text-secondary-text">{zh ? '外部净流入' : 'Net external'} {currencyMoney(item.externalCashFlow, item.currency)}</div></td><td>{currencyMoney(item.cash, item.currency)}</td><td>{zh ? '成本口径权益' : 'Book equity'} {currencyMoney(item.bookEquity ?? item.cash + item.investedCapital, item.currency)}<div className="text-xs text-secondary-text">{zh ? '市值口径权益' : 'Market equity'} {marketCurrencyMoney(item.marketEquity, item.currency, item.valuationComplete, zh)}</div><div className="text-xs text-secondary-text">{zh ? '已投入' : 'Invested'} {currencyMoney(item.investedCapital, item.currency)}</div></td><td><div className={(item.realizedPnl || 0) >= 0 ? 'text-success' : 'text-danger'}>{zh ? '已实现盈亏' : 'Realized PnL'} {currencyMoney(item.realizedPnl, item.currency)}</div><div className={(item.unrealizedPnl || 0) >= 0 ? 'text-success' : 'text-danger'}>{zh ? '未实现盈亏' : 'Unrealized PnL'} {marketCurrencyMoney(item.unrealizedPnl, item.currency, item.valuationComplete, zh)}</div><div className={(item.totalPnl || 0) >= 0 ? 'text-success' : 'text-danger'}>{zh ? '总盈亏' : 'Total PnL'} {marketCurrencyMoney(item.totalPnl, item.currency, item.valuationComplete, zh)}</div></td><td>{zh ? '持仓市值' : 'Market value'} {marketCurrencyMoney(item.marketValue, item.currency, item.valuationComplete, zh)}{!item.valuationComplete ? <div className="text-xs text-warning">{unpricedValuationText(item.unpricedHoldingCount, item.unpricedPositionCount, zh)}</div> : null}{valuationWarning(item.valuationStale, item.valuationObservedAt, zh) ? <div className="text-xs text-warning">{valuationWarning(item.valuationStale, item.valuationObservedAt, zh)}</div> : <div className="text-xs text-secondary-text">{formatChinaTime(item.valuationObservedAt)}</div>}</td><td>{currencyMoney(item.exposure, item.currency)}<div className="text-xs text-secondary-text">{zh ? '持仓批次' : 'Open lots'} {item.openPositionCount}</div></td><td><div className="text-xs text-secondary-text">{zh ? '新分批计划上限' : 'New staged-plan cap'}: {currencyMoney(item.maxPositionCapital, item.currency)} / {pct(item.maxPositionPct)}</div><div className="text-xs text-secondary-text">{zh ? '总账户上限' : 'Total account cap'}: {currencyMoney(item.maxTotalExposure, item.currency)} / {pct(item.maxTotalExposurePct)}</div><div className="text-xs text-secondary-text">{zh ? '账户硬约束' : 'Account hard limits'}: {zh ? '现金 / 总敞口 / 最大持仓数' : 'cash / total exposure / max open holdings'}</div><div className="text-xs text-secondary-text">{zh ? '单笔风险' : 'Risk/trade'}: {pct(item.riskPerTradePct)} · {zh ? '最多' : 'Max'} {item.maxOpenPositions ?? '--'}</div></td><td className="min-w-[260px] pr-4"><div className="grid grid-cols-[96px_1fr] gap-2"><select className="input-surface h-9 rounded-lg border bg-transparent px-2 text-xs" value={form.adjustmentType} onChange={(event) => updateFundForm(item.id, { adjustmentType: event.target.value as 'deposit' | 'withdrawal' })}><option value="deposit">{zh ? '入金' : 'Deposit'}</option><option value="withdrawal">{zh ? '出金' : 'Withdrawal'}</option></select><input className="input-surface h-9 rounded-lg border bg-transparent px-2 text-xs" type="number" min="0" step="100" placeholder={zh ? '金额' : 'Amount'} value={form.amount} onChange={(event) => updateFundForm(item.id, { amount: event.target.value })} /><input className="input-surface col-span-2 h-9 rounded-lg border bg-transparent px-2 text-xs" placeholder={zh ? '原因（必填）' : 'Reason required'} value={form.reason} onChange={(event) => updateFundForm(item.id, { reason: event.target.value })} /><button type="button" className="btn-secondary col-span-2 h-9 px-3 text-xs" disabled={fundSubmitting} onClick={() => void submitFundAdjustment(item)}>{zh ? '记录模拟流水' : 'Record paper flow'}</button></div></td><td>{formatChinaTime(item.updatedAt)}</td></tr>;
              })}</tbody></table></div>
            </>
          )}
          <Pager page={pages.accounts} total={totals.accounts} onPage={(page) => setPage('accounts', page)} zh={zh} />
        </Card>

        <Card className="order-[30]" title={zh ? '资金调整流水' : 'Fund adjustment journal'} padding="none">
          {adjustments.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无资金流水' : 'No fund adjustments'} description={zh ? '手动入金或出金后，这里会保留不可变审计记录。' : 'Manual deposits and withdrawals are kept here as immutable audit records.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1080px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '时间' : 'Time'}</th><th>{zh ? '账户' : 'Account'}</th><th>{zh ? '类型 / 金额' : 'Type / amount'}</th><th>{zh ? '现金变化' : 'Cash change'}</th><th>{zh ? '账面权益变化' : 'Book equity change'}</th><th>{zh ? '原因' : 'Reason'}</th><th>{zh ? '幂等键' : 'Idempotency key'}</th></tr></thead><tbody>{adjustments.map((item) => <tr key={item.id} className="border-b border-border/60 align-top"><td className="p-4">{formatChinaTime(item.createdAt)}</td><td>{item.sourceModel}<div className="text-xs text-secondary-text">{item.market}/{item.currency} · #{item.accountId}</div></td><td><Badge variant={item.adjustmentType === 'deposit' ? 'success' : 'warning'}>{item.adjustmentType === 'deposit' ? (zh ? '入金' : 'Deposit') : (zh ? '出金' : 'Withdrawal')}</Badge><div className="mt-1 font-medium">{currencyMoney(item.amount, item.currency)}</div></td><td>{currencyMoney(item.beforeCash, item.currency)} → {currencyMoney(item.afterCash, item.currency)}</td><td>{currencyMoney(item.beforeBookEquity, item.currency)} → {currencyMoney(item.afterBookEquity, item.currency)}</td><td className="max-w-[280px]">{item.reason || '--'}</td><td className="max-w-[260px] break-all text-xs text-secondary-text">{item.idempotencyKey}</td></tr>)}</tbody></table></div>
          )}
          <Pager page={pages.adjustments} total={totals.adjustments} onPage={(page) => setPage('adjustments', page)} zh={zh} />
        </Card>

        {!model && Array.isArray(performance.models) && performance.models.length > 0 ? (
          <Card className="order-[40]" title={zh ? '模型模拟表现并排比较' : 'Side-by-side model performance'}>
            <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
              {performance.models.map((item) => (
                <div key={item.model} className="rounded-xl border border-border bg-surface-2 p-4">
                  <p className="truncate text-sm font-semibold">{item.model}</p>
                  <div className="mt-3 grid grid-cols-3 gap-2 text-xs text-secondary-text">
                    <span>{zh ? '已平仓批次' : 'Closed lots'}<strong className="mt-1 block text-base text-foreground">{item.closedPositions || 0}</strong></span>
                    <span>{zh ? '胜率' : 'Win rate'}<strong className="mt-1 block text-base text-foreground">{pct(item.winRatePct)}</strong></span>
                    <span>{zh ? '收益' : 'Return'}<strong className="mt-1 block text-base text-foreground">{pct(item.realizedReturnPct)}</strong></span>
                    <span>{zh ? '已实现' : 'Realized'}<strong className="mt-1 block text-base text-foreground">{currencyMoney(item.realizedPnl, undefined)}</strong></span>
                    <span>{zh ? '未实现' : 'Unrealized'}<strong className="mt-1 block text-base text-foreground">{item.valuationComplete === false ? '--' : currencyMoney(item.unrealizedPnl, undefined)}</strong></span>
                    <span>{zh ? '总盈亏' : 'Total PnL'}<strong className="mt-1 block text-base text-foreground">{item.valuationComplete === false ? '--' : currencyMoney(item.totalPnl, undefined)}</strong></span>
                    <span>{zh ? '显式成本' : 'Explicit costs'}<strong className="mt-1 block text-base text-foreground">{currencyMoney(item.explicitCostTotal ?? item.totalFees, undefined)}</strong></span>
                    <span>{zh ? '佣金/税' : 'Commission/tax'}<strong className="mt-1 block text-base text-foreground">{currencyMoney(item.totalCommission, undefined)} / {currencyMoney(item.totalSellTax, undefined)}</strong></span>
                    <span>{zh ? '滑点' : 'Slippage'}<strong className="mt-1 block text-base text-foreground">{currencyMoney(item.totalSlippageAmount, undefined)}</strong></span>
                  </div>
                </div>
              ))}
            </div>
          </Card>
        ) : null}
        </div>
      </details>

      <details className="group order-[8]">
        <summary className="flex cursor-pointer select-none items-center gap-2 px-4 py-3 text-sm font-semibold text-foreground">
          <ChevronDown className="h-4 w-4 shrink-0 text-secondary-text transition-transform group-open:rotate-180" aria-hidden="true" />
          <span>{zh ? '运行诊断（调度心跳、开盘确认、盘中监控、报告→计划、意图解析、风险修订、通知、规则证据、冻结策略）' : 'Runtime diagnostics (scheduler, opening confirmation, intraday monitor, report-to-plan, intents, risk revisions, notifications, rule evidence, frozen strategy)'}</span>
        </summary>
        <div className="space-y-4 border-t border-border p-4">
        <Card className="order-[80]" title={zh ? '运行与调度审计' : 'Runtime and scheduler audit'}>
          <div className="grid gap-3 text-sm md:grid-cols-5">
            <div>
              <p className="text-xs text-secondary-text">{zh ? '模拟交易' : 'Paper trading'}</p>
              <Badge variant={status?.enabled ? 'success' : 'default'}>{status?.enabled ? (zh ? '已启用' : 'Enabled') : (zh ? '未启用' : 'Disabled')}</Badge>
            </div>
            <div>
              <p className="text-xs text-secondary-text">{zh ? '盘中轮询配置' : 'Intraday config'}</p>
              <Badge variant={status?.enabled && status?.intradayEnabled ? 'success' : 'warning'}>
                {status?.enabled && status?.intradayEnabled ? (zh ? '已配置' : 'Configured') : (zh ? '未启用' : 'Disabled')}
              </Badge>
            </div>
            <div>
              <p className="text-xs text-secondary-text">{zh ? '调度心跳' : 'Scheduler heartbeat'}</p>
              <p className="mt-1 font-medium text-foreground">
                {status ? `${status.schedulerRunning ? (zh ? '运行' : 'Running') : (zh ? '未接管' : 'Not owner')} · ${status.intervalMinutes}m` : '--'}
              </p>
              {status?.schedulerOwnerKind || status?.schedulerLastSeenAt ? (
                <p className="mt-1 text-xs text-secondary-text">
                  {status.schedulerOwnerKind || status.localSchedulerOwner || '--'} · {status.schedulerHeartbeatFresh ? (zh ? '新鲜' : 'fresh') : (zh ? '陈旧/缺失' : 'stale/missing')} · {formatChinaTime(status.schedulerLastSeenAt)}
                </p>
              ) : null}
            </div>
            <div>
              <p className="text-xs text-secondary-text">{zh ? '共享调度心跳' : 'Shared scheduler heartbeat'}</p>
              <p className="mt-1 break-words font-medium text-foreground">
                {status ? (status.schedulerHeartbeatFresh ? (zh ? '新鲜' : 'Fresh') : (zh ? '陈旧/缺失' : 'Stale/missing')) : '--'}
              </p>
              {status?.schedulerOwnerKind ? (
                <p className="mt-1 break-words text-[11px] text-secondary-text">
                  {status.schedulerOwnerKind} · {formatChinaTime(status.schedulerLastSeenAt)}
                </p>
              ) : null}
            </div>
            <div>
              <p className="text-xs text-secondary-text">{zh ? '最近观察桶' : 'Latest observation bucket'}</p>
              <p className="mt-1 break-words font-medium text-foreground">{formatChinaTime(status?.latestObservationBucket || '')}</p>
              <p className="mt-1 text-xs text-secondary-text">
                {zh ? '只代表已写入 observation 的时间' : 'Only persisted observation time'}
              </p>
            </div>
          </div>
          {hasIntradayRunSummary ? (
            <div className="mt-4 rounded-xl border border-border bg-surface-2 p-3 text-xs text-secondary-text">
              <div className="font-semibold text-foreground">{zh ? '盘中监控最近实跑摘要' : 'Intraday monitor last actual run'}</div>
              <div className="mt-2 grid gap-2 md:grid-cols-6">
                <span>{zh ? '完成' : 'Finished'}: {formatChinaTime(String(intradayRunSummary.finishedAt || ''))}</span>
                <span>{zh ? '耗时' : 'Duration'}: {formatNumber(intradayRunSummary.durationSeconds, 's')}</span>
                <span>{zh ? '目标' : 'Targets'}: {formatNumber(intradayRunSummary.targetIdentityCount)}</span>
                <span>{zh ? '报价成功' : 'Quote successes'}: {formatNumber(intradayRunSummary.quoteFetchSuccesses)}</span>
                <span>{zh ? '跳过/闭市' : 'Skipped/closed'}: {formatNumber(intradayRunSummary.skippedClosedMarket)}</span>
                <span>{zh ? '失败' : 'Failures'}: {formatNumber(intradayRunSummary.failedClosed)}</span>
              </div>
            </div>
          ) : null}
          <div className="mt-4 grid gap-4 xl:grid-cols-2">
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">
                {zh ? '共享数据库调度任务' : 'Shared database scheduler tasks'}
              </div>
              <div className="overflow-x-auto">
                <table className="w-full min-w-[760px] text-left text-xs">
                  <thead className="border-b border-border text-secondary-text">
                    <tr>
                      <th className="p-3">{zh ? '任务' : 'Task'}</th>
                      <th>{zh ? '心跳' : 'Heartbeat'}</th>
                      <th>{zh ? '租约' : 'Lease'}</th>
                      <th>{zh ? '最近成功 / 错误' : 'Last success / error'}</th>
                      <th>{zh ? '最近实跑摘要' : 'Last run summary'}</th>
                      <th>{zh ? '错误' : 'Error'}</th>
                    </tr>
                  </thead>
                  <tbody>
                    {schedulerRows.map((item) => (
                      <tr key={item.name} className="border-b border-border/60 align-top">
                        <td className="p-3 font-medium">{item.name}</td>
                        <td>
                          <Badge variant={item.heartbeat.fresh ? 'success' : 'danger'}>
                            {item.heartbeat.fresh ? (zh ? '新鲜' : 'fresh') : (zh ? '陈旧/缺失' : 'stale/missing')}
                          </Badge>
                          <div className="mt-1 text-secondary-text">{item.heartbeat.status ? String(item.heartbeat.status) : '--'} · age {formatNumber(item.heartbeat.ageSeconds, 's')}</div>
                          <div className="text-secondary-text">{formatChinaTime(String(item.heartbeat.observedAt || ''))}</div>
                        </td>
                        <td>
                          <Badge variant={item.lease.active ? 'success' : item.lease.status ? 'warning' : 'default'}>
                            {item.lease.active ? (zh ? '已接管' : 'active owner') : String(item.lease.status || (zh ? '无活动租约' : 'no active lease'))}
                          </Badge>
                          <div className="mt-1 break-all text-secondary-text">{String(item.lease.ownerKind || item.heartbeat.ownerKind || '--')} · {String(item.lease.ownerId || item.heartbeat.ownerId || '--')}</div>
                          <div className="text-secondary-text">{zh ? '到期' : 'until'} {formatChinaTime(String(item.lease.leaseUntil || ''))}</div>
                        </td>
                        <td>
                          <div>{formatChinaTime(String(item.heartbeat.lastSuccessAt || item.lease.finishedAt || ''))}</div>
                          <div className="text-secondary-text">{formatChinaTime(String(item.heartbeat.lastErrorAt || ''))}</div>
                        </td>
                        <td className="max-w-[240px] break-words text-secondary-text">
                          {Object.keys(item.summary).length ? (
                            <div>
                              <div>{String(item.summary.status || '--')} · {formatChinaTime(String(item.summary.finishedAt || ''))}</div>
                              <div>{zh ? '耗时' : 'duration'} {formatNumber(item.summary.durationSeconds, 's')}</div>
                              <div>{zh ? '目标' : 'targets'} {formatNumber(item.summary.targetIdentityCount)} · {zh ? '报价' : 'quotes'} {formatNumber(item.summary.quoteFetchSuccesses)} / {formatNumber(item.summary.quoteFetchAttempts)}</div>
                              <div>{zh ? '观察' : 'obs'} {formatNumber(item.summary.observations)} · {zh ? '跳过/闭市' : 'closed skips'} {formatNumber(item.summary.skippedClosedMarket)} · {zh ? '失败' : 'failed'} {formatNumber(item.summary.failedClosed)}</div>
                            </div>
                          ) : '--'}
                        </td>
                        <td className="max-w-[220px] break-words text-danger">{String(item.heartbeat.errorMessage || item.lease.lastError || '--')}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              {schedulerRows.length === 0 ? (
                <p className="px-3 py-3 text-xs text-danger">
                  {zh ? 'status 未返回 schedulerTaskStates；不能用 API 进程本地状态判断跨容器调度健康，新开仓/加仓应视为无法确认。' : 'status did not return schedulerTaskStates; API-local state cannot prove cross-container scheduler health, so new open/add health is unconfirmed.'}
                </p>
              ) : null}
              <p className="border-t border-border px-3 py-2 text-xs text-secondary-text">
                {zh ? '本地 API 调度器：' : 'Local API scheduler: '}
                {boolText(status?.localSchedulerEnabled, zh)} / {boolText(status?.localSchedulerRunning, zh)}
                {zh ? '；仅作本进程附加信息，不代表共享运行状态。' : '; this is process-local context only, not shared runtime truth.'}
              </p>
            </div>
            <div className="rounded-xl border border-border bg-surface-2 p-3">
              <div className="flex flex-wrap items-center gap-2">
                <span className="text-sm font-semibold">{zh ? '策略控制面' : 'Strategy controls'}</span>
                <Badge variant={status?.planCompensationEnabled ? 'success' : 'warning'}>{zh ? '报告->计划补偿' : 'Report-to-plan compensation'} {status?.planCompensationEnabled ? (zh ? '开启' : 'on') : (zh ? '关闭' : 'off')}</Badge>
                <Badge variant={status?.marketContextRiskControlEnabled ? 'success' : 'warning'}>{zh ? '大盘硬风控' : 'Market hard gate'} {status?.marketContextRiskControlEnabled ? (zh ? '开启' : 'on') : (zh ? '关闭' : 'off')}</Badge>
              </div>
              <div className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
                {styleRows.map((item) => (
                  <div key={item.label} className="rounded-lg bg-surface px-2 py-2">
                    <span className="text-secondary-text">{item.label}</span>
                    <strong className="mt-1 block text-foreground">{String(item.value)}</strong>
                  </div>
                ))}
              </div>
              <p className="mt-3 text-xs text-secondary-text">
                {zh
                  ? `补偿回看：${status?.planCompensationLookbackDays ?? '--'} 天；当前后端没有独立补偿队列列表 endpoint，页面只能展示启用状态、回看窗口，以及 reanalysis/dispatch 失败。`
                  : `Compensation lookback: ${status?.planCompensationLookbackDays ?? '--'} day(s). There is no separate compensation-queue list endpoint yet, so the page shows enablement, lookback, and visible reanalysis/dispatch failures only.`}
              </p>
              <p className="mt-2 text-xs text-secondary-text">
                {zh
                  ? `费用假设：滑点 ${pct(status?.slippagePct)}，佣金 ${pct(status?.commissionRate)}，最低佣金 ${money(status?.minCommission)}，CN 股票卖出印花税 ${pct(status?.sellTaxRate)}；ETF/基金卖出印花税按 0 处理。佣金、最低佣金、滑点均为用户模拟假设，不是券商或税务承诺。`
                  : `Cost assumptions: slippage ${pct(status?.slippagePct)}, commission ${pct(status?.commissionRate)}, minimum commission ${money(status?.minCommission)}, CN stock sell stamp tax ${pct(status?.sellTaxRate)}; ETF/fund sells use zero stamp tax. Commission, minimum commission, and slippage are user assumptions, not broker or tax claims.`}
              </p>
              <div className="mt-3 rounded-lg border border-border bg-surface p-3 text-xs">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="font-semibold">{zh ? '当前大盘执行风控' : 'Current market execution gate'}</span>
                  <Badge variant={marketContextRisk.fresh ? 'success' : 'danger'}>{marketContextRisk.fresh ? (zh ? '新鲜' : 'fresh') : (zh ? '陈旧/缺失' : 'stale/missing')}</Badge>
                  <Badge variant={marketContextRisk.entryAllowed ? 'success' : 'danger'}>{zh ? '新开/加仓' : 'open/add'} {marketContextRisk.entryAllowed ? (zh ? '允许' : 'allowed') : (zh ? '阻塞' : 'blocked')}</Badge>
                  <Badge variant={marketContextRisk.blockNewRisk ? 'danger' : 'success'}>{marketContextRisk.blockNewRisk ? (zh ? 'fail-closed' : 'fail-closed') : (zh ? '可进入' : 'entry possible')}</Badge>
                </div>
                <div className="mt-2 grid gap-1 text-secondary-text sm:grid-cols-2">
                  <span>{zh ? 'regime' : 'regime'}: {String(marketContextRisk.regime || '--')}</span>
                  <span>{zh ? '倍率' : 'multiplier'}: {formatNumber(marketContextRisk.allocationMultiplier)}</span>
                  <span>{zh ? '市场 / session' : 'market / session'}: {String(marketContextRisk.market || marketContextScope.market || '--')} / {String(marketContextRisk.sessionDate || '--')}</span>
                  <span>{zh ? '来源' : 'source'}: {String(marketContextRisk.source || marketContextScope.source || '--')}</span>
                  <span>{zh ? '报告 / context' : 'report / context'}: #{String(marketContextRisk.sourceReportId ?? '--')} / #{String(marketRiskId(marketContextRisk) ?? '--')}</span>
                  <span>{zh ? '观察' : 'observed'}: {formatChinaTime(String(marketContextRisk.observedAt || ''))}</span>
                  <span>{zh ? '策略版本' : 'policy'}: {String(marketContextRisk.policyVersion || '--')}</span>
                  <span>{zh ? '原因' : 'reasons'}: {marketContextReasons.length ? marketContextReasons.join(', ') : '--'}</span>
                </div>
              </div>
            </div>
          </div>
          <p className="mt-3 text-xs text-secondary-text">
            {status ? (zh
              ? `自动开仓阈值：交易风格 ${status.tradingStyle ?? 'position'}，至少 ${status.minFactorDomains} 个市场因子通过，综合分不低于 ${pct(status.minCompositeScore * 100)}，盈亏比不低于 ${status.minRiskReward.toFixed(2)}，行情陈旧上限 ${status.maxQuoteStaleSeconds}s；报告补偿 ${status.planCompensationEnabled ? `开启，回看 ${status.planCompensationLookbackDays ?? '--'} 天` : '关闭'}。`
              : `Entry policy: style ${status.tradingStyle ?? 'position'}, at least ${status.minFactorDomains} market factors, composite score >= ${pct(status.minCompositeScore * 100)}, reward/risk >= ${status.minRiskReward.toFixed(2)}, quote staleness <= ${status.maxQuoteStaleSeconds}s; report compensation ${status.planCompensationEnabled ? `on, ${status.planCompensationLookbackDays ?? '--'}d lookback` : 'off'}.`
            ) : '--'}
          </p>
          <p className="mt-2 text-xs text-secondary-text">
            {status ? (zh
              ? `资金配置：初始本金 ${money(status.initialCapital)}，单笔风险 ${pct(status.riskPerTradePct)}，分批计划上限 ${pct(status.maxPositionPct)}，全局单标的上限 ${pct(status.maxSymbolExposurePct)}，资产有效上限 ${assetExposureCapsText(status, zh)}，总账户上限 ${pct(status.maxTotalExposurePct)}，最多持仓 ${status.maxOpenPositions ?? '--'}；大盘报告硬风控 ${status.marketContextRiskControlEnabled ? `开启，回看 ${status.marketContextLookbackDays ?? '--'} 个已完成交易日` : '关闭'}；飞书模拟成交通知 ${status.feishuNotifyEnabled ? '已启用' : '未启用'}，路由 ${status.feishuRouteConfigured ? '已配置' : '未配置'}。`
              : `Capital config: initial ${money(status.initialCapital)}, risk/trade ${pct(status.riskPerTradePct)}, staged-plan cap ${pct(status.maxPositionPct)}, global symbol cap ${pct(status.maxSymbolExposurePct)}, effective asset caps ${assetExposureCapsText(status, zh)}, total account cap ${pct(status.maxTotalExposurePct)}, max open ${status.maxOpenPositions ?? '--'}; market-report hard risk control ${status.marketContextRiskControlEnabled ? `on, ${status.marketContextLookbackDays ?? '--'} completed-session lookback` : 'off'}; Feishu paper-fill notifications ${status.feishuNotifyEnabled ? 'enabled' : 'disabled'}, route ${status.feishuRouteConfigured ? 'configured' : 'not configured'}.`
            ) : '--'}
          </p>
          <p className="mt-2 text-xs text-secondary-text">
            {zh ? '提示：如果项目以 web/analyse 双容器运行，盘中轮询可能由 analyse 容器接管；上方调度心跳来自共享数据库，本 API 进程状态只作为本地附加信息。' : 'Note: in a web/analyse two-container deployment, intraday polling may be owned by the analyse container. The scheduler heartbeat above comes from the shared database; this API process state is local-only context.'}
          </p>
          <p className="mt-2 text-xs text-secondary-text">
            {zh ? '本页面在前台可见时每 60 秒静默刷新一次，切回页面时会立即同步最新监控结果。' : 'While visible, this page refreshes silently every 60 seconds and syncs immediately when you return to it.'}
          </p>
          <p className="mt-2 text-xs text-secondary-text">
            {zh
              ? `A 股执行窗口：09:30–11:30、13:00–14:57（北京时间）。调度器约每 ${status?.intervalMinutes ?? 5} 分钟唤醒，但盘前、午休、14:57–15:00 收盘集合竞价及闭市期间会跳过，不获取行情、不产生模拟成交。`
              : `A-share execution windows: 09:30–11:30 and 13:00–14:57 China time. The scheduler wakes roughly every ${status?.intervalMinutes ?? 5} minute(s), but skips quote retrieval and paper fills during premarket, lunch, the 14:57–15:00 closing auction, and after close.`}
          </p>
        </Card>

        <Card className="order-[80]" title={zh ? '开盘确认与策略刷新控制' : 'Opening confirmation and strategy refresh'} padding="none">
          <div className="border-b border-border px-4 py-3 text-xs text-secondary-text">
            {zh
              ? '权威口径：09:30–09:35 使用现有行情源返回的可归属、新鲜开盘报价确认昨日计划；这不是集合竞价委托簿或买卖不平衡数据。确认任务只记录证据或触发同模型重分析，不直接成交；缺失、陈旧、身份错误或失败重分析均阻止新开仓/加仓，但不阻止已有持仓止损、止盈、T+1、减仓或卖出。'
              : 'Authority: from 09:30 to 09:35 the system confirms prior-session plans with a fresh, attributable opening quote from the configured realtime providers. This is not auction order-book or imbalance data. The confirmation sweep records evidence or requests same-model reanalysis and never fills by itself; missing, stale, wrong-identity, or failed reanalysis states block new open/add while exits remain enabled.'}
          </div>
          {contractErrors.auctionObservations ? <div className="border-b border-border bg-danger/10 px-4 py-3 text-sm text-danger">{dependencyText(zh ? '开盘确认观察' : 'Opening-confirmation observations', zh)}<div className="mt-1 text-xs">{contractErrors.auctionObservations}</div></div> : null}
          {contractErrors.reanalysisRequests ? <div className="border-b border-border bg-danger/10 px-4 py-3 text-sm text-danger">{dependencyText(zh ? '重分析请求' : 'Reanalysis requests', zh)}<div className="mt-1 text-xs">{contractErrors.reanalysisRequests}</div></div> : null}
          <div className="grid items-start gap-4 p-4 xl:grid-cols-2">
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? '今天未解决的开盘确认/重分析阻塞' : 'Today unresolved opening-confirmation/reanalysis blockers'}</div>
              <div data-testid="opening-blocker-list" className="max-h-80 space-y-2 overflow-y-auto overscroll-contain p-3">
                {currentReanalysisRequests.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无未解决重分析；若后端契约缺失，上方会显示 fail-closed。' : 'No unresolved reanalysis; backend contract gaps are shown fail-closed above.'}</p> : null}
                {currentReanalysisRequests.map((item) => (
                  <div key={String(item.id)} className="rounded-lg border border-warning/40 bg-warning/10 p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={statusVariant(item.status)}>{item.status || '--'}</Badge>
                      <span className="font-medium">{item.stockName || item.stockCode || '--'}</span>
                      <span className="text-secondary-text">{item.sourceModel || '--'} · {item.market || '--'} · {item.triggerType || '--'}</span>
                    </div>
                    <div className="mt-2 grid gap-1 text-xs text-secondary-text md:grid-cols-2">
                      <span>{zh ? '请求键' : 'Request key'}: {item.requestKey || item.id || '--'}</span>
                      <span>{zh ? '会话' : 'Session'}: {item.sessionDate || '--'}</span>
                      <span>{zh ? '到期' : 'Due'}: {formatChinaTime(item.dueAt)}</span>
                      <span>{zh ? '观察' : 'Observed'}: {formatChinaTime(item.observedAt)}</span>
                      <span>{zh ? '提交' : 'Submitted'}: {formatChinaTime(item.submittedAt)}</span>
                      <span>{zh ? '任务' : 'Task'}: {item.taskId || item.queryId || '--'}</span>
                      <span>{zh ? '替代报告/计划' : 'Replacement report/plan'}: #{item.replacementReportId ?? '--'} / #{item.replacementPlanId ?? '--'}</span>
                    </div>
                    <p className="mt-2 text-xs text-danger">{zh ? '新开仓/加仓保持阻塞；退出仍允许。' : 'New open/add remains blocked; exits remain enabled.'}</p>
                    {item.failureReason ? <p className="mt-1 text-xs text-danger">{item.failureReason}</p> : null}
                  </div>
                ))}
              </div>
            </div>
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? '开盘确认观察证据' : 'Opening-confirmation evidence'}</div>
              <div data-testid="opening-evidence-list" className="max-h-80 space-y-2 overflow-y-auto overscroll-contain p-3">
                {auctionObservations.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无开盘确认观察。系统不会把盘前报价或陈旧报价当作已完成确认。' : 'No opening-confirmation observation. Premarket or stale quotes are never treated as completed confirmation.'}</p> : null}
                {auctionObservations.map((item) => (
                  <div key={item.id} className="rounded-lg border border-border bg-surface p-3 text-sm">
                    <div className="flex flex-wrap items-center gap-2">
                      <Badge variant={statusVariant(item.status || item.result)}>{item.status || item.result || '--'}</Badge>
                      <span className="font-medium">{item.stockName || item.stockCode}</span>
                      <span className="text-secondary-text">{item.sourceModel} · {item.market} · {item.phase || '--'}</span>
                      {item.identityVerified === false ? <Badge variant="danger">{zh ? '身份错误' : 'Identity mismatch'}</Badge> : null}
                      {item.stale ? <Badge variant="danger">{zh ? '快照陈旧' : 'Stale snapshot'}</Badge> : null}
                      {item.failClosedReason ? <Badge variant="danger">Fail-closed</Badge> : null}
                    </div>
                    <div className="mt-2 grid gap-1 text-xs text-secondary-text md:grid-cols-3">
                      <span>{zh ? '观察' : 'Observed'}: {formatChinaTime(item.observedAt)}</span>
                      <span>{zh ? '源时间' : 'Provider'}: {formatChinaTime(item.providerTimestamp)}</span>
                      <span>{zh ? '抓取' : 'Fetched'}: {formatChinaTime(item.fetchedAt)}</span>
                      <span>{zh ? '开盘确认价' : 'Opening confirmation'}: {money(item.virtualPrice ?? item.indicativePrice ?? item.quotePrice)}</span>
                      <span>{zh ? '昨收' : 'Prior close'}: {money(item.priorClose)}</span>
                      <span>{zh ? '缺口' : 'Gap'}: {pct(item.gapPct)}</span>
                      <span>{zh ? '买入区间' : 'Entry band'}: {money(item.entryLow)} - {money(item.entryHigh)}</span>
                      <span>{zh ? '开盘量比' : 'Opening volume ratio'}: {pct(item.openingVolumeRatio)}</span>
                      <span>{zh ? '历史竞价匹配量' : 'Legacy matched vol'}: {money(item.matchedVolume)}</span>
                      <span>{zh ? '历史竞价匹配量比' : 'Legacy matched ratio'}: {pct(item.matchedVolumeRatio)}</span>
                      <span>{zh ? '未匹配方向' : 'Unmatched side'}: {item.unmatchedSide || '--'}</span>
                      <span>{zh ? '不平衡' : 'Imbalance'}: {pct(item.imbalancePct)}</span>
                      <span>{zh ? '涨跌停接近度' : 'Limit proximity'}: {pct(item.limitProximityPct)}</span>
                    </div>
                    {item.failClosedReason ? <p className="mt-2 text-xs text-danger">{item.failClosedReason}</p> : null}
                  </div>
                ))}
              </div>
              <Pager page={pages.auctionObservations} total={totals.auctionObservations} onPage={(page) => setPage('auctionObservations', page)} zh={zh} />
            </div>
          </div>
          <div className="border-t border-border px-4 py-3">
            <details>
              <summary className="cursor-pointer text-sm font-semibold text-primary">{zh ? '历史重分析审计' : 'Historical reanalysis audit'}</summary>
              <div className="mt-3 space-y-2">
                {historicalReanalysisRequests.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无历史重分析。' : 'No historical reanalysis.'}</p> : null}
                {historicalReanalysisRequests.map((item) => (
                  <div key={String(item.id)} className="rounded-lg bg-surface-2 px-3 py-2 text-sm">
                    <span className="font-medium">{item.stockName || item.stockCode || '--'}</span>
                    <Badge variant={statusVariant(item.status)}>{item.status || '--'}</Badge>
                    <span className="ml-2 text-xs text-secondary-text">{item.requestKey || item.id} · {formatChinaTime(item.completedAt || item.createdAt)} · replacement #{item.replacementReportId ?? '--'} / #{item.replacementPlanId ?? '--'}</span>
                  </div>
                ))}
              </div>
              <Pager page={pages.reanalysisRequests} total={totals.reanalysisRequests} onPage={(page) => setPage('reanalysisRequests', page)} zh={zh} />
            </details>
          </div>
        </Card>

        <Card className="order-[90]" title={zh ? '报告到计划处理证据' : 'Report-to-plan processing evidence'} padding="none">
          <div className="border-b border-border px-4 py-3 text-xs text-secondary-text">
            {zh
              ? '这里展示持久化 reconciliation 队列/状态，是报告补偿的处理证据；上方开关和回看天数只是设置，不代表某份报告已成功生成计划。'
              : 'This shows the durable reconciliation queue/status as processing evidence. The enablement flag and lookback window above are settings only and do not prove any report produced a plan.'}
          </div>
          {contractErrors.reportPlanReconciliations ? (
            <div className="border-b border-border bg-danger/10 px-4 py-3 text-sm text-danger">
              {dependencyText(zh ? '报告到计划处理证据' : 'Report-to-plan processing evidence', zh)}
              <div className="mt-1 text-xs">{contractErrors.reportPlanReconciliations}</div>
            </div>
          ) : null}
          {reportPlanReconciliations.length === 0 ? (
            <div className="p-6"><EmptyState title={zh ? '暂无处理证据' : 'No processing evidence'} description={zh ? '成功、重试等待、失败和终止分类会出现在这里；不要把补偿设置当作处理结果。' : 'Completed, retry-wait, failed, and terminal classifications appear here; compensation settings are not processing outcomes.'} /></div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1320px] text-left text-sm">
                <thead className="border-b border-border text-xs text-secondary-text">
                  <tr>
                    <th className="p-4">{zh ? '报告 / 标的' : 'Report / symbol'}</th>
                    <th>{zh ? '状态 / 分类' : 'Status / classification'}</th>
                    <th>{zh ? '尝试 / 下次重试' : 'Attempts / next retry'}</th>
                    <th>{zh ? '模型 / 来源' : 'Model / source'}</th>
                    <th>{zh ? '计划 / 信号' : 'Plan / signal'}</th>
                    <th>{zh ? '最近时间' : 'Recent time'}</th>
                    <th>{zh ? '错误' : 'Error'}</th>
                  </tr>
                </thead>
                <tbody>
                  {reportPlanReconciliations.map((item) => (
                    <tr key={item.id} className="border-b border-border/60 align-top">
                      <td className="p-4 font-medium">{reconciliationStockLabel(item)}<div className="text-xs text-secondary-text">report #{item.sourceReportId ?? item.id} · {reconciliationReportMeta(item, zh)}</div><div className="break-all text-xs text-secondary-text">{item.reconciliationKey}</div></td>
                      <td><Badge variant={statusVariant(item.status)}>{label('reconciliation', item.status, (zh ? 'zh' : 'en'))}</Badge><div className="mt-1 text-xs text-secondary-text">{item.classification ? label('reconciliation', item.classification, (zh ? 'zh' : 'en')) : '--'}</div></td>
                      <td>{item.attemptCount ?? 0} / {item.maxAttempts ?? '--'}<div className="text-xs text-secondary-text">{zh ? '下次重试' : 'Next retry'}: {formatChinaTime(item.nextRetryAt)}</div></td>
                      <td>{item.sourceModel || '--'}<div className="text-xs text-secondary-text">{reconciliationTriggerSource(item)} · {item.planVersion}</div></td>
                      <td>{zh ? '计划' : 'Plan'} #{item.paperPlanId ?? '--'}<div className="text-xs text-secondary-text">signal #{item.decisionSignalId ?? '--'}</div></td>
                      <td>{formatChinaTime(reconciliationTime(item))}</td>
                      <td className="max-w-[280px] break-words text-danger">{reconciliationError(item)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
          <Pager page={pages.reportPlanReconciliations} total={totals.reportPlanReconciliations} onPage={(page) => setPage('reportPlanReconciliations', page)} zh={zh} />
        </Card>

        <Card className="order-[90]" title={zh ? '冻结策略与费用审计' : 'Frozen strategy and cost audit'} padding="none">
          <div className="border-b border-border px-4 py-3 text-xs text-secondary-text">
            {zh
              ? '这些字段来自计划、持仓批次和成交事件本身；缺失会按后端未暴露显示，不用当前页面配置回填历史。'
              : 'These fields come from plans, lots, and trade events themselves. Missing values are shown as not exposed; current page configuration is not backfilled into history.'}
          </div>
          <div className="grid gap-4 p-4 xl:grid-cols-2">
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? '计划冻结风格 / 大盘门控' : 'Plan frozen style / market gate'}</div>
              <div className="max-h-80 space-y-2 overflow-y-auto overscroll-contain p-3">
                {(operations.length ? operations : plans).slice(0, 12).map((item) => (
                  <div key={`strategy-${item.id}`} className="rounded-lg bg-surface px-3 py-2 text-xs">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="font-medium">{item.stockName || item.stockCode}</span>
                      <span className="text-secondary-text">{item.sourceModel} · report #{item.sourceReportId}</span>
                      <Badge variant={item.tradingStyle ? 'success' : 'warning'}>{item.tradingStyle || (zh ? '风格未暴露' : 'style not exposed')}</Badge>
                    </div>
                    <div className="mt-1 text-secondary-text">{frozenStyleSummary(item.tradingStyle, item.tradingStyleSettings, zh)}</div>
                    <div className="mt-1 text-secondary-text">
                      {zh ? '风格策略 / 历史充分' : 'Style policy / history sufficient'}: {item.tradingStylePolicyVersion || '--'} / {historySufficiencyText(item, zh)}
                    </div>
                    <div className="mt-1 text-secondary-text">
                      {zh ? '大盘 regime' : 'Market regime'}: {item.marketRegime || (zh ? '后端未暴露' : 'not exposed')}
                      {item.marketRegimePolicyVersion ? ` · ${item.marketRegimePolicyVersion}` : ''}
                      {item.marketRegimeContextId ? ` · context #${item.marketRegimeContextId}` : ''}
                    </div>
                    <div className="mt-1 text-secondary-text">{marketGateText(item, zh)}</div>
                    <div className="mt-1 break-words text-secondary-text">{zh ? '大盘来源' : 'Market provenance'}: {compactJson(item.marketRegimeProvenance)}</div>
                  </div>
                ))}
                {!operations.length && !plans.length ? <p className="text-sm text-secondary-text">{zh ? '暂无计划。' : 'No plans.'}</p> : null}
              </div>
            </div>
            <div className="rounded-xl border border-border bg-surface-2">
              <div className="border-b border-border px-3 py-2 text-sm font-semibold">{zh ? 'lot 冻结字段 / 成交费用' : 'Lot frozen fields / fill costs'}</div>
              <div className="max-h-80 space-y-2 overflow-y-auto overscroll-contain p-3">
                {positions.slice(0, 8).map((item) => (
                  <div key={`lot-strategy-${item.id}`} className="rounded-lg bg-surface px-3 py-2 text-xs">
                    <div className="font-medium">{item.stockName || item.stockCode} · lot #{item.id}</div>
                    <div className="mt-1 text-secondary-text">{zh ? '冻结风格' : 'Frozen style'}: {frozenStyleSummary(item.tradingStyle, item.tradingStyleSettings, zh)} {item.tradingStylePolicyVersion ? `· ${item.tradingStylePolicyVersion}` : ''}</div>
                    <div className="text-secondary-text">{zh ? '历史充分' : 'History sufficient'}: {historySufficiencyText(item, zh)}</div>
                    <div className="text-secondary-text">{zh ? '冻结大盘' : 'Frozen regime'}: {item.marketRegime || (zh ? '后端未暴露' : 'not exposed')} {item.marketRegimePolicyVersion ? `· ${item.marketRegimePolicyVersion}` : ''}</div>
                    <div className="text-secondary-text">{marketGateText(item, zh)}</div>
                    <div className="break-words text-secondary-text">{zh ? '大盘来源' : 'Market provenance'}: {compactJson(item.marketRegimeProvenance)}</div>
                  </div>
                ))}
                {events.slice(0, 8).map((item) => (
                  <div key={`event-cost-${item.id}`} className="rounded-lg bg-surface px-3 py-2 text-xs">
                    <div className="font-medium">{item.eventDate} · {item.stockName || item.stockCode} · {eventActionLabel(item.eventType, zh)}</div>
                    <div className="mt-1 text-secondary-text">
                      {zh ? '成交费用' : 'Fill costs'}: gross {currencyMoney(item.grossAmount, item.currency)} · commission {currencyMoney(item.commission, item.currency)} · tax {currencyMoney(item.tax, item.currency)} · slippage {currencyMoney(item.slippageAmount, item.currency)} · fees {currencyMoney(item.fees, item.currency)} · net {currencyMoney(item.netAmount, item.currency)}
                    </div>
                    <div className="text-secondary-text">
                      {zh ? '已实现' : 'Realized'}: gross {currencyMoney(item.grossRealizedPnl, item.currency)} · net {currencyMoney(item.netRealizedPnl ?? item.realizedPnlAfter, item.currency)}
                    </div>
                  </div>
                ))}
                {!positions.length && !events.length ? <p className="text-sm text-secondary-text">{zh ? '暂无 lot 或成交事件。' : 'No lots or trade events.'}</p> : null}
              </div>
            </div>
          </div>
        </Card>

        <Card className="order-[90]" title={zh ? '意图解析与模拟订单审计' : 'Intent resolution and simulated-order audit'} padding="none">
          {resolutions.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无意图解析记录' : 'No resolution records'} description={zh ? '开仓、减仓、止盈止损、T+1 待卖出和被压制的意图都会记录在这里。' : 'Opens, reductions, stops, targets, T+1 pending exits, and suppressed intents are recorded here.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1480px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '时间 / 标的' : 'Time / symbol'}</th><th>{zh ? '模型 / 策略' : 'Model / policy'}</th><th>{zh ? '选择意图' : 'Chosen intent'}</th><th>{zh ? '候选 / 被压制' : 'Candidate / suppressed'}</th><th>{zh ? '数量' : 'Quantity'}</th><th>{zh ? '解析键' : 'Resolution keys'}</th><th>{zh ? '证据' : 'Evidence'}</th></tr></thead><tbody>{resolutions.map((item) => (
              <tr key={item.id} className="border-b border-border/60 align-top">
                <td className="p-4">{formatChinaTime(item.bucketStartedAt || item.createdAt)}<div className="text-xs text-secondary-text">{item.stockCode} · {item.market}/{item.currency || '--'}</div></td>
                <td>{item.sourceModel}<div className="text-xs text-secondary-text">{item.policyVersion || '--'} · {item.mode || '--'}</div></td>
                <td><Badge variant={item.status === 'failed_closed' ? 'danger' : item.selectedIntent ? 'success' : 'warning'}>{item.selectedIntent ? label('intent', item.selectedIntent, (zh ? 'zh' : 'en')) : label('resolutionStatus', item.status, (zh ? 'zh' : 'en'))}</Badge><div className="mt-1 text-xs text-secondary-text">{compactJson(item.chosenIntent)}</div></td>
                <td><div className="text-xs">{zh ? '候选' : 'Candidate'}: {intentSummary(item.candidateIntents, zh)}</div><div className="mt-1 text-xs text-secondary-text">{zh ? '压制' : 'Suppressed'}: {intentSummary(item.suppressedIntents, zh)}</div></td>
                <td>{zh ? '请求' : 'Req'} {money(item.requestedQuantity)}<div className="text-xs text-secondary-text">{zh ? '执行' : 'Exec'} {money(item.executedQuantity)} · {zh ? '未成' : 'Unfilled'} {money(item.unfilledQuantity)}</div></td>
                <td className="max-w-[360px] break-all text-xs text-secondary-text">{item.resolutionKey}<div>{zh ? '账户' : 'Account'} #{item.capitalAccountId ?? '--'} · {zh ? '报告' : 'report'} #{item.sourceReportId ?? '--'} · signal #{item.decisionSignalId ?? '--'}</div></td>
                <td><details><summary className="cursor-pointer text-primary">{zh ? '查看完整证据' : 'Full evidence'}</summary><pre className="mt-2 max-w-[520px] overflow-x-auto whitespace-pre-wrap text-xs text-secondary-text">{JSON.stringify(item.evidence || {}, null, 2)}</pre></details></td>
              </tr>
            ))}</tbody></table></div>
          )}
          <Pager page={pages.resolutions} total={totals.resolutions} onPage={(page) => setPage('resolutions', page)} zh={zh} />
        </Card>

        <Card className="order-[90]" title={zh ? '风险修订审计' : 'Risk-revision audit'} padding="none">
          {riskRevisions.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无风险修订' : 'No risk revisions'} description={zh ? '后续模型报告对持仓止损/目标的收紧、拒绝或无变化会出现在这里。' : 'Later report-driven stop/target tightening, rejection, and no-op decisions appear here.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1280px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '时间 / lot' : 'Time / lot'}</th><th>{zh ? '模型 / 标的' : 'Model / symbol'}</th><th>{zh ? '结果' : 'Result'}</th><th>{zh ? '止损修订' : 'Stop revision'}</th><th>{zh ? '目标修订' : 'Target revision'}</th><th>{zh ? '来源' : 'Source'}</th><th>{zh ? '证据' : 'Evidence'}</th></tr></thead><tbody>{riskRevisions.map((item) => (
              <tr key={item.id} className="border-b border-border/60 align-top">
                <td className="p-4">{formatChinaTime(item.evaluatedAt || item.createdAt)}<div className="text-xs text-secondary-text">position #{item.positionId} · v{item.revisionVersion ?? '--'}</div></td>
                <td>{item.sourceModel}<div className="text-xs text-secondary-text">{item.stockCode} · {item.market}</div></td>
                <td><Badge variant={item.result?.includes('applied') ? 'success' : item.result === 'rejected' ? 'danger' : 'default'}>{item.result}</Badge><div className="mt-1 text-xs text-secondary-text">{item.reason}</div></td>
                <td>{money(item.oldStopLoss)} → {money(item.newStopLoss)}<div className="text-xs text-secondary-text">{zh ? '应用' : 'Applied'} {money(item.appliedStopLoss)}</div></td>
                <td>{money(item.oldTargetPrice)} → {money(item.newTargetPrice)}<div className="text-xs text-secondary-text">{zh ? '应用' : 'Applied'} {money(item.appliedTargetPrice)}</div></td>
                <td className="max-w-[300px] break-all text-xs text-secondary-text">{item.revisionKey}<div>{zh ? '计划' : 'Plan'} #{item.planId ?? '--'} · {zh ? '报告' : 'report'} #{item.sourceReportId ?? '--'} · signal #{item.decisionSignalId ?? '--'}</div></td>
                <td><details><summary className="cursor-pointer text-primary">{zh ? '查看证据' : 'Evidence'}</summary><pre className="mt-2 max-w-[520px] overflow-x-auto whitespace-pre-wrap text-xs text-secondary-text">{JSON.stringify(item.evidence || {}, null, 2)}</pre></details></td>
              </tr>
            ))}</tbody></table></div>
          )}
          <Pager page={pages.riskRevisions} total={totals.riskRevisions} onPage={(page) => setPage('riskRevisions', page)} zh={zh} />
        </Card>

        <Card className="order-[90]" title={zh ? '通知投递记录' : 'Notification dispatches'}>
          <div className="space-y-2">{dispatches.map((item) => <div key={item.id} className="rounded-xl bg-surface-2 px-3 py-2 text-sm"><div className="flex flex-wrap items-center justify-between gap-2"><span>{item.channel} · {item.dispatchKey}</span><Badge variant={item.status === 'sent' ? 'success' : item.status === 'failed' ? 'danger' : 'warning'}>{item.status}</Badge></div><div className="mt-1 text-xs text-secondary-text">event #{item.eventId} · {zh ? '尝试' : 'attempts'} {item.attemptCount} · {zh ? '发送' : 'sent'} {formatChinaTime(item.sentAt)}</div>{item.lastError ? <div className="mt-1 text-xs text-danger">{item.lastError}</div> : null}</div>)}{dispatches.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无通知投递记录' : 'No notification dispatches'}</p> : null}</div>
          <Pager page={pages.dispatches} total={totals.dispatches} onPage={(page) => setPage('dispatches', page)} zh={zh} />
        </Card>

        <Card className="order-[80]" title={zh ? '最近盘中监控观察' : 'Recent intraday monitor observations'} padding="none">
          {observations.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无盘中观察' : 'No intraday observations'} description={zh ? '开启盘中模拟监控后，开市期间会记录每个时间桶的综合判断。' : 'When intraday paper monitoring is enabled, market-session checks are recorded by time bucket.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1180px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '时间 / 股票' : 'Time / stock'}</th><th>{zh ? '模型' : 'Model'}</th><th>{zh ? '行情' : 'Quote'}</th><th>{zh ? '结果' : 'Result'}</th><th>{zh ? '规则与因子' : 'Rules and factors'}</th><th>{zh ? '分数 / 盈亏比' : 'Score / R:R'}</th><th>{zh ? '原因 / 原始证据' : 'Reasons / raw evidence'}</th></tr></thead><tbody>{observations.map((item) => {
              const evidence = asRecord(item.evidence);
              const rules = asArray(evidence.requiredRules);
              const factors = asArray(evidence.factorDomains);
              const reasons = Array.isArray(evidence.reasons) ? evidence.reasons.map(String) : [];
              const riskReward = typeof evidence.riskReward === 'number' ? evidence.riskReward : null;
              const policy = asRecord(evidence.policy);
              return (
                <tr key={item.id} className="border-b border-border/60 align-top">
                  <td className="p-4">{formatChinaTime(item.observedAt)}<div className="text-xs text-secondary-text">{item.stockName || item.stockCode}</div></td>
                  <td>{item.sourceModel}</td>
                  <td>{money(item.quotePrice)}<div className="text-xs text-secondary-text">{item.quoteSource || '--'}</div><div className="text-xs text-secondary-text">{zh ? '源时间' : 'Provider'}: {formatChinaTime(item.quoteProviderTimestamp)}</div><div className="text-xs text-secondary-text">{zh ? '抓取' : 'Fetched'}: {formatChinaTime(item.quoteFetchedAt)}</div></td>
                  <td><Badge variant={item.action === 'open' || item.action === 'close' ? 'success' : item.result === 'failed_closed' ? 'danger' : 'default'}>{item.result}</Badge><div className="mt-1 text-xs text-secondary-text">{item.action}</div></td>
                  <td className="max-w-[360px]">
                    <div className="mb-2"><Badge variant={item.requiredPassed ? 'success' : 'warning'}>{item.requiredPassed ? (zh ? '硬规则通过' : 'Rules passed') : (zh ? '硬规则未通过' : 'Rules not passed')}</Badge></div>
                    <div className="flex flex-wrap gap-1">{rules.slice(0, 4).map((rule, index) => <Badge key={index} variant={rule.passed ? 'success' : rule.available === false ? 'warning' : 'default'}>{String(asRecord(rule.rule).type || rule.mode || 'rule')}</Badge>)}</div>
                    <div className="mt-2 flex flex-wrap gap-1">{factors.map((factor) => <Badge key={String(factor.domain)} variant={factorBadgeVariant(factor)}>{String(factor.domain)} {factor.available === false ? (zh ? '不可用' : 'N/A') : factor.passed ? (zh ? '通过' : 'Pass') : (zh ? '未过' : 'No')}</Badge>)}</div>
                  </td>
                  <td>{item.factorsPassed}/{item.factorsAvailable}<div className="text-xs text-secondary-text">{zh ? '综合分' : 'Composite'}: {pct(typeof item.compositeScore === 'number' ? item.compositeScore * 100 : null)}</div><div className="text-xs text-secondary-text">{zh ? '阈值' : 'Min'}: {pct(typeof policy.minCompositeScore === 'number' ? policy.minCompositeScore * 100 : null)}</div><div className="text-xs text-secondary-text">{zh ? '盈亏比' : 'R:R'}: {riskReward == null ? '--' : riskReward.toFixed(2)} / {typeof policy.minRiskReward === 'number' ? policy.minRiskReward.toFixed(2) : '--'}</div></td>
                  <td><div className="max-w-[320px] space-y-1 text-xs text-secondary-text">{reasons.slice(0, 4).map((reason) => <div key={reason}>{reason}</div>)}</div><details><summary className="mt-2 cursor-pointer text-primary">{zh ? '原始证据' : 'Raw evidence'}</summary><pre className="mt-2 max-w-[520px] overflow-x-auto whitespace-pre-wrap text-xs text-secondary-text">{JSON.stringify(item.evidence, null, 2)}</pre></details></td>
                </tr>
              );
            })}</tbody></table></div>
          )}
          <Pager page={pages.observations} total={totals.observations} onPage={(page) => setPage('observations', page)} zh={zh} />
        </Card>

        <Card className="order-[90]" title={zh ? '最近规则评估证据' : 'Recent rule-evaluation evidence'}>
          <div className="space-y-2">{evaluations.map((item) => <details key={item.id} className="rounded-xl bg-surface-2 px-3 py-2 text-sm"><summary className="cursor-pointer">{item.evaluationDate} · {item.stockName || item.stockCode || `Plan #${item.planId}`} · {item.result} · {(item.reasons || []).join(', ') || '--'}</summary><pre className="mt-2 overflow-x-auto whitespace-pre-wrap text-xs text-secondary-text">{JSON.stringify(item.evidence, null, 2)}</pre></details>)}{evaluations.length === 0 ? <p className="text-sm text-secondary-text">{zh ? '暂无评估证据' : 'No evaluation evidence'}</p> : null}</div>
          <Pager page={pages.evaluations} total={totals.evaluations} onPage={(page) => setPage('evaluations', page)} zh={zh} />
        </Card>
        </div>
      </details>

      <details className="group order-[9]">
        <summary className="flex cursor-pointer select-none items-center gap-2 px-4 py-3 text-sm font-semibold text-foreground">
          <ChevronDown className="h-4 w-4 shrink-0 text-secondary-text transition-transform group-open:rotate-180" aria-hidden="true" />
          <span>{zh ? '计划与批次明细（汇总统计、聚合持仓、操作生命周期、报告归因批次）' : 'Plan and lot detail (summary stats, aggregate holdings, operational lifecycle, report-attributed lots)'}</span>
        </summary>
        <div className="space-y-4 border-t border-border p-4">
        <div className="order-[10] grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <Card title={planScope === 'active' ? (zh ? '当前操作' : 'Current operations') : (zh ? '计划审计记录' : 'Plan audit records')}><p className="text-2xl font-semibold">{planScope === 'active' ? totals.operations : totals.plans}</p></Card>
          <Card title={zh ? '持仓批次 / 已平仓批次' : 'Open lots / closed lots'}><p className="text-2xl font-semibold">{summary.openPositions || 0} / {summary.closedPositions || 0}</p><p className="mt-1 text-xs text-secondary-text">{zh ? `共 ${totals.positions} 条 lot` : `${totals.positions} lots total`}</p></Card>
          <Card title={zh ? '已实现 / 未实现' : 'Realized / unrealized'}><p className="text-2xl font-semibold">{currencyMoney(summary.realizedPnl ?? summary.netRealizedPnl, undefined)}</p><p className="mt-1 text-xs text-secondary-text">{zh ? '未实现' : 'Unrealized'} {summary.valuationComplete === false ? (zh ? '缺价 / --' : 'Unpriced / --') : currencyMoney(summary.unrealizedPnl, undefined)}</p></Card>
          <Card title={zh ? '总盈亏 / 胜率' : 'Total PnL / win rate'}><p className="text-2xl font-semibold">{summary.valuationComplete === false ? '--' : currencyMoney(summary.totalPnl, undefined)}</p><p className="mt-1 text-xs text-secondary-text">{pct(summary?.winRatePct)} · {zh ? '标准化' : 'Norm'} {pct(normalizedReturn)}</p></Card>
          <Card title={zh ? '显式成本拆分' : 'Explicit cost breakdown'}><p className="text-2xl font-semibold">{currencyMoney(summary.explicitCostTotal ?? summary.totalFees, undefined)}</p><p className="mt-1 text-xs text-secondary-text">{zh ? '佣金' : 'Commission'} {currencyMoney(summary.totalCommission, undefined)} · {zh ? '卖出税' : 'Sell tax'} {currencyMoney(summary.totalSellTax, undefined)} · {zh ? '滑点' : 'Slippage'} {currencyMoney(summary.totalSlippageAmount, undefined)}</p></Card>
        </div>

        <Card className="order-[50]" title={zh ? '聚合模拟持仓（按模型/市场/标的/币种）' : 'Aggregate paper holdings by model / market / symbol / currency'} padding="none">
          <div className="border-b border-border px-4 py-3 text-xs text-secondary-text">
            {zh
              ? '这里是一条聚合持仓，来自下方多条报告归因的模拟买入 lot；不会把不同模型、市场或币种混合。'
              : 'Each row is one aggregate holding derived from the report-attributed paper lots below; models, markets, and currencies are never merged.'}
          </div>
          {holdings.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无聚合持仓' : 'No aggregate holdings'} description={zh ? '产生模拟买入后，这里会按模型、市场、标的和币种聚合展示。' : 'After simulated buys, this table aggregates by model, market, symbol, and currency.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1500px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '聚合持仓' : 'Aggregate holding'}</th><th>{zh ? '模型 / 市场' : 'Model / market'}</th><th>{zh ? '总数量' : 'Total qty'}</th><th>{zh ? '可卖 / T+1 冻结' : 'Sellable / T+1 frozen'}</th><th>{zh ? '总成本 / 加权成本' : 'Total cost / weighted cost'}</th><th>{zh ? '批次数' : 'Lots'}</th><th>{zh ? '标记价 / 来源' : 'Mark / source'}</th><th>{zh ? '持仓市值' : 'Market value'}</th><th>{zh ? '未实现盈亏' : 'Unrealized'}</th><th>{zh ? '同股集中度' : 'Same-symbol concentration'}</th></tr></thead><tbody>{holdings.map((item) => <tr key={`${item.sourceModel}|${item.market}|${item.stockCode}|${item.currency}`} className="border-b border-border/60 align-top"><td className="p-4 font-medium">{item.stockName || item.stockCode}<div className="text-xs text-secondary-text">{item.stockCode} · {item.currency}</div></td><td>{item.sourceModel}<div className="text-xs text-secondary-text">{item.market}</div></td><td>{money(item.totalQuantity)}</td><td><Badge variant={item.availableQuantity > 0 ? 'success' : 'warning'}>{zh ? '可卖' : 'Sellable'} {money(item.availableQuantity)}</Badge><div className="mt-1 text-xs text-secondary-text">{zh ? 'T+1 冻结' : 'T+1 frozen'} {money(item.frozenQuantity)}</div></td><td>{currencyMoney(item.totalCost, item.currency)}<div className="text-xs text-secondary-text">{zh ? '加权成本' : 'Weighted cost'} {currencyMoney(item.weightedAveragePrice, item.currency)}</div></td><td>{item.lotCount}</td><td>{currencyMoney(item.latestPrice, item.currency)}<div className="text-xs text-secondary-text">{zh ? '来源' : 'Source'}: {item.markSource || '--'}</div><div className="text-xs text-secondary-text">{formatChinaTime(item.observedAt)}</div>{item.valuationComplete === false ? <Badge variant="warning">{zh ? '估值不完整' : 'Incomplete valuation'}</Badge> : null}{valuationWarning(item.markStale, item.observedAt, zh) ? <div className="mt-1 text-xs text-warning">{valuationWarning(item.markStale, item.observedAt, zh)}</div> : null}</td><td>{marketCurrencyMoney(item.marketValue, item.currency, item.valuationComplete, zh)}</td><td className={(item.unrealizedPnl || 0) >= 0 ? 'text-success' : 'text-danger'}>{marketCurrencyMoney(item.unrealizedPnl, item.currency, item.valuationComplete, zh)}<div className="text-xs text-secondary-text">{item.valuationComplete === false ? '--' : pct(item.unrealizedReturnPct)}</div></td><td>{pct(item.symbolConcentrationPct)}<div className="text-xs text-secondary-text">{zh ? '按账户成本口径权益计算' : 'of account book equity'}</div><div className="text-xs text-secondary-text">{zh ? '超过共享同股上限时会缩量或拒绝新增买入' : 'new buys resize or reject above shared same-symbol cap'}</div></td></tr>)}</tbody></table></div>
          )}
          <Pager page={pages.holdings} total={totals.holdings} onPage={(page) => setPage('holdings', page)} zh={zh} label={zh ? '聚合持仓分页' : 'Aggregate holdings pagination'} />
        </Card>

        <Card className="order-[60]" title={planScope === 'active' ? (zh ? '当前操作生命周期' : 'Current operational lifecycle') : (zh ? '计划审计记录' : 'Plan audit records')} padding="none">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3">
            <p className="text-xs text-secondary-text">{planScope === 'active'
              ? (zh ? '来自 /operations：已触发但仍有 open/pending lot 的计划会继续显示为当前操作；缺少开盘确认或重分析数据时显示不可用/未排程。' : 'From /operations: triggered plans with open or pending lots remain visible as current operations; missing opening-confirmation or reanalysis data is shown as unavailable/not scheduled.')
              : (zh ? '来自 /plans 全状态审计：过期、被替代、不可执行和已关闭计划只作历史，不会显示为可成交。' : 'From /plans all-status audit: expired, superseded, non-executable, and closed plans are history only and never shown as executable.')}</p>
            <div className="flex rounded-xl border border-border bg-surface-2 p-1 text-xs">
              <button type="button" className={`rounded-lg px-3 py-1.5 ${planScope === 'active' ? 'bg-primary text-white' : 'text-secondary-text'}`} onClick={() => selectPlanScope('active')}>{zh ? '当前操作' : 'Current operations'}</button>
              <button type="button" className={`rounded-lg px-3 py-1.5 ${planScope === 'all' ? 'bg-primary text-white' : 'text-secondary-text'}`} onClick={() => selectPlanScope('all')}>{zh ? '审计历史' : 'Audit history'}</button>
            </div>
          </div>
          {planScope === 'active' ? (
            operations.length === 0 ? <div className="p-6"><EmptyState title={zh ? '当前没有操作' : 'No current operations'} description={zh ? '等待入场、已触发但仍有持仓监控、T+1 待卖出或重分析阻塞的计划会出现在这里。' : 'Waiting-entry, triggered-but-monitored, T+1 pending-exit, and reanalysis-blocked plans appear here.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1880px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '股票 / 报告' : 'Stock / report'}</th><th>{zh ? '操作状态' : 'Operational state'}</th><th>{zh ? '模型意见 / 执行策略' : 'Model opinion / authority'}</th><th>{zh ? '有效交易日' : 'Valid session'}</th><th>{zh ? '批次 / 持仓' : 'Legs / lots'}</th><th>{zh ? '下一次检查' : 'Next check'}</th><th>{zh ? '重分析' : 'Reanalysis'}</th><th>{zh ? '规则 / 风控值' : 'Rules / risk values'}</th><th>{zh ? '关联 ID' : 'Linked IDs'}</th></tr></thead><tbody>{operations.map((item) => <tr key={item.id} className="border-b border-border/60 align-top"><td className="p-4 font-medium">{item.stockName || item.stockCode}<div className="text-xs text-secondary-text">{item.stockCode} · {item.sourceModel}</div><div className="text-xs text-secondary-text">#{item.sourceReportId} · {item.reportDate}</div></td><td><Badge variant={lifecycleVariant(item)}>{lifecycleLabel(item, zh)}</Badge><div className="mt-1 text-xs text-secondary-text">{item.operationalState || item.stage || item.status || '--'}</div>{item.isOperational === false ? <div className="mt-1 text-xs text-warning">{zh ? '不可执行：仅审计或等待外部条件' : 'Not executable: audit-only or waiting external conditions'}</div> : null}</td><td className="min-w-[240px]"><div>{zh ? 'LLM 原始' : 'Raw LLM'}: <span className="font-medium">{strategyActionLabel(item.strategyThesisRawAction ?? item.action, zh)}</span></div><div>{zh ? '执行层生效' : 'Effective authority'}: <span className="font-medium">{strategyActionLabel(item.strategyThesisEffectiveAction, zh)}</span></div><div className="mt-1"><Badge variant={item.strategyThesisEntryPermitted === true ? 'success' : item.strategyThesisEntryPermitted === false ? 'warning' : 'default'}>{item.strategyThesisEntryPermitted == null ? (zh ? '旧数据：未记录入场权限' : 'Legacy: entry permission not recorded') : item.strategyThesisEntryPermitted ? (zh ? '允许新入场' : 'New entry allowed') : (zh ? '暂停新入场' : 'New entry paused')}</Badge></div><div className="mt-1 text-xs text-secondary-text">{item.strategyThesisResult || (zh ? '未记录' : 'not recorded')} · {item.strategyThesisState || '--'}</div><div className="text-xs text-secondary-text">{zh ? '确认' : 'Confirmation'}: {strategyConfirmationText(item, zh)}</div><div className="max-w-[280px] break-words text-xs text-secondary-text">{zh ? '原因' : 'Reason'}: {item.strategyThesisReason || (zh ? '旧数据未记录' : 'not recorded for legacy data')}</div></td><td>{item.validForDate || '--'}<div className="text-xs text-secondary-text">{zh ? '交易时段' : 'Session'}: {item.validForDate ? (zh ? '有效' : 'valid') : (zh ? '不可用' : 'unavailable')}</div><div className="text-xs text-secondary-text">{zh ? '开盘确认' : 'Opening confirmation'}: {zh ? '详见上方证据/重分析状态' : 'see evidence/reanalysis status above'}</div></td><td><div>{zh ? '已成交腿' : 'Filled legs'} {item.executedEntryLegIds?.length || 0} / {item.entryLegIds?.length || item.entryLegs?.length || 0}</div><div className="text-xs text-secondary-text">{zh ? '待执行腿' : 'Pending legs'}: {idList(item.pendingEntryLegIds)}</div><div className="text-xs text-secondary-text">{zh ? 'open lot' : 'open lots'}: {item.openLotCount ?? 0} · {zh ? '待卖出' : 'pending exit'} {item.pendingExitCount ?? 0}</div><div className="text-xs text-secondary-text">{zh ? '总 lot' : 'total lots'}: {item.lotCount ?? 0}</div>{exitLegText(item, zh)}</td><td>{nextCheckText(item, zh)}<div className="text-xs text-secondary-text">{zh ? '未提供时不视为已排程' : 'Missing value is not treated as scheduled'}</div></td><td>{reanalysisText(item, zh)}{lowPayoffCorrectionText(item, zh) ? <div className="mt-1 text-xs text-warning">{lowPayoffCorrectionText(item, zh)}</div> : null}<div className="text-xs text-secondary-text">{zh ? '来源/开盘确认证据受后端数据限制' : 'Provenance/opening-confirmation evidence limited by backend data'}</div></td><td><div className="space-y-1">{(item.rules || []).slice(0, 2).map((rule, index) => <div key={index} className="rounded-lg bg-surface-2 px-2 py-1 text-xs">{rule.sourceText || rule.type}</div>)}</div><div className="mt-2 text-xs text-secondary-text">{zh ? '原始计划 止损 / 目标' : 'Source plan stop / target'} {money(item.stopLoss)} / {money(item.targetPrice)}</div>{item.weightedTargetPrice ? <div className="text-xs text-secondary-text">{zh ? '加权目标 / 加权RR' : 'Weighted target / RR'} {money(item.weightedTargetPrice)} / {money(item.weightedRiskReward)}</div> : null}{item.effectiveRiskLots?.length ? <div className="mt-2 space-y-1">{item.effectiveRiskLots.map((lot) => <div key={lot.positionId} className="rounded-lg border border-border px-2 py-1 text-xs"><div className="font-medium">lot #{lot.positionId} · {zh ? '数量' : 'qty'} {money(lot.quantity)} · {zh ? '当前生效' : 'effective'} {money(lot.effectiveStopLoss)} / {money(lot.effectiveTargetPrice)}</div><div className="text-secondary-text">{lot.revisionId ? `${zh ? '修订' : 'revision'} v${lot.revisionVersion ?? '--'} · ${zh ? '报告' : 'report'} #${lot.revisionSourceReportId ?? '--'} · ${lot.revisionResult || '--'}` : (zh ? '尚无后续修订，沿用原始计划' : 'No later revision; source plan remains effective')}</div></div>)}</div> : <div className="mt-1 text-xs text-secondary-text">{zh ? '当前生效：暂无 open lot' : 'Effective: no open lot'}</div>}</td><td><div className="text-xs text-secondary-text">position: {idList(item.positionIds)}</div><div className="text-xs text-secondary-text">entry leg: {idList(item.entryLegIds)}</div><div className="text-xs text-secondary-text">exit leg: {idList(item.pendingExitLegIds)}</div><div className="text-xs text-secondary-text">reanalysis: {idList(item.reanalysisRequestIds)}</div><div className="text-xs text-secondary-text">risk revision: {idList(item.latestRiskRevisionIds)}</div></td></tr>)}</tbody></table></div>
            )
          ) : plans.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无计划审计记录' : 'No plan audit records'} description={zh ? '开启配置后，新报告会生成可审计计划。' : 'New reports create auditable plans after the feature is enabled.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1000px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '股票' : 'Stock'}</th><th>{zh ? '模型 / 报告' : 'Model / report'}</th><th>{zh ? '生命周期状态' : 'Lifecycle status'}</th><th>{zh ? '有效交易日' : 'Valid session'}</th><th>{zh ? '可执行规则' : 'Executable rules'}</th><th>{zh ? '止损 / 目标' : 'Stop / target'}</th></tr></thead><tbody>{plans.map((item) => <tr key={item.id} className="border-b border-border/60 align-top"><td className="p-4 font-medium">{item.stockName || item.stockCode}<div className="text-xs text-secondary-text">{item.stockCode}</div></td><td>{item.sourceModel}<div className="text-xs text-secondary-text">#{item.sourceReportId} · {item.reportDate}</div></td><td><Badge variant={item.status === 'active' && item.executable ? 'success' : 'warning'}>{item.status}</Badge><div className="mt-1 text-xs text-secondary-text">{item.executable ? (zh ? '规则可执行' : 'Rules executable') : (zh ? '仅描述' : 'Descriptive')}</div></td><td>{item.validForDate || '--'}</td><td className="max-w-[420px] py-4"><div className="space-y-1">{(item.rules || []).map((rule, index) => <div key={index} className="rounded-lg bg-surface-2 px-2 py-1 text-xs">{rule.sourceText || rule.type}</div>)}</div>{item.descriptiveConditions?.length ? <div className="mt-2 space-y-1 text-xs text-warning">{item.descriptiveConditions.map((entry, index) => <p key={index}>{entry.text || entry.reason || '--'}</p>)}</div> : null}</td><td>{money(item.stopLoss)} / {money(item.targetPrice)}</td></tr>)}</tbody></table></div>
          )}
          <Pager page={planScope === 'active' ? pages.operations : pages.plans} total={planScope === 'active' ? totals.operations : totals.plans} onPage={(page) => setPage(planScope === 'active' ? 'operations' : 'plans', page)} zh={zh} />
        </Card>

        <Card className="order-[70]" title={zh ? '报告归因模拟批次（lot 明细）' : 'Report-attributed paper lots'} padding="none">
          {positions.length === 0 ? <div className="p-6"><EmptyState title={zh ? '暂无模拟买入批次' : 'No paper lots'} description={zh ? '只有全部白名单条件成立才会模拟买入。' : 'A paper lot is created only after every whitelisted condition passes.'} /></div> : (
            <div className="overflow-x-auto"><table className="w-full min-w-[1580px] text-left text-sm"><thead className="border-b border-border text-xs text-secondary-text"><tr><th className="p-4">{zh ? '股票 / 报告' : 'Stock / report'}</th><th>{zh ? '模型' : 'Model'}</th><th>{zh ? '模拟买入' : 'Simulated buy'}</th><th>{zh ? '当前剩余' : 'Current remaining'}</th><th>{zh ? '可卖 / 冻结' : 'Sellable / frozen'}</th><th>{zh ? '风控测算 / 生效值' : 'Sizing / effective risk'}</th><th>{zh ? '模拟卖出' : 'Simulated sell'}</th><th>{zh ? '已实现盈亏' : 'Realized PnL'}</th><th>{zh ? '同步' : 'Sync'}</th></tr></thead><tbody>{positions.map((item) => <tr key={item.id} className="border-b border-border/60 align-top"><td className="p-4">{item.stockName || item.stockCode}<div className="text-xs text-secondary-text">{item.stockCode} · {zh ? '报告' : 'report'} #{item.sourceReportId}</div></td><td>{item.sourceModel}<div className="text-xs text-secondary-text">{item.market}</div></td><td>{item.openedOn} @ {money(item.openedPrice)}<div className="text-xs text-secondary-text">{zh ? '初始数量' : 'Initial qty'} {money(item.initialQuantity ?? item.quantity)} · {zh ? '初始成本' : 'Initial cost'} {currencyMoney(item.initialAllocatedCapital ?? item.normalizedNotional, item.currency)}</div></td><td>{currencyMoney(item.remainingCost ?? item.allocatedCapital ?? item.normalizedNotional, item.currency)}<div className="text-xs text-secondary-text">{zh ? '剩余数量' : 'Remaining qty'} {money(item.quantity)} · {zh ? '手数' : 'Lot'} {item.lotSize ?? '--'}</div>{item.legacyFixedNotional ? <Badge variant="warning">{zh ? '旧固定名义本金' : 'Legacy fixed notional'}</Badge> : null}</td><td><Badge variant={(item.availableQuantity ?? 0) > 0 ? 'success' : 'warning'}>{zh ? '可卖' : 'Sellable'} {money(item.availableQuantity)}</Badge><div className="mt-1 text-xs text-secondary-text">{zh ? 'T+1 冻结' : 'T+1 frozen'} {money(item.frozenQuantity)}</div>{item.pendingExitReason ? <div className="text-xs text-warning">{zh ? '待卖出' : 'Pending exit'}: {item.pendingExitReason} {item.pendingExitOn || ''} {money(item.pendingExitPrice)}</div> : null}</td><td><div className="text-xs text-secondary-text">{zh ? '风险额' : 'Risk'}: {currencyMoney(item.riskAmount, item.currency)}</div><div className="text-xs text-secondary-text">{zh ? '单股风险' : 'Risk/share'}: {money(item.riskPerShare)}</div><div className="max-w-[220px] text-xs text-secondary-text">{item.sizingReason || '--'}</div><div className="mt-2 text-xs">{zh ? '原始计划' : 'Source plan'} {money(item.sourcePlanStopLoss ?? item.stopLoss)} / {money(item.sourcePlanTargetPrice ?? item.targetPrice)}</div><div className="text-xs font-medium text-primary">{zh ? '当前生效' : 'Effective'} {money(item.effectiveStopLoss ?? item.stopLoss)} / {money(item.effectiveTargetPrice ?? item.targetPrice)}</div>{item.latestRiskRevision ? <div className="text-xs text-secondary-text">{zh ? '修订' : 'Revision'} v{item.latestRiskRevision.revisionVersion ?? '--'} · {zh ? '报告' : 'report'} #{item.latestRiskRevision.sourceReportId ?? '--'} · {item.latestRiskRevision.result}</div> : <div className="text-xs text-secondary-text">{zh ? '尚无后续修订' : 'No later revision'}</div>}</td><td>{item.closedOn ? `${item.closedOn} @ ${money(item.closedPrice)} · ${item.closeReason || '--'}` : (zh ? '未卖出' : 'Not sold')}</td><td className={(item.realizedPnl || 0) >= 0 ? 'text-success' : 'text-danger'}>{currencyMoney(item.realizedPnl ?? item.simulatedPnl, item.currency)}<div className="text-xs text-secondary-text">{zh ? '已卖数量' : 'Sold qty'} {money(item.realizedQuantity)} · {zh ? '费用' : 'Fees'} {currencyMoney(item.realizedFees, item.currency)}</div><div className="text-xs text-secondary-text">{zh ? '累计收益率' : 'Return'} {pct(item.simulatedReturnPct)}</div></td><td><Badge variant={item.syncStatus?.includes('synced') ? 'success' : 'default'}>{label('syncStatus', item.syncStatus || 'paper_only', (zh ? 'zh' : 'en'))}</Badge><div className="mt-2"><button type="button" className="btn-secondary px-2 py-1 text-xs" disabled={running} onClick={() => void syncPaperPositionToPortfolio(item)}>{zh ? '同步至持仓' : 'Sync to portfolio'}</button></div></td></tr>)}</tbody></table></div>
          )}
          <Pager page={pages.positions} total={totals.positions} onPage={(page) => setPage('positions', page)} zh={zh} />
        </Card>
        </div>
      </details>
    </div>
  );
};

export default PaperTradingPage;

import type { PaperPlan, PaperRiskLock } from '../../types/paperTrading';
import { label, type LabelLocale } from './labels';

/**
 * 大盘风险门控口径（只读，来自 /status 的 marketContextRisk）。
 * 这是页面唯一允许陈述“为什么暂停开仓”的依据，缺字段时按保守处理。
 */
export const marketGateFacts = (risk: Record<string, unknown>): {
  controlEnabled: boolean;
  enabled: boolean;
  fresh: boolean;
  entryAllowed: boolean;
  blockNewRisk: boolean;
  regime: string;
  sessionDate: string | null;
  multiplier: number | null;
  reasons: string[];
} => {
  const controlEnabled = risk.controlEnabled === true || risk.marketContextRiskControlEnabled === true;
  const regime = String(risk.regime || 'unavailable');
  const entryAllowed = risk.entryAllowed !== false && risk.blockNewRisk !== true && regime !== 'risk_off' && regime !== 'unavailable';
  return {
    controlEnabled,
    enabled: risk.enabled === true,
    fresh: risk.fresh === true,
    entryAllowed,
    blockNewRisk: risk.blockNewRisk === true,
    regime,
    sessionDate: typeof risk.sessionDate === 'string' ? risk.sessionDate : null,
    multiplier: typeof risk.allocationMultiplier === 'number' ? risk.allocationMultiplier : null,
    reasons: Array.isArray(risk.reasons) ? risk.reasons.map((item) => String(item)) : [],
  };
};

export const lockReasonText = (lock: PaperRiskLock, locale: LabelLocale): string => {
  const reasons = (lock.activeReasons || []).filter(Boolean);
  if (reasons.length === 0) return locale === 'zh' ? '未记录具体原因' : 'No specific reason recorded';
  return reasons.map((item) => label('lockReason', item, locale)).join('、');
};

/** 计划所属的计划日：优先 validForDate，回退 reportDate。 */
export const planSessionDate = (plan: PaperPlan): string | null => (
  (plan.validForDate || plan.reportDate || null) as string | null
);

export type LatestPlanSession = {
  date: string | null;
  plans: PaperPlan[];
  /** 列表按创建时间倒序截取；最旧一条仍属最新计划日且还有未加载记录时，当日计划可能不全。 */
  truncated: boolean;
};

/** 取最新计划日及当日全部计划，供“为什么没有交易”和计划表共用同一口径。 */
export const latestPlanSession = (plans: PaperPlan[], total: number): LatestPlanSession => {
  const dates = plans.map(planSessionDate).filter((item): item is string => Boolean(item)).sort();
  const date = dates.length ? dates[dates.length - 1] : null;
  const oldest = plans.length ? plans[plans.length - 1] : null;
  return {
    date,
    plans: date ? plans.filter((plan) => planSessionDate(plan) === date) : [],
    truncated: date != null && oldest != null && total > plans.length && planSessionDate(oldest) === date,
  };
};

export const planIsBuyLike = (plan: PaperPlan): boolean => {
  const value = String(plan.action || '').toLowerCase();
  return value === 'buy' || value === 'add';
};

/** 计划是否“可执行”：状态为 active 且标记 executable。 */
export const planIsExecutable = (plan: PaperPlan): boolean => plan.status === 'active' && plan.executable === true;

/** 计划是否仍在等待连续交易日确认。 */
export const planAwaitingConfirmation = (plan: PaperPlan): boolean => (
  String(plan.strategyThesisReason || '') === 'awaiting_entry_confirmation'
  || String(plan.strategyThesisState || '') === 'candidate'
);

/**
 * 计划不可执行的中文原因，只取真正决定可执行性的两类来源：
 * 策略论点（含“非看多冻结”）与计划校验器（text 为分号拼接的原因码，reason 固定为 fail_closed）。
 * 入场腿、风格、复核义务等描述性条目不是不可执行原因，不在此列出。
 */
export const planReasonText = (plan: PaperPlan, locale: LabelLocale): string | null => {
  const parts: string[] = [];
  if (plan.strategyThesisReason) parts.push(label('thesisReason', plan.strategyThesisReason, locale));
  (plan.descriptiveConditions || []).forEach((entry) => {
    const source = String(entry?.source || '').trim();
    if (source === 'plan_validator') {
      String(entry?.text || '').split(';').map((item) => item.trim()).filter(Boolean)
        .forEach((item) => parts.push(label('planReason', item, locale)));
      return;
    }
    if (source === 'paper_trading.strategy_thesis') {
      const reason = String(entry?.reason || '').trim();
      if (reason) parts.push(label('thesisReason', reason, locale));
    }
  });
  const unique = Array.from(new Set(parts.filter(Boolean)));
  return unique.length ? unique.join('、') : null;
};

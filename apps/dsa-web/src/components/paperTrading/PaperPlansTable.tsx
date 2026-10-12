import { useState } from 'react';
import type { PaperPlan } from '../../types/paperTrading';
import { Badge } from '../common/Badge';
import { Card } from '../common/Card';
import { EmptyState } from '../common/EmptyState';
import { formatNumber, label, type LabelLocale } from './labels';
import { planAwaitingConfirmation, planIsBuyLike, planIsExecutable, planReasonText, type LatestPlanSession } from './paperFacts';

/** 状态列：可执行 / 确认 k/n / 状态中文；原因列给出具体原因而不是统一话术。 */
const planStateCell = (plan: PaperPlan, locale: LabelLocale) => {
  const zh = locale === 'zh';
  if (planIsExecutable(plan)) {
    return { variant: 'success' as const, text: zh ? '可执行' : 'Executable' };
  }
  if (planAwaitingConfirmation(plan)) {
    const count = plan.strategyThesisConfirmationCount;
    const required = plan.strategyThesisConfirmationRequired;
    return {
      variant: 'warning' as const,
      text: (zh ? '确认 ' : 'Confirm ') + `${count ?? 0}/${required ?? '--'}`,
    };
  }
  const status = String(plan.status || '').toLowerCase();
  if (status && status !== 'active') {
    return { variant: (status === 'expired' || status === 'superseded' ? 'default' : 'warning') as 'default' | 'warning', text: label('planStatus', status, locale) };
  }
  return { variant: 'warning' as const, text: zh ? '不执行' : 'Not executable' };
};

type Props = {
  locale: LabelLocale;
  session: LatestPlanSession;
};

/**
 * 最新计划日的计划：默认只看买入，可切换到全部；
 * 不可执行原因使用计划自身的具体原因，不再统一成一句话。
 */
export const PaperPlansTable: React.FC<Props> = ({ locale, session }) => {
  const zh = locale === 'zh';
  const [showAll, setShowAll] = useState(false);
  const { date: latestDate, plans: sessionPlans, truncated } = session;
  const buyPlans = sessionPlans.filter(planIsBuyLike);
  const rows: PaperPlan[] = showAll ? sessionPlans : buyPlans;

  return (
    <Card
      className="order-[4]"
      padding="none"
      title={latestDate
        ? (zh ? `计划 · ${latestDate}（${sessionPlans.length}）` : `Plans · ${latestDate} (${sessionPlans.length})`)
        : (zh ? '计划（0）' : 'Plans (0)')}
    >
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border px-4 py-3 text-xs text-secondary-text">
        <span className="text-warning">
          {truncated ? (zh ? '计划较多，只显示最近加载的部分。' : 'Many plans; only the most recently loaded ones are shown.') : null}
        </span>
        <div className="flex rounded-xl border border-border bg-surface-2 p-1">
          <button
            type="button"
            className={`rounded-lg px-3 py-1.5 ${showAll ? 'text-secondary-text' : 'bg-primary text-white'}`}
            onClick={() => setShowAll(false)}
          >
            {zh ? `只看买入（${buyPlans.length}）` : `Buy only (${buyPlans.length})`}
          </button>
          <button
            type="button"
            className={`rounded-lg px-3 py-1.5 ${showAll ? 'bg-primary text-white' : 'text-secondary-text'}`}
            onClick={() => setShowAll(true)}
          >
            {zh ? `全部（${sessionPlans.length}）` : `All (${sessionPlans.length})`}
          </button>
        </div>
      </div>

      {rows.length === 0 ? (
        <div className="p-5">
          <EmptyState
            title={zh ? '最新计划日没有买入计划' : 'No buy plan on the latest plan date'}
            description={zh ? '可切换到“全部”查看当天观望、回避等计划及其原因。' : 'Switch to All to see the watch/avoid plans and their reasons for the same session.'}
          />
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[860px] text-left text-sm">
            <thead className="border-b border-border text-xs text-secondary-text">
              <tr>
                <th className="p-4">{zh ? '股票' : 'Stock'}</th>
                <th>{zh ? '建议' : 'Recommendation'}</th>
                <th>{zh ? '入场区间' : 'Entry zone'}</th>
                <th>{zh ? '止损' : 'Stop'}</th>
                <th>{zh ? '目标' : 'Target'}</th>
                <th>{zh ? '状态' : 'State'}</th>
                <th>{zh ? '原因' : 'Reason'}</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((plan) => {
                const state = planStateCell(plan, locale);
                const reason = planReasonText(plan, locale);
                return (
                  <tr key={plan.id} className="border-b border-border/60 align-top">
                    <td className="p-4 font-medium">
                      {plan.stockName || plan.stockCode}
                      <div className="text-xs text-secondary-text">{plan.stockCode} · {plan.sourceModel}</div>
                    </td>
                    <td>{label('action', plan.action, locale)}</td>
                    <td>{plan.entryLow != null || plan.entryHigh != null ? `${formatNumber(plan.entryLow)} – ${formatNumber(plan.entryHigh)}` : '—'}</td>
                    <td>{formatNumber(plan.effectiveStopLoss ?? plan.stopLoss)}</td>
                    <td>{formatNumber(plan.effectiveTargetPrice ?? plan.targetPrice)}</td>
                    <td>
                      <Badge variant={state.variant}>{state.text}</Badge>
                    </td>
                    <td className="max-w-[420px] py-4 text-xs">
                      {reason ?? (planIsExecutable(plan) ? '—' : (zh ? '未记录具体原因' : 'No specific reason recorded'))}
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
    </Card>
  );
};

export default PaperPlansTable;

import type { PaperPosition, PaperRiskLock } from '../../types/paperTrading';
import { Badge } from '../common/Badge';
import { Card } from '../common/Card';
import { label, type LabelLocale } from './labels';
import { lockReasonText, planIsBuyLike, planIsExecutable, planReasonText, type LatestPlanSession } from './paperFacts';

type Props = {
  locale: LabelLocale;
  session: LatestPlanSession;
  openLots: PaperPosition[];
  openLotTotal: number;
  recentClosedLots: PaperPosition[];
  riskLockAccounts: PaperRiskLock[];
  marketRisk: Record<string, unknown>;
  marketGateText: string;
  experimentScope: boolean;
};

/**
 * “为什么没有交易”：只陈述当前数据能证明的事实，
 * 不做数据之外的因果推断（例如“收盘后会重判”不属于事实）。
 */
export const PaperNoTradeFacts: React.FC<Props> = ({
  locale,
  session,
  openLots,
  openLotTotal,
  recentClosedLots,
  riskLockAccounts,
  marketRisk,
  marketGateText,
  experimentScope,
}) => {
  const zh = locale === 'zh';
  const { date: latestDate, plans: sessionPlans, truncated } = session;
  const counts = new Map<string, number>();
  sessionPlans.forEach((plan) => {
    const action = String(plan.action || 'unknown').toLowerCase();
    counts.set(action, (counts.get(action) || 0) + 1);
  });
  const buyPlans = sessionPlans.filter(planIsBuyLike);
  const executable = sessionPlans.filter(planIsExecutable);
  const executableBuys = buyPlans.filter(planIsExecutable);
  const blockedBuyReasons = new Map<string, number>();
  buyPlans.filter((plan) => !planIsExecutable(plan)).forEach((plan) => {
    const reason = planReasonText(plan, locale);
    if (!reason) return;
    blockedBuyReasons.set(reason, (blockedBuyReasons.get(reason) || 0) + 1);
  });
  // 开仓与加仓都会生成新批次，因此按批次开仓日统计当日买入成交。
  const openedOnSession = latestDate
    ? [...openLots, ...recentClosedLots].filter((lot) => lot.openedOn === latestDate).length
    : 0;
  const title = latestDate
    ? (openedOnSession > 0
      ? (zh ? `交易事实（计划日 ${latestDate}）` : `Trading facts (plan date ${latestDate})`)
      : (zh ? `为什么没有交易（计划日 ${latestDate}）` : `Why no trade (plan date ${latestDate})`))
    : (zh ? '为什么没有交易' : 'Why no trade');
  const actionSummary = Array.from(counts.entries())
    .sort((left, right) => right[1] - left[1])
    .map(([action, count]) => `${label('action', action, locale)} ${count}`)
    .join(' · ');

  return (
    <Card className="order-[2]" title={title} padding="none">
      <ol className="list-decimal space-y-2 py-5 pl-10 pr-5 text-sm">
        <li>
          {latestDate ? (
            zh
              ? `共 ${sessionPlans.length} 个计划（${actionSummary || '未记录建议'}），其中买入 ${buyPlans.length} 个、可执行 ${executable.length} 个（买入且可执行 ${executableBuys.length} 个）。`
              : `${sessionPlans.length} plan(s) (${actionSummary || 'no action recorded'}), of which ${buyPlans.length} buy/add and ${executable.length} executable (${executableBuys.length} buy and executable).`
          ) : (
            zh ? '当前没有计划记录，无法判断计划日。' : 'No plan record loaded, so no plan date can be determined.'
          )}
          {truncated ? (
            <span className="ml-1 text-warning">{zh ? '（计划较多，只统计了最近加载的部分）' : '(many plans; only the most recently loaded ones are counted)'}</span>
          ) : null}
        </li>
        <li>
          {zh ? '大盘门控：' : 'Market gate: '}{marketGateText}
        </li>
        {blockedBuyReasons.size > 0 ? (
          <li>
            {zh ? '买入但不可执行的原因：' : 'Why buys are not executable: '}
            {Array.from(blockedBuyReasons.entries()).map(([reason, count]) => `${reason} ×${count}`).join('；')}
          </li>
        ) : null}
        <li className="text-secondary-text">
          {zh ? '账户风险锁：' : 'Account risk locks: '}
          {experimentScope
            ? (zh ? '实验分组不读取主库风险锁' : 'Not read in experiment scope')
            : riskLockAccounts.length === 0
              ? (zh ? '无' : 'None')
              : riskLockAccounts.map((lock) => `${lock.accountKey || lock.sourceModel || `#${lock.accountId}`} ${label('lockState', lock.state || lock.status, locale)}（${lockReasonText(lock, locale)}）`).join('；')}
        </li>
        <li className="text-secondary-text">
          {zh
            ? `当前未平仓批次 ${openLotTotal} 个；${latestDate || '—'} 新开批次 ${openedOnSession} 个。`
            : `${openLotTotal} open lot(s) currently; ${openedOnSession} lot(s) opened on ${latestDate || '—'}.`}
        </li>
      </ol>
      <div className="border-t border-border/60 px-5 py-3 text-xs text-secondary-text">
        {zh
          ? '以上只列出计划、门控、风险锁与批次的实际取值，不推断数据之外的因果。'
          : 'These lines only restate the actual plan, gate, lock and lot values; no causality beyond the data is inferred.'}
        {!experimentScope && marketRisk.enabled !== true ? (
          <span className="ml-2"><Badge variant="default">{zh ? '大盘上下文未启用' : 'Market context disabled'}</Badge></span>
        ) : null}
      </div>
    </Card>
  );
};

export default PaperNoTradeFacts;

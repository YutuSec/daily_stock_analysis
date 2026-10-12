import type { PaperRiskSnapshot } from '../../types/paperTrading';
import { Badge } from '../common/Badge';
import { Card } from '../common/Card';
import { formatMoney, formatPercent, type LabelLocale } from './labels';

export type CurrencyTotals = {
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
};

type Props = {
  locale: LabelLocale;
  totals: CurrencyTotals[];
  /** 仅包含“该币种下只有一个账户”的风险快照；多账户币种不在此表中。 */
  riskSnapshotByCurrency: Map<string, PaperRiskSnapshot>;
  experimentScope: boolean;
};

/** 有值才着色，缺值（—）保持中性色。 */
const pnlClass = (value: number | null | undefined) => {
  if (value == null) return 'text-secondary-text';
  return value >= 0 ? 'text-success' : 'text-danger';
};

/**
 * 账户概览：每个币种一行，权益标明口径；当日盈亏与回撤只取账户风险快照。
 * 不跨币种求和，取不到的值显示 “—”，不推测。大盘门控与风险锁见“为什么没有交易”卡片。
 */
export const PaperAccountOverview: React.FC<Props> = ({
  locale,
  totals,
  riskSnapshotByCurrency,
  experimentScope,
}) => {
  const zh = locale === 'zh';

  return (
    <Card className="order-[1]" title={zh ? '账户概览' : 'Account overview'} padding="none">
      {totals.length === 0 ? (
        <div className="p-5 text-sm text-secondary-text">
          {zh ? '当前模型 / 执行范围下没有资金账户。' : 'No capital account under the current model / scope.'}
        </div>
      ) : (
        <div className="divide-y divide-border/60">
          {totals.map((item) => {
            const valuationComplete = item.valuationComplete !== false;
            const equity = valuationComplete ? item.marketEquity : item.bookEquity;
            const basisText = valuationComplete
              ? (zh ? '市值口径' : 'at market')
              : (zh ? '成本口径（存在缺价持仓）' : 'at cost (unpriced holdings)');
            const base = item.initialCapital + item.externalCashFlow;
            const returnPct = base > 0 ? ((equity - base) / base) * 100 : null;
            const positionPct = equity > 0 ? ((item.marketValue || 0) / equity) * 100 : null;
            const snapshot = riskSnapshotByCurrency.get(item.currency) ?? null;
            const unrealized = valuationComplete ? item.unrealizedPnl : null;
            const dailyPnl = valuationComplete && snapshot && typeof snapshot.dailyPnl === 'number' ? snapshot.dailyPnl : null;
            const drawdownPct = valuationComplete && snapshot && typeof snapshot.drawdownPct === 'number' ? snapshot.drawdownPct : null;
            return (
              <div key={item.currency} className="p-5">
                <div className="flex flex-wrap items-baseline gap-x-6 gap-y-2 text-sm">
                  <div>
                    <span className="text-xs text-secondary-text">{zh ? '权益' : 'Equity'}（{basisText}）</span>
                    <p className="text-2xl font-semibold">{formatMoney(equity, item.currency)}</p>
                  </div>
                  <div>
                    <span className="text-xs text-secondary-text">{zh ? '相对初始资金收益率' : 'Return vs. initial capital'}</span>
                    <p className={`text-lg font-semibold ${pnlClass(returnPct)}`}>{formatPercent(returnPct)}</p>
                  </div>
                  <div>
                    <span className="text-xs text-secondary-text">{zh ? '现金' : 'Cash'}</span>
                    <p className="text-lg font-semibold">{formatMoney(item.cash, item.currency)}</p>
                  </div>
                  <div>
                    <span className="text-xs text-secondary-text">{zh ? '持仓市值' : 'Position value'}</span>
                    <p className="text-lg font-semibold">{valuationComplete ? formatMoney(item.marketValue, item.currency) : '—'}</p>
                  </div>
                  <div>
                    <span className="text-xs text-secondary-text">{zh ? '仓位' : 'Position ratio'}</span>
                    <p className="text-lg font-semibold">{valuationComplete ? formatPercent(positionPct) : '—'}</p>
                  </div>
                </div>

                <div className="mt-3 flex flex-wrap gap-x-6 gap-y-2 text-sm">
                  <span className={pnlClass(item.realizedPnl)}>
                    {zh ? '已实现' : 'Realized'} {formatMoney(item.realizedPnl, item.currency)}
                  </span>
                  <span className={pnlClass(unrealized)}>
                    {zh ? '浮动' : 'Unrealized'} {unrealized == null ? '—' : formatMoney(unrealized, item.currency)}
                  </span>
                  <span className={pnlClass(dailyPnl)}>
                    {zh ? '当日盈亏' : 'Session PnL'} {dailyPnl == null ? '—' : formatMoney(dailyPnl, item.currency)}
                  </span>
                  <span className="text-secondary-text">
                    {zh ? '回撤' : 'Drawdown'} {drawdownPct == null ? '—' : formatPercent(drawdownPct)}
                  </span>
                  {snapshot?.sessionDate ? (
                    <span className="text-xs text-secondary-text">
                      {zh ? `风险快照 ${snapshot.sessionDate}` : `Risk snapshot ${snapshot.sessionDate}`}
                    </span>
                  ) : null}
                  {item.valuationStale ? (
                    <span className="text-xs text-warning">{zh ? '估值行情过期' : 'Valuation mark stale'}</span>
                  ) : null}
                </div>
              </div>
            );
          })}
        </div>
      )}
      <div className="border-t border-border/60 px-5 py-3 text-xs text-secondary-text">
        {zh
          ? '不跨币种求和。当日盈亏与回撤取自账户最新风险快照（日期见“风险快照”）：缺快照或同币种有多个模型账户时显示 —，可选单个模型查看。'
          : 'Never summed across currencies. Session PnL and drawdown come from the latest account risk snapshot (dated next to them) and show — when it is missing or several model accounts share the currency; pick a single model to see them.'}
        {experimentScope ? (
          <span className="ml-1">
            <Badge variant="warning">{zh ? '实验分组：主库风险快照不适用' : 'Experiment scope: primary-DB risk snapshots do not apply'}</Badge>
          </span>
        ) : null}
      </div>
    </Card>
  );
};

export default PaperAccountOverview;

import type { PaperHolding, PaperPosition } from '../../types/paperTrading';
import { Badge } from '../common/Badge';
import { Card } from '../common/Card';
import { EmptyState } from '../common/EmptyState';
import { formatMoney, formatNumber, formatPercent, label, type LabelLocale } from './labels';

/** 批次成本口径：优先剩余成本，其次分配资金，最后按开仓价 × 数量。 */
const lotCostBasis = (lot: PaperPosition): number | null => {
  if (typeof lot.remainingCost === 'number' && lot.remainingCost >= 0) return lot.remainingCost;
  if (typeof lot.allocatedCapital === 'number' && lot.allocatedCapital > 0) return lot.allocatedCapital;
  if (typeof lot.openedPrice === 'number' && typeof lot.quantity === 'number') return lot.openedPrice * lot.quantity;
  return null;
};

const holdingKey = (value: { sourceModel?: string | null; market?: string | null; stockCode?: string | null; currency?: string | null }) => (
  `${value.sourceModel || ''}|${value.market || ''}|${value.stockCode || ''}|${value.currency || ''}`
);

const pnlClass = (value: number | null | undefined) => {
  if (value == null) return '';
  return value >= 0 ? 'text-success' : 'text-danger';
};

type Props = {
  locale: LabelLocale;
  openLots: PaperPosition[];
  openTotal: number;
  closedLots: PaperPosition[];
  closedTotal: number;
  holdings: PaperHolding[];
};

const formatHeldSessions = (lot: PaperPosition, locale: LabelLocale) => {
  const held = typeof lot.heldSessions === 'number' ? String(lot.heldSessions) : '—';
  // 批次自身冻结的上限优先，它才是收盘时间退出实际使用的值。
  const max = typeof lot.maxHoldingSessions === 'number'
    ? String(lot.maxHoldingSessions)
    : (lot.tradingStyleSettings && typeof lot.tradingStyleSettings.maxHoldingSessions === 'number'
      ? String(lot.tradingStyleSettings.maxHoldingSessions)
      : '—');
  const suffix = locale === 'zh' ? '交易日' : ' sessions';
  return `${held} / ${max}${suffix}`;
};

/**
 * 批次表：未平仓一张表回答“持有什么、成本多少、现在赚亏多少、止损止盈在哪、还能拿多久”；
 * 最近已平仓默认折叠，只看开平仓与已实现盈亏，不再套用现价 / 浮盈列。
 */
export const PaperLotsTable: React.FC<Props> = ({
  locale,
  openLots,
  openTotal,
  closedLots,
  closedTotal,
  holdings,
}) => {
  const zh = locale === 'zh';
  const holdingIndex = new Map(holdings.map((item) => [holdingKey(item), item]));

  const stockCell = (lot: PaperPosition) => (
    <td className="p-4 font-medium">
      {lot.stockName || lot.stockCode}
      <div className="text-xs text-secondary-text">{lot.stockCode} · {lot.market}</div>
    </td>
  );

  const sourceCell = (lot: PaperPosition) => {
    const sourceKind = lot.sourceKind ?? null;
    return (
      <td>
        <Badge variant={sourceKind === 'market_opportunity' ? 'info' : 'default'}>{label('source', sourceKind, locale)}</Badge>
        {lot.tradingStyle ? <div className="mt-1 text-xs text-secondary-text">{label('style', lot.tradingStyle, locale)}</div> : null}
      </td>
    );
  };

  const renderOpenRow = (lot: PaperPosition) => {
    const holding = holdingIndex.get(holdingKey(lot));
    const mark = holding?.latestPrice ?? null;
    const cost = lotCostBasis(lot);
    const quantity = typeof lot.quantity === 'number' ? lot.quantity : null;
    const pnl = mark != null && cost != null && quantity != null ? mark * quantity - cost : null;
    const pnlPct = pnl != null && cost ? (pnl / cost) * 100 : null;
    return (
      <tr key={lot.id} className="border-b border-border/60 align-top">
        {stockCell(lot)}
        {sourceCell(lot)}
        <td>{formatNumber(quantity)}</td>
        <td>{formatMoney(cost, lot.currency)}</td>
        <td>
          {formatNumber(mark)}
          {mark != null && holding?.markStale ? <div className="text-xs text-warning">{zh ? '行情过期' : 'Mark stale'}</div> : null}
        </td>
        <td className={pnlClass(pnl)}>
          {formatMoney(pnl, lot.currency)}
          <div className="text-xs text-secondary-text">{formatPercent(pnlPct)}</div>
        </td>
        <td>{formatNumber(lot.effectiveStopLoss ?? lot.stopLoss ?? lot.sourcePlanStopLoss)}</td>
        <td>{formatNumber(lot.effectiveTargetPrice ?? lot.targetPrice ?? lot.sourcePlanTargetPrice)}</td>
        <td>{formatHeldSessions(lot, locale)}</td>
      </tr>
    );
  };

  const renderClosedRow = (lot: PaperPosition) => {
    const realized = lot.realizedPnl ?? lot.simulatedPnl ?? null;
    return (
      <tr key={lot.id} className="border-b border-border/60 align-top">
        {stockCell(lot)}
        {sourceCell(lot)}
        <td>{lot.openedOn || '—'}<div className="text-xs text-secondary-text">@ {formatNumber(lot.openedPrice)}</div></td>
        <td>{lot.closedOn || '—'}<div className="text-xs text-secondary-text">@ {formatNumber(lot.closedPrice)}</div></td>
        <td>{lot.closeReason ? label('closeReason', lot.closeReason, locale) : '—'}</td>
        <td className={pnlClass(realized)}>
          {formatMoney(realized, lot.currency)}
          <div className="text-xs text-secondary-text">{formatPercent(lot.simulatedReturnPct)}</div>
        </td>
        <td>{formatHeldSessions(lot, locale)}</td>
      </tr>
    );
  };

  return (
    <Card
      className="order-[3]"
      padding="none"
      title={zh ? `持仓（${openTotal}）` : `Open lots (${openTotal})`}
    >
      {openLots.length === 0 ? (
        <div className="p-5">
          <EmptyState
            title={zh ? '当前没有未平仓批次' : 'No open lot'}
            description={zh ? '只有全部白名单条件成立才会模拟买入并生成批次。' : 'A paper lot is created only after every whitelisted condition passes.'}
          />
        </div>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full min-w-[860px] text-left text-sm">
            <thead className="border-b border-border text-xs text-secondary-text">
              <tr>
                <th className="p-4">{zh ? '股票' : 'Stock'}</th>
                <th>{zh ? '来源 / 风格' : 'Source / style'}</th>
                <th>{zh ? '数量' : 'Quantity'}</th>
                <th>{zh ? '成本' : 'Cost'}</th>
                <th>{zh ? '现价' : 'Price'}</th>
                <th>{zh ? '浮盈' : 'Unrealized'}</th>
                <th>{zh ? '止损' : 'Stop'}</th>
                <th>{zh ? '止盈' : 'Target'}</th>
                <th>{zh ? '持有 / 上限' : 'Held / max'}</th>
              </tr>
            </thead>
            <tbody>{openLots.map(renderOpenRow)}</tbody>
          </table>
          {openTotal > openLots.length ? (
            <p className="px-4 py-3 text-xs text-warning">
              {zh ? `仅显示前 ${openLots.length} 个，共 ${openTotal} 个未平仓批次。` : `Showing ${openLots.length} of ${openTotal} open lots.`}
            </p>
          ) : null}
        </div>
      )}

      <details className="border-t border-border/60">
        <summary className="cursor-pointer px-4 py-3 text-sm text-secondary-text">
          {zh
            ? `最近已平仓（显示 ${closedLots.length} / 共 ${closedTotal} 笔，完整记录见下方批次审计）`
            : `Recently closed (${closedLots.length} of ${closedTotal}; full history in the lot audit below)`}
        </summary>
        {closedLots.length === 0 ? (
          <p className="px-4 pb-4 text-sm text-secondary-text">{zh ? '还没有已平仓批次。' : 'No closed lot yet.'}</p>
        ) : (
          <div className="overflow-x-auto px-1 pb-3">
            <table className="w-full min-w-[760px] text-left text-sm">
              <thead className="border-b border-border text-xs text-secondary-text">
                <tr>
                  <th className="p-4">{zh ? '股票' : 'Stock'}</th>
                  <th>{zh ? '来源 / 风格' : 'Source / style'}</th>
                  <th>{zh ? '开仓' : 'Opened'}</th>
                  <th>{zh ? '平仓' : 'Closed'}</th>
                  <th>{zh ? '平仓原因' : 'Close reason'}</th>
                  <th>{zh ? '已实现盈亏' : 'Realized PnL'}</th>
                  <th>{zh ? '持有 / 上限' : 'Held / max'}</th>
                </tr>
              </thead>
              <tbody>{closedLots.map(renderClosedRow)}</tbody>
            </table>
          </div>
        )}
      </details>
    </Card>
  );
};

export default PaperLotsTable;

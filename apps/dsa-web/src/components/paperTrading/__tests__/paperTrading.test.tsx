import { fireEvent, render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';
import type { PaperHolding, PaperPlan, PaperPosition, PaperRiskLock, PaperRiskSnapshot } from '../../../types/paperTrading';
import { label } from '../labels';
import { PaperAccountOverview, type CurrencyTotals } from '../PaperAccountOverview';
import { PaperLotsTable } from '../PaperLotsTable';
import { PaperNoTradeFacts } from '../PaperNoTradeFacts';
import { PaperPlansTable } from '../PaperPlansTable';
import { latestPlanSession, planReasonText } from '../paperFacts';

const basePlan: PaperPlan = {
  id: 1,
  sourceReportId: 10,
  sourceModel: 'deepseek/deepseek-v4-pro',
  market: 'cn',
  stockCode: '600803',
  stockName: '新奥股份',
  reportDate: '2026-10-08',
  validForDate: '2026-10-09',
  action: 'buy',
  executable: false,
  status: 'active',
};

const baseLot: PaperPosition = {
  id: 7,
  sourceReportId: 10,
  sourceModel: 'deepseek/deepseek-v4-pro',
  market: 'cn',
  stockCode: '600803',
  stockName: '新奥股份',
  status: 'open',
  openedOn: '2026-10-08',
  openedPrice: 10,
  normalizedNotional: 1000,
  quantity: 100,
  remainingCost: 1000,
  currency: 'CNY',
  sourceKind: 'market_opportunity',
  tradingStyle: 'short_swing',
  heldSessions: 1,
  maxHoldingSessions: 15,
  tradingStyleSettings: { maxHoldingSessions: 60 },
  effectiveStopLoss: 9.5,
  effectiveTargetPrice: 11.2,
};

const baseTotals: CurrencyTotals = {
  currency: 'CNY',
  initialCapital: 100000,
  bookEquity: 101000,
  externalCashFlow: 0,
  cash: 90000,
  investedCapital: 10000,
  realizedPnl: 500,
  unrealizedPnl: 500,
  totalPnl: 1000,
  marketValue: 11000,
  exposure: 11000,
  marketEquity: 101000,
  valuationComplete: true,
  unpricedPositionCount: 0,
  unpricedHoldingCount: 0,
  valuationObservedAt: null,
  valuationStale: false,
  openPositionCount: 1,
};

describe('planReasonText', () => {
  it('translates validator codes and thesis reasons while skipping descriptive notes', () => {
    const plan: PaperPlan = {
      ...basePlan,
      strategyThesisReason: 'accepted_non_bullish_freezes_unfilled_entry',
      descriptiveConditions: [
        { source: 'paper_trading.entry_legs', text: 'legs', reason: 'descriptive_only' },
        { source: 'plan_validator', text: 'missing_valid_stop_loss;risk_reward_below_policy', reason: 'fail_closed' },
        { source: 'paper_trading.next_check_obligation', text: 'review', reason: 'audit_context_only' },
      ],
    };
    expect(planReasonText(plan, 'zh')).toBe('结论不是看多，未成交的入场已冻结、缺少有效止损、盈亏比低于下限');
  });

  it('returns null when no executability reason is recorded', () => {
    expect(planReasonText({ ...basePlan, descriptiveConditions: [{ source: 'paper_trading.trading_style', reason: 'descriptive_only' }] }, 'zh')).toBeNull();
  });
});

describe('latestPlanSession', () => {
  it('keeps only the latest plan date and flags a possibly cut-off session', () => {
    const plans = [
      { ...basePlan, id: 3 },
      { ...basePlan, id: 2 },
      { ...basePlan, id: 1, validForDate: '2026-10-08' },
    ];
    const complete = latestPlanSession(plans, 3);
    expect(complete.date).toBe('2026-10-09');
    expect(complete.plans.map((plan) => plan.id)).toEqual([3, 2]);
    expect(complete.truncated).toBe(false);

    // 最旧一条已是更早计划日：即使后端还有更多记录，最新计划日也已取全。
    expect(latestPlanSession(plans, 30).truncated).toBe(false);
    // 最旧一条仍属最新计划日且还有更多记录：当日计划可能不全。
    expect(latestPlanSession(plans.slice(0, 2), 30).truncated).toBe(true);
  });
});

describe('label', () => {
  it('keeps unknown codes visible and marks empty values as not recorded', () => {
    expect(label('syncStatus', 'paper_only', 'zh')).toBe('仅模拟（未同步）');
    expect(label('planReason', 'brand_new_code', 'zh')).toBe('brand_new_code');
    expect(label('action', null, 'zh')).toBe('未记录');
  });
});

describe('PaperPlansTable', () => {
  it('shows only buy plans of the latest plan date until switched to all', () => {
    const plans: PaperPlan[] = [
      { ...basePlan, id: 1, executable: true, strategyThesisReason: 'accepted_entry_thesis' },
      { ...basePlan, id: 2, stockCode: '600028', stockName: '中国石化', action: 'watch', descriptiveConditions: [{ source: 'plan_validator', text: 'unsupported_or_non_bullish_action', reason: 'fail_closed' }] },
      { ...basePlan, id: 3, stockCode: '000001', stockName: '平安银行', validForDate: '2026-10-08' },
    ];
    render(<PaperPlansTable locale="zh" session={latestPlanSession(plans, plans.length)} />);

    expect(screen.getByText('计划 · 2026-10-09（2）')).toBeInTheDocument();
    expect(screen.getByText('新奥股份')).toBeInTheDocument();
    expect(screen.getByText('可执行')).toBeInTheDocument();
    expect(screen.queryByText('中国石化')).not.toBeInTheDocument();
    expect(screen.queryByText('平安银行')).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: '全部（2）' }));
    const row = screen.getByText('中国石化').closest('tr') as HTMLElement;
    expect(within(row).getByText('结论不是买入')).toBeInTheDocument();
    expect(screen.queryByText('平安银行')).not.toBeInTheDocument();
  });
});

describe('PaperLotsTable', () => {
  it('derives unrealized PnL from the holding mark and prefers the lot frozen holding cap', () => {
    const holding = {
      sourceModel: 'deepseek/deepseek-v4-pro',
      market: 'cn',
      stockCode: '600803',
      currency: 'CNY',
      totalQuantity: 100,
      availableQuantity: 0,
      frozenQuantity: 100,
      totalCost: 1000,
      lotCount: 1,
      latestPrice: 10.5,
    } as PaperHolding;
    render(<PaperLotsTable locale="zh" openLots={[baseLot]} openTotal={1} closedLots={[]} closedTotal={0} holdings={[holding]} />);

    const row = screen.getByText('新奥股份').closest('tr') as HTMLElement;
    expect(within(row).getByText('机会池')).toBeInTheDocument();
    expect(within(row).getByText('1 / 15交易日')).toBeInTheDocument();
    expect(within(row).getByText('CNY 50.00')).toHaveClass('text-success');
  });

  it('shows closed lots with realized PnL and a translated close reason instead of a live mark', () => {
    const closed: PaperPosition = {
      ...baseLot,
      id: 8,
      status: 'closed',
      closedOn: '2026-10-09',
      closedPrice: 9.4,
      closeReason: 'expiry',
      realizedPnl: -60,
      simulatedReturnPct: -6,
      heldSessions: 15,
    };
    // 同一标的仍有估值时，已平仓行也不能借用现价去算浮盈。
    const holding = { sourceModel: baseLot.sourceModel, market: 'cn', stockCode: '600803', currency: 'CNY', latestPrice: 12 } as PaperHolding;
    render(<PaperLotsTable locale="zh" openLots={[]} openTotal={0} closedLots={[closed]} closedTotal={5} holdings={[holding]} />);

    expect(screen.getByText('当前没有未平仓批次')).toBeInTheDocument();
    expect(screen.getByText(/最近已平仓（显示 1 \/ 共 5 笔/)).toBeInTheDocument();
    const row = screen.getByText('新奥股份').closest('tr') as HTMLElement;
    expect(within(row).getByText('到期退出（持有上限）')).toBeInTheDocument();
    expect(within(row).getByText('CNY -60.00')).toHaveClass('text-danger');
    expect(within(row).queryByText(/12\.00/)).not.toBeInTheDocument();
  });
});

describe('PaperNoTradeFacts', () => {
  const lock: PaperRiskLock = {
    id: 1,
    accountId: 9,
    accountKey: 'm:cn:CNY',
    sourceModel: 'm',
    market: 'cn',
    currency: 'CNY',
    state: 'reduce_only',
    activeReasons: ['daily_loss_breached'],
  };

  it('states plan counts, blocked reasons and lots opened on the plan date without guessing', () => {
    const plans: PaperPlan[] = [
      { ...basePlan, id: 1, descriptiveConditions: [{ source: 'plan_validator', text: 'missing_valid_stop_loss', reason: 'fail_closed' }] },
      { ...basePlan, id: 2, action: 'watch' },
    ];
    render(
      <PaperNoTradeFacts
        locale="zh"
        session={latestPlanSession(plans, plans.length)}
        openLots={[baseLot]}
        openLotTotal={1}
        recentClosedLots={[]}
        riskLockAccounts={[lock]}
        marketRisk={{ enabled: true }}
        marketGateText="新开仓 / 加仓允许。"
        experimentScope={false}
      />,
    );

    expect(screen.getByText('为什么没有交易（计划日 2026-10-09）')).toBeInTheDocument();
    expect(screen.getByText(/共 2 个计划.*其中买入 1 个、可执行 0 个/)).toBeInTheDocument();
    expect(screen.getByText(/缺少有效止损 ×1/)).toBeInTheDocument();
    expect(screen.getByText(/m:cn:CNY 只减仓（触发单日亏损上限）/)).toBeInTheDocument();
    expect(screen.getByText(/当前未平仓批次 1 个；2026-10-09 新开批次 0 个/)).toBeInTheDocument();
  });

  it('does not show primary-DB risk locks in an experiment scope', () => {
    render(
      <PaperNoTradeFacts
        locale="zh"
        session={latestPlanSession([basePlan], 1)}
        openLots={[{ ...baseLot, openedOn: '2026-10-09' }]}
        openLotTotal={1}
        recentClosedLots={[]}
        riskLockAccounts={[lock]}
        marketRisk={{}}
        marketGateText="实验分组不读取主库大盘门控。"
        experimentScope
      />,
    );

    expect(screen.getByText('交易事实（计划日 2026-10-09）')).toBeInTheDocument();
    expect(screen.getByText(/实验分组不读取主库风险锁/)).toBeInTheDocument();
    expect(screen.queryByText(/只减仓/)).not.toBeInTheDocument();
    expect(screen.queryByText('大盘上下文未启用')).not.toBeInTheDocument();
  });
});

describe('PaperAccountOverview', () => {
  it('shows snapshot-based today PnL and drawdown only when a single-account snapshot exists', () => {
    const snapshot: PaperRiskSnapshot = {
      id: 1,
      accountId: 9,
      sourceModel: 'm',
      market: 'cn',
      currency: 'CNY',
      sessionDate: '2026-10-09',
      dailyPnl: -120,
      drawdownPct: 1.5,
    };
    const { rerender } = render(
      <PaperAccountOverview locale="zh" totals={[baseTotals]} riskSnapshotByCurrency={new Map([['CNY', snapshot]])} experimentScope={false} />,
    );
    expect(screen.getByText('当日盈亏 CNY -120.00')).toHaveClass('text-danger');
    expect(screen.getByText('回撤 1.50%')).toBeInTheDocument();
    expect(screen.getByText('风险快照 2026-10-09')).toBeInTheDocument();

    rerender(<PaperAccountOverview locale="zh" totals={[baseTotals]} riskSnapshotByCurrency={new Map()} experimentScope={false} />);
    expect(screen.getByText('当日盈亏 —')).toHaveClass('text-secondary-text');
    expect(screen.getByText('回撤 —')).toBeInTheDocument();
  });

  it('falls back to book equity and hides market-based numbers when valuation is incomplete', () => {
    render(
      <PaperAccountOverview
        locale="zh"
        totals={[{ ...baseTotals, valuationComplete: false, marketEquity: 0, marketValue: 0, unrealizedPnl: 0 }]}
        riskSnapshotByCurrency={new Map()}
        experimentScope={false}
      />,
    );
    expect(screen.getByText('权益（成本口径（存在缺价持仓））')).toBeInTheDocument();
    expect(screen.getByText('CNY 101,000.00')).toBeInTheDocument();
    expect(screen.getByText('浮动 —')).toHaveClass('text-secondary-text');
  });
});

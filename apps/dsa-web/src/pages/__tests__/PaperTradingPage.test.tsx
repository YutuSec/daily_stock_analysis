import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { UiLanguageProvider } from '../../contexts/UiLanguageContext';
import { toCamelCase } from '../../api/utils';
import PaperTradingPage from '../PaperTradingPage';

const mocks = vi.hoisted(() => ({
  getModels: vi.fn(), getStatus: vi.fn(), getPlans: vi.fn(), getOperations: vi.fn(), getPositions: vi.fn(),
  getEvents: vi.fn(), getAccounts: vi.fn(), getAdjustments: vi.fn(), getHoldings: vi.fn(), createAccount: vi.fn(), adjustAccount: vi.fn(),
  getResolutions: vi.fn(), getRiskRevisions: vi.fn(), getDispatches: vi.fn(), getEvaluations: vi.fn(), getObservations: vi.fn(),
  getReanalysisRequests: vi.fn(), getAuctionObservations: vi.fn(), getRiskSnapshots: vi.fn(), getRiskLocks: vi.fn(), getRiskEvents: vi.fn(),
  getReportPlanReconciliations: vi.fn(),
  manualHold: vi.fn(), manualResume: vi.fn(),
  getPerformance: vi.fn(), run: vi.fn(), syncToPortfolio: vi.fn(), getPortfolioAccounts: vi.fn(), getScopes: vi.fn(),
}));

vi.mock('../../api/analysis', () => ({ analysisApi: { getModels: mocks.getModels } }));
vi.mock('../../api/paperTrading', () => ({
  paperTradingApi: {
    getStatus: mocks.getStatus,
    getAccounts: mocks.getAccounts,
    getAdjustments: mocks.getAdjustments,
    getHoldings: mocks.getHoldings,
    createAccount: mocks.createAccount,
    adjustAccount: mocks.adjustAccount,
    getPlans: mocks.getPlans,
    getOperations: mocks.getOperations,
    getPositions: mocks.getPositions,
    getEvents: mocks.getEvents,
    getResolutions: mocks.getResolutions,
    getRiskRevisions: mocks.getRiskRevisions,
    getDispatches: mocks.getDispatches,
    getEvaluations: mocks.getEvaluations,
    getObservations: mocks.getObservations,
    getReanalysisRequests: mocks.getReanalysisRequests,
    getReportPlanReconciliations: mocks.getReportPlanReconciliations,
    getAuctionObservations: mocks.getAuctionObservations,
    getRiskSnapshots: mocks.getRiskSnapshots,
    getRiskLocks: mocks.getRiskLocks,
    getRiskEvents: mocks.getRiskEvents,
    manualHold: mocks.manualHold,
    manualResume: mocks.manualResume,
    getPerformance: mocks.getPerformance,
    run: mocks.run,
    syncToPortfolio: mocks.syncToPortfolio,
    getScopes: mocks.getScopes,
  },
}));
vi.mock('../../api/portfolio', () => ({
  portfolioApi: {
    getAccounts: mocks.getPortfolioAccounts,
  },
}));

beforeEach(() => {
  vi.clearAllMocks();
  mocks.getModels.mockResolvedValue({ models: [
    { model: 'model-a', isDefault: true }, { model: 'model-b', isDefault: false },
  ], supportedMarkets: ['cn', 'hk', 'us', 'jp'] });
  mocks.getStatus.mockResolvedValue({
    enabled: true,
    intradayEnabled: true,
    intervalMinutes: 5,
    intervalSeconds: 300,
    minFactorDomains: 2,
    minCompositeScore: 0.6,
    minRiskReward: 1.2,
    maxQuoteStaleSeconds: 600,
    tradingStyle: 'position',
    tradingStyleSettings: {
      analysisWindowSessions: 180,
      expectedHoldingSessionsRange: [10, 40],
      maxHoldingSessions: 60,
      entryLegLimit: 2,
      addLimit: 1,
      riskMultiplier: 1,
      llmEventCooldownMinutes: 240,
      llmDailyCap: 1,
      planRefreshCadenceSessions: 2,
    },
    planCompensationEnabled: true,
    planCompensationLookbackDays: 30,
    marketContextRiskControlEnabled: true,
    marketContextLookbackDays: 3,
    marketContextRisk: {
      scope: { market: 'cn', source: 'daily_market_regimes', model: null },
      sourceReportId: 314,
      contextId: 55,
      observedAt: '2026-08-21T01:00:00',
      market: 'cn',
      sessionDate: '2026-08-21',
      regime: 'cautious',
      policyVersion: 'market-regime-v1',
      source: 'daily_market_report',
      provenance: { parser: 'structured' },
      blockNewRisk: false,
      allocationMultiplier: 0.5,
      reasons: ['market_context_cautious'],
      fresh: true,
      entryAllowed: true,
    },
    slippagePct: 0.1,
    commissionRate: 0.03,
    minCommission: 5,
    sellTaxRate: 0.05,
    initialCapital: 100000,
    riskPerTradePct: 1,
    maxPositionPct: 20,
    maxSymbolExposurePct: 50,
    assetExposureCaps: { individualStock: 20, themeEtf: 30, broadEtf: 40, unknown: 20 },
    assetClassOverrides: { 'cn:510300': 'broad_etf' },
    maxTotalExposurePct: 80,
    maxOpenPositions: 5,
    feishuNotifyEnabled: true,
    feishuRouteConfigured: true,
    schedulerEnabled: true,
    schedulerRunning: false,
    localSchedulerEnabled: false,
    localSchedulerRunning: false,
    schedulerHeartbeatFresh: true,
    schedulerTaskStates: {
      'daily_analysis:15:10': {
        heartbeat: {
          fresh: true,
          status: 'idle',
          ownerId: 'analyse-daily',
          ownerKind: 'analyse',
          observedAt: '2026-08-21T07:09:55',
          ageSeconds: 5,
          lastSuccessAt: '2026-08-20T07:11:00',
          lastErrorAt: null,
          errorMessage: null,
        },
        lease: {
          active: false,
          status: 'completed',
          ownerId: 'analyse-daily',
          ownerKind: 'analyse',
          leaseUntil: '2026-08-21T07:12:00',
          finishedAt: '2026-08-20T07:11:00',
          lastError: null,
        },
      },
      paper_trading_intraday_monitor: {
        heartbeat: {
          fresh: true,
          status: 'alive',
          ownerId: 'analyse-1',
          ownerKind: 'analyse',
          observedAt: '2026-08-21T02:00:00',
          ageSeconds: 12,
          lastSuccessAt: '2026-08-21T01:55:00',
          lastErrorAt: null,
          errorMessage: null,
        },
        lease: {
          active: true,
          status: 'running',
          ownerId: 'analyse-1',
          ownerKind: 'analyse',
          leaseUntil: '2026-08-21T02:02:00',
          finishedAt: null,
          lastError: null,
        },
      },
      report_plan_reconciler: {
        heartbeat: {
          fresh: false,
          status: 'error',
          ownerId: 'analyse-2',
          ownerKind: 'analyse',
          observedAt: '2026-08-21T01:30:00',
          ageSeconds: 1800,
          lastSuccessAt: '2026-08-20T07:10:00',
          lastErrorAt: '2026-08-21T01:31:00',
          errorMessage: 'reconciler failed',
        },
        lease: {
          active: false,
          status: 'failed',
          ownerId: 'analyse-2',
          ownerKind: 'analyse',
          leaseUntil: '2026-08-21T01:32:00',
          finishedAt: '2026-08-21T01:31:30',
          lastError: 'reconciler failed',
        },
      },
    },
    scheduleTimes: ['15:10'],
    latestObservationBucket: '2026-08-21T02:00:00',
  });
  mocks.getPlans.mockResolvedValue({ total: 2, items: [{
    id: 1, sourceReportId: 9, sourceModel: 'model-a', market: 'cn', stockCode: '600519',
    reportDate: '2026-08-20', validForDate: '2026-08-21', action: 'buy', executable: true, status: 'active',
    rules: [], descriptiveConditions: [{ text: '关注量能', reason: 'ambiguous' }],
  }, {
    id: 99, sourceReportId: 99, sourceModel: 'model-a', market: 'cn', stockCode: '000001',
    reportDate: '2026-08-18', validForDate: '2026-08-19', action: 'buy', executable: false, status: 'expired',
    rules: [], descriptiveConditions: [{ text: '历史记录', reason: 'terminal' }],
  }] });
  mocks.getOperations.mockResolvedValue({ total: 501, items: [{
    id: 1,
    sourceReportId: 9,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '600519',
    stockName: '贵州茅台',
    reportDate: '2026-08-20',
    validForDate: '2026-08-21',
    action: 'buy',
    executable: true,
    status: 'active',
    operationalState: 'waiting_entry',
    stage: 'waiting_entry',
    isOperational: true,
    entryLegSchemaVersion: 'v2',
    entryLegs: [{ id: 'initial', role: 'initial', status: 'pending' }],
    entryLegIds: ['initial'],
    executedEntryLegIds: [],
    pendingEntryLegIds: ['initial'],
    lotCount: 0,
    openLotCount: 0,
    pendingExitCount: 0,
    positionIds: [],
    nextCheckObligation: null,
    reanalysisRequests: [],
    reanalysisRequestIds: [],
    rules: [{ type: 'price_breakout', sourceText: '放量突破' }],
    stopLoss: 95,
    targetPrice: 120,
    tradingStyle: 'position',
    tradingStylePolicyVersion: 'trading-style-v1',
    tradingStyleSettings: { analysisWindowSessions: 180, expectedHoldingSessionsRange: [10, 40], maxHoldingSessions: 60, entryLegLimit: 2, addLimit: 1, riskMultiplier: 1, llmEventCooldownMinutes: 240, llmDailyCap: 1, planRefreshCadenceSessions: 2 },
    tradingStyleEvidence: { historySufficient: true, frozenEvidenceValid: true, policyVersion: 'trading-style-v1' },
    historySufficient: true,
    frozenEvidenceValid: true,
    marketRegime: 'cautious',
    marketRegimePolicyVersion: 'market-regime-v1',
    marketRegimeContextId: 55,
    marketRegimeProvenance: { source: 'daily_market_report', parser: 'structured' },
    marketRegimeFresh: true,
    marketRegimeEntryAllowed: true,
    marketRegimeAllocationMultiplier: 0.5,
    marketRegimeBlockNewRisk: false,
    marketRegimeReasons: ['market_context_cautious'],
    strategyThesisRawAction: 'buy',
    strategyThesisEffectiveAction: 'watch',
    strategyThesisState: 'candidate',
    strategyThesisResult: 'observed',
    strategyThesisReason: 'awaiting_entry_confirmation',
    strategyThesisEntryPermitted: false,
    strategyThesisConfirmationCount: 1,
    strategyThesisConfirmationRequired: 2,
    strategyThesisConfirmationSessionDates: ['2026-08-20'],
  }, {
    id: 2,
    sourceReportId: 10,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    reportDate: '2026-08-20',
    validForDate: '2026-08-21',
    action: 'buy',
    executable: true,
    status: 'triggered',
    operationalState: 'entry_and_position_monitoring',
    stage: 'entry_and_position_monitoring',
    isOperational: true,
    entryLegSchemaVersion: 'v2',
    entryLegs: [{ id: 'initial', role: 'initial', status: 'filled' }, { id: 'pullback', role: 'pullback', status: 'pending' }],
    entryLegIds: ['initial', 'pullback'],
    executedEntryLegIds: ['initial'],
    pendingEntryLegIds: ['pullback'],
    lotCount: 1,
    openLotCount: 1,
    pendingExitCount: 0,
    positionIds: [2],
    nextCheckObligation: { status: 'scheduled', nextCheckTime: '2026-08-21T02:30:00' },
    reanalysisRequests: [],
    reanalysisRequestIds: [],
    rules: [{ type: 'volume', sourceText: '量能确认' }],
    stopLoss: 35,
    targetPrice: 42,
    exitLegSchemaVersion: 'exit-legs-v1',
    exitLegs: [
      { legId: 'tp1', sequence: 1, role: 'partial_take_profit', allocationFraction: 0.5, targetPrice: 120 },
      { legId: 'tp2', sequence: 2, role: 'final_take_profit', allocationFraction: 0.5, targetPrice: 140 },
    ],
    weightedTargetPrice: 130,
    weightedRiskReward: 1.63,
    executedExitLegIds: ['tp1'],
    pendingExitLegIds: ['tp2'],
    effectiveRiskLots: [{
      positionId: 2,
      quantity: 100,
      sourcePlanStopLoss: 35,
      sourcePlanTargetPrice: 42,
      effectiveStopLoss: 36,
      effectiveTargetPrice: 43,
      revisionId: 91,
      revisionVersion: 1,
      revisionSourceReportId: 99,
      revisionDecisionSignalId: 199,
      revisionResult: 'applied',
      revisionReason: 'accepted',
    }],
    effectiveStopLossValues: [36],
    effectiveTargetPriceValues: [43],
    latestRiskRevisionIds: [91],
  }, {
    id: 3,
    sourceReportId: 11,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '513120',
    stockName: '港股创新药ETF',
    reportDate: '2026-08-20',
    validForDate: '2026-08-21',
    action: 'hold',
    executable: true,
    status: 'triggered',
    operationalState: 'position_monitoring',
    stage: 'position_monitoring',
    isOperational: true,
    entryLegIds: ['legacy-initial'],
    executedEntryLegIds: ['legacy-initial'],
    pendingEntryLegIds: [],
    lotCount: 1,
    openLotCount: 1,
    pendingExitCount: 0,
    positionIds: [3],
    nextCheckObligation: { status: 'not_scheduled' },
    reanalysisRequests: [],
    reanalysisRequestIds: [],
    stopLoss: 1.2,
    targetPrice: 1.4,
  }, {
    id: 4,
    sourceReportId: 12,
    sourceModel: 'model-b',
    market: 'cn',
    stockCode: '512480',
    stockName: '半导体ETF',
    reportDate: '2026-08-20',
    validForDate: '2026-08-21',
    action: 'buy',
    executable: false,
    status: 'blocked',
    operationalState: 'reanalysis_blocked',
    stage: 'reanalysis_blocked',
    isOperational: false,
    entryLegIds: ['initial'],
    executedEntryLegIds: [],
    pendingEntryLegIds: ['initial'],
    lotCount: 0,
    openLotCount: 0,
    pendingExitCount: 0,
    positionIds: [],
    nextCheckObligation: { status: 'blocked' },
    reanalysisRequests: [{
      id: 77,
      status: 'pending',
      triggerType: 'plan_contract_low_payoff',
      source: 'plan_validation',
      failureReason: 'risk_reward_below_policy',
      evidence: { reasons: ['risk_reward_below_policy', 'target_too_close'] },
    }],
    reanalysisRequestIds: [77],
    stopLoss: 0.95,
    targetPrice: 1.01,
  }, {
    id: 5,
    sourceReportId: 13,
    sourceModel: 'model-b',
    market: 'cn',
    stockCode: '159915',
    stockName: '创业板ETF',
    reportDate: '2026-08-20',
    validForDate: '2026-08-21',
    action: 'sell',
    executable: true,
    status: 'triggered',
    operationalState: 't1_pending_exit',
    stage: 't1_pending_exit',
    isOperational: true,
    entryLegIds: ['legacy-initial'],
    executedEntryLegIds: ['legacy-initial'],
    pendingEntryLegIds: [],
    lotCount: 1,
    openLotCount: 1,
    pendingExitCount: 1,
    positionIds: [5],
    nextCheckObligation: null,
    reanalysisRequests: [],
    reanalysisRequestIds: [],
  }] });
  mocks.getAccounts.mockResolvedValue({ total: 2, items: [{
    id: 7,
    accountKey: 'model-a:cn:CNY',
    sourceModel: 'model-a',
    market: 'cn',
    currency: 'CNY',
    initialCapital: 100000,
    externalCashFlow: 0,
    bookEquity: 99925,
    marketValue: 14925,
    unrealizedPnl: -75,
    totalPnl: -150,
    marketEquity: 99850,
    valuationComplete: true,
    unpricedPositionCount: 0,
    unpricedHoldingCount: 0,
    valuationObservedAt: '2026-08-25T07:00:00',
    valuationStale: true,
    cash: 85000,
    investedCapital: 15000,
    realizedPnl: -75,
    exposure: 15000,
    openPositionCount: 1,
    maxTotalExposure: 80000,
    maxPositionCapital: 20000,
    riskPerTradePct: 1,
    maxPositionPct: 20,
    maxTotalExposurePct: 80,
    maxOpenPositions: 5,
    updatedAt: '2026-08-21T08:00:00',
  }, {
    id: 8,
    accountKey: 'model-a:hk:HKD',
    sourceModel: 'model-a',
    market: 'hk',
    currency: 'HKD',
    initialCapital: 15000,
    externalCashFlow: 0,
    bookEquity: 15000,
    marketValue: null,
    unrealizedPnl: null,
    totalPnl: null,
    marketEquity: null,
    valuationComplete: false,
    unpricedPositionCount: 3,
    unpricedHoldingCount: 1,
    valuationObservedAt: null,
    valuationStale: false,
    cash: 12000,
    investedCapital: 3000,
    realizedPnl: -20,
    exposure: 3000,
    openPositionCount: 1,
    maxTotalExposure: 12000,
    maxPositionCapital: 3000,
    riskPerTradePct: 1,
    maxPositionPct: 20,
    maxTotalExposurePct: 80,
    maxOpenPositions: 5,
    updatedAt: '2026-08-21T08:00:00',
  }] });
  mocks.getPortfolioAccounts.mockResolvedValue({ accounts: [
    { id: 101, name: 'CN real', broker: 'manual', market: 'cn', baseCurrency: 'CNY', isActive: true },
    { id: 102, name: 'HK real', broker: 'manual', market: 'hk', baseCurrency: 'HKD', isActive: true },
  ] });
  mocks.syncToPortfolio.mockResolvedValue({ created: true, item: {} });
  mocks.getAdjustments.mockResolvedValue({ total: 1, items: [{
    id: 20,
    idempotencyKey: 'web-test-key',
    accountId: 7,
    sourceModel: 'model-a',
    market: 'cn',
    currency: 'CNY',
    adjustmentType: 'deposit',
    amount: 1000,
    reason: 'add test capital',
    beforeCash: 84000,
    afterCash: 85000,
    beforeBookEquity: 99000,
    afterBookEquity: 100000,
    createdAt: '2026-08-21T08:30:00',
  }] });
  mocks.createAccount.mockResolvedValue({ created: true, item: {} });
  mocks.adjustAccount.mockResolvedValue({ created: true, item: {}, account: {} });
  mocks.getHoldings.mockResolvedValue({ total: 51, items: [{
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    currency: 'CNY',
    totalQuantity: 300,
    availableQuantity: 200,
    frozenQuantity: 100,
    weightedAveragePrice: 40.5,
    totalCost: 12150,
    lotCount: 3,
    marketValue: 14925,
    unrealizedPnl: -75,
    unrealizedReturnPct: -0.62,
    latestPrice: 49.75,
    observedAt: '2026-08-25T07:00:00',
    markSource: 'history_close',
    markStale: true,
    valuationComplete: true,
    symbolConcentrationPct: 12.15,
  }, {
    sourceModel: 'model-b',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    currency: 'CNY',
    totalQuantity: 100,
    availableQuantity: 100,
    frozenQuantity: 0,
    weightedAveragePrice: 41,
    totalCost: 4100,
    lotCount: 1,
    marketValue: 4200,
    unrealizedPnl: 100,
    unrealizedReturnPct: 2.44,
    latestPrice: 42,
    observedAt: '2026-08-25T07:00:00',
    markSource: 'realtime_quote',
    markStale: false,
    valuationComplete: true,
    symbolConcentrationPct: 4.1,
  }, {
    sourceModel: 'model-a',
    market: 'hk',
    stockCode: '002714',
    stockName: '牧原股份',
    currency: 'HKD',
    totalQuantity: 100,
    availableQuantity: 100,
    frozenQuantity: 0,
    weightedAveragePrice: 38,
    totalCost: 3800,
    lotCount: 1,
    marketValue: null,
    unrealizedPnl: null,
    unrealizedReturnPct: null,
    latestPrice: null,
    observedAt: null,
    markSource: null,
    markStale: false,
    valuationComplete: false,
    symbolConcentrationPct: 25.33,
  }] });
  mocks.getPositions.mockResolvedValue({ total: 3, items: [{
    id: 2,
    sourceReportId: 9,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    status: 'open',
    openedOn: '2026-08-21',
    openedPrice: 40,
    normalizedNotional: 4000,
    quantity: 100,
    capitalAccountId: 7,
    currency: 'CNY',
    allocatedCapital: 4000,
    lotSize: 100,
    riskAmount: 200,
    riskPerShare: 2,
    sizingReason: 'risk_cap_lot_adjusted',
    legacyFixedNotional: false,
    stopLoss: 36,
    targetPrice: 43,
    sourcePlanStopLoss: 35,
    sourcePlanTargetPrice: 42,
    effectiveStopLoss: 36,
    effectiveTargetPrice: 43,
    tradingStyle: 'trend',
    tradingStylePolicyVersion: 'trading-style-v1',
    tradingStyleSettings: { analysisWindowSessions: 500, expectedHoldingSessionsRange: [40, 120], maxHoldingSessions: 180, entryLegLimit: 3, addLimit: 2, riskMultiplier: 0.8, llmEventCooldownMinutes: 1440, llmDailyCap: 1, planRefreshCadenceSessions: 5 },
    tradingStyleEvidence: { historySufficient: true, frozenEvidenceValid: true, source: 'paper_trading_style_override_config' },
    historySufficient: true,
    frozenEvidenceValid: true,
    marketRegime: 'cautious',
    marketRegimePolicyVersion: 'market-regime-v1',
    marketRegimeContextId: 55,
    marketRegimeProvenance: { source: 'daily_market_report', parser: 'structured' },
    marketRegimeFresh: true,
    marketRegimeEntryAllowed: true,
    marketRegimeAllocationMultiplier: 0.5,
    marketRegimeBlockNewRisk: false,
    latestRiskRevision: {
      id: 91,
      revisionKey: 'risk-91',
      positionId: 2,
      planId: 2,
      sourceReportId: 99,
      decisionSignalId: 199,
      sourceModel: 'model-a',
      market: 'cn',
      stockCode: '002714',
      revisionVersion: 1,
      result: 'applied',
      reason: 'accepted',
      oldStopLoss: 35,
      newStopLoss: 36,
      appliedStopLoss: 36,
      oldTargetPrice: 42,
      newTargetPrice: 43,
      appliedTargetPrice: 43,
    },
  }, {
    id: 3,
    sourceReportId: 10,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    status: 'open',
    openedOn: '2026-08-22',
    openedPrice: 41,
    normalizedNotional: 4100,
    quantity: 100,
    capitalAccountId: 7,
    currency: 'CNY',
    allocatedCapital: 4100,
    lotSize: 100,
    riskAmount: 210,
    riskPerShare: 2.1,
    sizingReason: 'risk_cap_lot_adjusted',
    legacyFixedNotional: false,
  }, {
    id: 4,
    sourceReportId: 11,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    status: 'open',
    openedOn: '2026-08-25',
    openedPrice: 40.5,
    normalizedNotional: 4050,
    quantity: 100,
    capitalAccountId: 7,
    currency: 'CNY',
    allocatedCapital: 4050,
    lotSize: 100,
    riskAmount: 205,
    riskPerShare: 2.05,
    sizingReason: 'risk_cap_lot_adjusted',
    legacyFixedNotional: false,
  }] });
  mocks.getEvents.mockResolvedValue({ total: 1, items: [{
    id: 11,
    planId: 1,
    positionId: 2,
    sourceReportId: 9,
    sourceModel: 'model-a',
    stockCode: '002714',
    stockName: '牧原股份',
    eventType: 'open',
    eventDate: '2026-08-21',
    price: 40,
    quantity: 100,
    reason: 'entry_rules_passed',
    capitalAccountId: 7,
    currency: 'CNY',
    allocatedCapital: 4000,
    cashAfter: 85000,
    exposureAfter: 4000,
    realizedPnlAfter: 320.5,
    notificationDispatch: {
      id: 12,
      dispatchKey: 'paper-open-11-feishu',
      eventId: 11,
      channel: 'feishu',
      status: 'sent',
      attemptCount: 1,
      lastError: null,
      sentAt: '2026-08-21T08:01:00',
    },
    grossAmount: 4000,
    commission: 5,
    tax: 0,
    slippageAmount: 4,
    fees: 9,
    netAmount: 4009,
    grossRealizedPnl: 0,
    netRealizedPnl: -9,
  }] });
  mocks.getResolutions.mockResolvedValue({ total: 1, items: [{
    id: 1,
    resolutionKey: 'res-abc',
    policyVersion: 'paper-hybrid-v3',
    mode: 'intraday_position',
    status: 'closed',
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    currency: 'CNY',
    bucketStartedAt: '2026-08-21T02:00:00',
    selectedIntent: 'model_reduce',
    requestedQuantity: 500,
    executedQuantity: 500,
    unfilledQuantity: 0,
    candidateIntents: [{ intent: 'model_reduce' }],
    suppressedIntents: [{ intent: 'hold' }],
    chosenIntent: { intent: 'model_reduce', logicalOrderKey: 'ord-abc' },
    evidence: { policy: 'unit' },
  }] });
  mocks.getRiskRevisions.mockResolvedValue({ total: 1, items: [{
    id: 1,
    revisionKey: 'risk-abc',
    positionId: 2,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    result: 'applied',
    reason: 'accepted',
    oldStopLoss: 35,
    newStopLoss: 36,
    appliedStopLoss: 36,
    oldTargetPrice: 42,
    newTargetPrice: 43,
    appliedTargetPrice: 43,
    evaluatedAt: '2026-08-21T02:00:00',
    evidence: { source: 'unit' },
  }] });
  mocks.getDispatches.mockResolvedValue({ total: 1, items: [{
    id: 12,
    dispatchKey: 'paper-open-11-feishu',
    eventId: 11,
    channel: 'feishu',
    status: 'sent',
    attemptCount: 1,
    lastError: null,
    sentAt: '2026-08-21T08:01:00',
    updatedAt: '2026-08-21T08:01:00',
  }] });
  mocks.getEvaluations.mockResolvedValue({ total: 1, items: [{
    id: 3, planId: 1, evaluationDate: '2026-08-21', result: 'not_triggered',
    reasons: ['volume_below_threshold'], evidence: { rules: [{ passed: false }] },
    sourceModel: 'model-a', stockCode: '600519',
  }] });
  mocks.getObservations.mockResolvedValue({ total: 1, items: [{
    id: 4, planId: 1, sourceReportId: 9, sourceModel: 'model-a', market: 'cn',
    stockCode: '600519', mode: 'intraday', observedAt: '2026-08-21T10:00:00',
    bucketStartedAt: '2026-08-21T10:00:00', quotePrice: 101.2,
    quoteSource: 'unit_quote', result: 'not_triggered', action: 'none',
    compositeScore: 0.3, requiredPassed: false, factorsPassed: 1,
    factorsAvailable: 3, evidence: { reasons: ['composite_score_below_threshold'] },
  }] });
  mocks.getReanalysisRequests.mockResolvedValue({ total: 51, items: [{
    id: 77,
    requestKey: 'paper-reanalysis-current',
    triggerType: 'auction_material_change',
    status: 'submitted',
    sourcePlanId: 4,
    sourceReportId: 12,
    decisionSignalId: 88,
    sourceModel: 'model-b',
    market: 'cn',
    stockCode: '512480',
    stockName: '半导体ETF',
    sessionDate: '2026-08-21',
    dueAt: '2026-08-21T01:25:00',
    observedAt: '2026-08-21T01:25:30',
    submittedAt: '2026-08-21T01:26:00',
    taskId: 'task-77',
    queryId: 'task-77',
    replacementReportId: null,
    replacementPlanId: null,
    evidence: { deviationPct: 3.2, exitsRemainEnabled: true },
  }, {
    id: 78,
    requestKey: 'paper-reanalysis-history',
    triggerType: 'next_check',
    status: 'completed',
    sourcePlanId: 2,
    sourceReportId: 10,
    sourceModel: 'model-a',
    market: 'cn',
    stockCode: '002714',
    stockName: '牧原股份',
    sessionDate: '2026-08-20',
    completedAt: '2026-08-21T02:00:00',
    replacementReportId: 99,
    replacementPlanId: 100,
  }] });
  mocks.getReportPlanReconciliations.mockResolvedValue(toCamelCase({ total: 51, items: [{
    id: 501,
    reconciliation_key: 'report-plan-9-model-a',
    source_report_id: 9,
    report: {
      id: 9,
      code: '600519',
      name: '贵州茅台',
      report_type: 'full',
      model_used: 'model-a',
      created_at: '2026-08-21T07:00:00',
      market: 'cn',
    },
    plan_version: 'paper-hybrid-v3',
    source_model: 'model-a',
    status: 'completed',
    classification: 'stock_report_plan_reconciled',
    attempt_count: 1,
    max_attempts: 3,
    next_retry_at: null,
    last_attempt_at: '2026-08-21T07:10:00',
    last_error: null,
    decision_signal_id: 88,
    paper_plan_id: 1,
    details: { trigger_source: 'analysis_save' },
    created_at: '2026-08-21T07:00:01',
    updated_at: '2026-08-21T07:10:00',
  }, {
    id: 502,
    reconciliation_key: 'report-plan-12-model-b',
    source_report_id: 12,
    report: {
      id: 12,
      code: '512480',
      name: '半导体ETF',
      report_type: 'full',
      model_used: 'model-b',
      created_at: '2026-08-21T07:05:00',
      market: 'cn',
    },
    plan_version: 'paper-hybrid-v3',
    source_model: 'model-b',
    status: 'retry_wait',
    classification: 'missing_decision_signal',
    attempt_count: 2,
    max_attempts: 3,
    next_retry_at: '2026-08-21T07:20:00',
    last_attempt_at: '2026-08-21T07:11:00',
    last_error: 'signal not ready',
    decision_signal_id: 12,
    paper_plan_id: 23,
    details: { trigger_source: 'scheduled_reconciler' },
    created_at: '2026-08-21T07:05:01',
    updated_at: '2026-08-21T07:11:00',
  }, {
    id: 503,
    reconciliation_key: 'report-plan-13-model-a',
    source_report_id: 13,
    report: {
      id: 13,
      code: '000001',
      name: '平安银行',
      report_type: 'market_review',
      model_used: 'model-a',
      created_at: '2026-08-21T07:25:00',
      market: 'cn',
    },
    plan_version: 'paper-hybrid-v3',
    source_model: 'model-a',
    status: 'terminal',
    classification: 'market_review_not_stock_plan',
    attempt_count: 3,
    max_attempts: 3,
    next_retry_at: null,
    last_attempt_at: '2026-08-21T07:30:00',
    last_error: 'terminal classification',
    decision_signal_id: 13,
    paper_plan_id: 31,
    details: { trigger_source: 'scheduled_reconciler' },
    created_at: '2026-08-21T07:25:01',
    updated_at: '2026-08-21T07:30:00',
  }] }));
  mocks.getAuctionObservations.mockResolvedValue({ total: 51, items: [{
    id: 31,
    observationKey: 'auction-31',
    sourceModel: 'model-b',
    market: 'cn',
    stockCode: '512480',
    stockName: '半导体ETF',
    sessionDate: '2026-08-21',
    phase: 'opening_auction',
    status: 'fail_closed',
    observedAt: '2026-08-21T01:25:30',
    providerTimestamp: '2026-08-21T01:25:20',
    fetchedAt: '2026-08-21T01:25:31',
    source: 'unit_auction',
    identityVerified: false,
    stale: true,
    virtualPrice: 1.23,
    priorClose: 1.2,
    entryLow: 1.18,
    entryHigh: 1.21,
    gapPct: 2.5,
    matchedVolume: 120000,
    matchedVolumeRatio: 18.5,
    unmatchedSide: 'buy',
    imbalancePct: 66.6,
    limitProximityPct: 3.1,
    materialChange: true,
    failClosedReason: 'auction_identity_mismatch',
    reanalysisRequestId: 77,
  }] });
  mocks.getRiskSnapshots.mockResolvedValue({ total: 51, items: [{
    id: 41,
    snapshotKey: 'risk-snapshot-41',
    policyVersion: 'account-risk-v1',
    accountId: 7,
    accountKey: 'model-a:cn:CNY',
    sourceModel: 'model-a',
    market: 'cn',
    currency: 'CNY',
    sessionDate: '2026-08-21',
    bucketStartedAt: '2026-08-21T02:00:00',
    computedAt: '2026-08-21T02:00:02',
    valuationComplete: false,
    valuationStale: true,
    sessionStartEquity: 100000,
    currentEquity: 97000,
    peakEquity: 101000,
    cash: 85000,
    exposure: 15000,
    realizedPnl: -75,
    unrealizedPnl: -2925,
    totalPnl: -3000,
    dailyPnl: -3000,
    dailyLossPct: 3,
    drawdownPct: 3.96,
    losingStreak: 3,
    dailyTradeCount: 10,
    dailyOpenAddCount: 5,
    dailyReduceCloseCount: 1,
    totalExposurePct: 15,
    symbolUtilizationPct: 75,
    sectorUtilizationPct: 58,
    correlationUtilizationPct: 61,
    unknownUtilizationPct: 49,
    unknownCapacity: { bucket: 'unknown', remainingCapacity: 1000, capPct: 50, utilizationPct: 49 },
    sectorCapacities: [{ name: '农业', remainingCapacity: 2000, capPct: 60, utilizationPct: 58 }],
    correlationClusters: [{ name: 'cluster-a', remainingCapacity: 0, capPct: 60, utilizationPct: 61 }],
  }] });
  mocks.getRiskLocks.mockResolvedValue({ total: 51, items: [{
    id: 42,
    lockKey: 'risk-lock-42',
    policyVersion: 'account-risk-v1',
    accountId: 7,
    accountKey: 'model-a:cn:CNY',
    sourceModel: 'model-a',
    market: 'cn',
    currency: 'CNY',
    state: 'reduce_only',
    status: 'active',
    activeReasons: ['daily_loss_breached', 'valuation_incomplete'],
    entryAllowed: false,
    addAllowed: false,
    reduceAllowed: true,
    closeAllowed: true,
    exitsRemainEnabled: true,
    manualResumeRequired: true,
    recoveryCondition: 'daily_loss_next_session_and_manual_resume',
    haltScope: 'account',
    cooldownUntil: '2026-08-22T01:30:00',
    snapshotId: 41,
    lastEventId: 43,
    metric: 3,
    threshold: 3,
  }] });
  mocks.getRiskEvents.mockResolvedValue({ total: 51, items: [{
    id: 43,
    eventKey: 'risk-event-43',
    idempotencyKey: 'risk-key-43',
    accountId: 7,
    accountKey: 'model-a:cn:CNY',
    sourceModel: 'model-a',
    market: 'cn',
    currency: 'CNY',
    eventType: 'entry_suspended',
    source: 'system',
    oldState: 'normal',
    newState: 'reduce_only',
    activeReasons: ['daily_loss_breached'],
    metric: 3,
    threshold: 3,
    recoveryCondition: 'manual_resume_after_recovery',
    snapshotId: 41,
    reason: 'daily loss reached policy',
    status: 'applied',
    createdAt: '2026-08-21T02:00:03',
  }] });
  mocks.manualHold.mockResolvedValue({ created: true, item: {} });
  mocks.manualResume.mockResolvedValue({ created: true, item: {} });
  mocks.getScopes.mockResolvedValue([{ scopeId: 'primary', label: '主账户', kind: 'primary' }]);
  mocks.getPerformance.mockResolvedValue({
    closedPositions: 2, winRatePct: 50, realizedReturnPct: 1.25,
    totalFees: 19,
    totalCommission: 10,
    totalSellTax: 5,
    totalSlippageAmount: 4,
    explicitCostTotal: 19,
    models: [
      { model: 'model-a', closedPositions: 1, winRatePct: 100, realizedReturnPct: 2, totalFees: 12, totalCommission: 7, totalSellTax: 2, totalSlippageAmount: 3, explicitCostTotal: 12 },
      { model: 'model-b', closedPositions: 1, winRatePct: 0, realizedReturnPct: -0.75, totalFees: 7, totalCommission: 3, totalSellTax: 3, totalSlippageAmount: 1, explicitCostTotal: 7 },
    ],
  });
});

describe('PaperTradingPage', () => {
  it('renders configured models, monitor status, comparison, and evaluation evidence', async () => {
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Hybrid Paper Trading Monitor')).toBeInTheDocument();
    expect((await screen.findAllByText('model-a')).length).toBeGreaterThan(0);
    expect(screen.getAllByText('model-b').length).toBeGreaterThan(0);
    expect(screen.getByText('Configured')).toBeInTheDocument();
    expect(screen.getByText('Runtime and scheduler audit')).toBeInTheDocument();
    expect(screen.getByText('Open lots / closed lots')).toBeInTheDocument();
    expect(screen.getByText('3 lots total')).toBeInTheDocument();
    expect(screen.getByText(/Entry policy: style position, at least 2 market factors/)).toBeInTheDocument();
    expect(screen.getByText(/Capital config: initial 100000.00/)).toBeInTheDocument();
    expect(screen.getByText(/staged-plan cap 20.00%, global symbol cap 50.00%, effective asset caps stock 20.00%/)).toBeInTheDocument();
    expect(screen.getByText(/Feishu paper-fill notifications enabled, route configured/)).toBeInTheDocument();
    expect(screen.getByText('Shared database scheduler tasks')).toBeInTheDocument();
    expect(screen.getByText('daily_analysis:15:10')).toBeInTheDocument();
    expect(screen.queryByText('daily_analysis')).not.toBeInTheDocument();
    expect(screen.getByText('paper_trading_intraday_monitor')).toBeInTheDocument();
    expect(screen.getByText('report_plan_reconciler')).toBeInTheDocument();
    expect(screen.getByText('reconciler failed')).toBeInTheDocument();
    expect(screen.getByText('Strategy controls')).toBeInTheDocument();
    expect(screen.getByText('Data window')).toBeInTheDocument();
    expect(screen.getByText('180 sessions')).toBeInTheDocument();
    expect(screen.getByText(/Compensation lookback: 30 day/)).toBeInTheDocument();
    expect(screen.getByText(/Cost assumptions: slippage 0.10%, commission 0.03%, minimum commission 5.00, CN stock sell stamp tax 0.05%; ETF\/fund sells use zero stamp tax/)).toBeInTheDocument();
    expect(screen.getByText('Current market execution gate')).toBeInTheDocument();
    expect(screen.getAllByText(/regime: cautious/).length).toBeGreaterThan(0);
    expect(screen.getByText(/multiplier: 0.5/)).toBeInTheDocument();
    expect(screen.getByText(/market \/ session: cn \/ 2026-08-21/)).toBeInTheDocument();
    expect(screen.getByText(/source: daily_market_report/)).toBeInTheDocument();
    expect(screen.getByText(/report \/ context: #314 \/ #55/)).toBeInTheDocument();
    expect(screen.getByText(/reasons: market_context_cautious/)).toBeInTheDocument();
    expect(screen.getByText('Opening confirmation and strategy refresh')).toBeInTheDocument();
    expect(screen.getByText(/This is not auction order-book or imbalance data/)).toBeInTheDocument();
    expect(screen.getByText('Today unresolved opening-confirmation/reanalysis blockers')).toBeInTheDocument();
    expect(screen.getByText(/paper-reanalysis-current/)).toBeInTheDocument();
    expect(screen.getByText(/New open\/add remains blocked; exits remain enabled/)).toBeInTheDocument();
    expect(screen.getByText('Opening-confirmation evidence')).toBeInTheDocument();
    expect(screen.getByTestId('opening-blocker-list')).toHaveClass('max-h-80', 'overflow-y-auto');
    expect(screen.getByTestId('opening-evidence-list')).toHaveClass('max-h-80', 'overflow-y-auto');
    expect(screen.getByText('auction_identity_mismatch')).toBeInTheDocument();
    expect(screen.getByText('Identity mismatch')).toBeInTheDocument();
    expect(screen.getByText('Stale snapshot')).toBeInTheDocument();
    expect(screen.getByText('Account-level risk control')).toBeInTheDocument();
    expect(screen.getByText('Current risk permissions / locks')).toBeInTheDocument();
    expect(screen.getByText(/daily_loss_breached, valuation_incomplete/)).toBeInTheDocument();
    expect(screen.getByText(/Exits remain enabled/)).toBeInTheDocument();
    expect(screen.getByText('Latest risk snapshots and capacity')).toBeInTheDocument();
    expect(screen.getByText(/Unknown capacity/)).toBeInTheDocument();
    expect(screen.getByText(/Sector:/)).toBeInTheDocument();
    expect(screen.getByText(/Correlation:/)).toBeInTheDocument();
    expect(screen.getByText(/Manual hold \/ resume/)).toBeInTheDocument();
    expect(screen.getByText('Risk event history audit')).toBeInTheDocument();
    expect(screen.getByText('Report-to-plan processing evidence')).toBeInTheDocument();
    expect(screen.getByText(/settings only and do not prove any report produced a plan/)).toBeInTheDocument();
    expect(screen.getByText('report-plan-9-model-a')).toBeInTheDocument();
    expect(screen.getByText('Stock report reconciled to a plan')).toBeInTheDocument();
    expect(screen.getByText('Retry wait')).toBeInTheDocument();
    expect(screen.getByText('Missing decision signal')).toBeInTheDocument();
    expect(screen.getByText('signal not ready')).toBeInTheDocument();
    expect(screen.getByText('Market review does not create stock plans')).toBeInTheDocument();
    expect(screen.getByText(/2 \/ 3/)).toBeInTheDocument();
    expect(screen.getByText(/Next retry: 2026-08-21 15:20:00/)).toBeInTheDocument();
    expect(screen.getByText('Frozen strategy and cost audit')).toBeInTheDocument();
    expect(screen.getByText('Plan frozen style / market gate')).toBeInTheDocument();
    expect(screen.getByText(/cautious · market-regime-v1 · context #55/)).toBeInTheDocument();
    expect(screen.getByText(/Style policy \/ history sufficient: trading-style-v1 \/ yes/)).toBeInTheDocument();
    expect(screen.getAllByText(/open\/add allowed · fresh yes · multiplier 0.5/).length).toBeGreaterThan(0);
    expect(screen.getAllByText('Market provenance: {"source":"daily_market_report","parser":"structured"}').length).toBeGreaterThan(0);
    expect(screen.getByText(/trend · window 500 · hold 40\/120 · max 180 · legs\/adds 3\/2 · risk 0.8/)).toBeInTheDocument();
    expect(screen.getByText(/Fill costs: gross CNY 4000.00 · commission CNY 5.00 · tax CNY 0.00 · slippage CNY 4.00 · fees CNY 9.00 · net CNY 4009.00/)).toBeInTheDocument();
    expect(screen.getByText('Entry suspended')).toBeInTheDocument();
    expect(screen.getByText('Intent resolution and simulated-order audit')).toBeInTheDocument();
    expect(screen.getByText('Risk-revision audit')).toBeInTheDocument();
    expect(screen.getByText('Model reduce')).toBeInTheDocument();
    expect(screen.getByText('res-abc')).toBeInTheDocument();
    expect(screen.getByText('risk-abc')).toBeInTheDocument();
    expect(screen.getByText(/volume_below_threshold/)).toBeInTheDocument();
    expect(screen.getByText(/unit_quote/)).toBeInTheDocument();
    expect(screen.getAllByText(/composite_score_below_threshold/).length).toBeGreaterThan(0);
    expect(screen.getByText('Paper fund management')).toBeInTheDocument();
    expect(screen.getByText('Paper fund management').closest('.rounded-2xl')).toHaveClass('order-[20]');
    expect(screen.getAllByText('Open lots').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Closed lots').length).toBeGreaterThan(0);
    expect(screen.getByText('Explicit cost breakdown')).toBeInTheDocument();
    expect(screen.getByText('19.00')).toBeInTheDocument();
    expect(screen.getByText(/Commission 10.00 · Sell tax 5.00 · Slippage 4.00/)).toBeInTheDocument();
    expect(screen.getAllByText('Explicit costs').length).toBeGreaterThan(0);
    expect(screen.getByText('Fund adjustment journal')).toBeInTheDocument();
    expect(screen.getByText('add test capital')).toBeInTheDocument();
    expect(screen.getByText('web-test-key')).toBeInTheDocument();
    expect(screen.getAllByText('model-a:cn:CNY').length).toBeGreaterThan(0);
    expect(screen.getAllByText('model-a:hk:HKD').length).toBeGreaterThan(0);
    expect(screen.getAllByText('CNY 15000.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('HKD 15000.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Book equity').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Market equity').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Market value').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Realized PnL').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Unrealized PnL').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Total PnL').length).toBeGreaterThan(0);
    expect(screen.getAllByText('CNY 99925.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('CNY 99850.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText('CNY -75.00').length).toBeGreaterThanOrEqual(2);
    expect(screen.getAllByText('CNY -150.00').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Incomplete valuation: 1 unpriced holding\(s\) across 3 lot\(s\)/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/Incomplete valuation: 3 unpriced holding/)).not.toBeInTheDocument();
    expect(screen.getAllByText('Incomplete / --').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Valuation mark is stale \(not for trading\)/).length).toBeGreaterThan(0);
    expect(screen.queryByText('115000.00')).not.toBeInTheDocument();
    expect(screen.getByText('Aggregate paper holdings by model / market / symbol / currency')).toBeInTheDocument();
    expect(screen.getByText('Aggregate paper holdings by model / market / symbol / currency').closest('.rounded-2xl')).toHaveClass('order-[50]');
    expect(screen.getByText(/Each row is one aggregate holding/)).toBeInTheDocument();
    expect(screen.getAllByText('牧原股份').length).toBeGreaterThan(0);
    expect(screen.getByText('Sellable 200.00')).toBeInTheDocument();
    expect(screen.getByText('T+1 frozen 100.00')).toBeInTheDocument();
    expect(screen.getByText('Lots')).toBeInTheDocument();
    expect(screen.getByText('3')).toBeInTheDocument();
    expect(screen.getByText('CNY 12150.00')).toBeInTheDocument();
    expect(screen.getByText('Weighted cost CNY 40.50')).toBeInTheDocument();
    expect(screen.getAllByText('Source: history_close').length).toBeGreaterThan(0);
    expect(screen.getByText('Same-symbol concentration')).toBeInTheDocument();
    expect(screen.getByText('12.15%')).toBeInTheDocument();
    expect(screen.getAllByText(/new buys resize or reject above shared same-symbol cap/).length).toBeGreaterThan(0);
    expect(screen.queryByText(/add-ons blocked/)).not.toBeInTheDocument();
    expect(screen.getAllByText('51 total · Page 1/2').length).toBeGreaterThan(0);
    expect(screen.getByText('Report-attributed paper lots')).toBeInTheDocument();
    expect(screen.getAllByText(/Source plan stop \/ target 35.00 \/ 42.00/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/lot #2 · qty 100.00 · effective 36.00 \/ 43.00/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/revision v1 · report #99 · applied/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Effective 36.00 \/ 43.00/).length).toBeGreaterThan(0);
    expect(mocks.getOperations).toHaveBeenCalledWith(undefined, { page: 1, limit: 50, scopeId: 'primary' });
    expect(mocks.getPlans).toHaveBeenCalledWith(undefined, { page: 1, limit: 50, scopeId: 'primary' });
    expect(mocks.getReportPlanReconciliations).toHaveBeenCalledWith(undefined, { page: 1, limit: 50 });
    expect(screen.getAllByText('Current operations').length).toBeGreaterThan(0);
    expect(screen.getByText('Current operational lifecycle')).toBeInTheDocument();
    expect(screen.getByText('Current operational lifecycle').closest('.rounded-2xl')).toHaveClass('order-[60]');
    expect(screen.getByText('Runtime and scheduler audit').closest('.rounded-2xl')).toHaveClass('order-[80]');
    expect(screen.getByText('Recent audit events').closest('.rounded-2xl')).toHaveClass('order-[90]');
    expect(screen.getByText('Waiting entry')).toBeInTheDocument();
    expect(screen.getByText('Model opinion / authority')).toBeInTheDocument();
    expect(screen.getAllByText(/Raw LLM:/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Effective authority:/).length).toBeGreaterThan(0);
    expect(screen.getByText('New entry paused')).toBeInTheDocument();
    expect(screen.getByText(/1\/2 distinct sessions/)).toBeInTheDocument();
    expect(screen.getByText(/awaiting_entry_confirmation/)).toBeInTheDocument();
    expect(screen.getByText('Entry + position monitoring')).toBeInTheDocument();
    expect(screen.getByText('Position monitoring')).toBeInTheDocument();
    expect(screen.getByText('Reanalysis blocked')).toBeInTheDocument();
    expect(screen.getByText('T+1 pending exit')).toBeInTheDocument();
    expect(screen.getAllByText('Not scheduled / unavailable').length).toBeGreaterThan(0);
    expect(screen.getAllByText(/see evidence\/reanalysis status above/).length).toBeGreaterThan(0);
    expect(screen.getByText(/pending · plan_contract_low_payoff/)).toBeInTheDocument();
    expect(screen.getByText(/triggered plans with open or pending lots remain visible/)).toBeInTheDocument();
    expect(screen.getAllByText('牧原股份').length).toBeGreaterThan(0);
    expect(screen.getByText('501')).toBeInTheDocument();
    expect(screen.getByText('501 total · Page 1/11')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'Audit history' }));
    await waitFor(() => expect(mocks.getPlans).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50, scopeId: 'primary' }));
    expect(screen.getAllByText('Plan audit records').length).toBeGreaterThan(0);
    expect(screen.getByText(/expired, superseded, non-executable, and closed plans are history only/)).toBeInTheDocument();
    expect(screen.getAllByText('expired').length).toBeGreaterThan(0);
    expect(screen.getAllByText('Descriptive').length).toBeGreaterThan(0);
    expect(screen.getByText('关注量能')).toBeInTheDocument();
    const aggregateSection = screen.getByText('Aggregate paper holdings by model / market / symbol / currency');
    const planSection = screen.getAllByText('Plan audit records').at(-1) as HTMLElement;
    expect(aggregateSection.compareDocumentPosition(planSection) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
    expect(screen.getAllByText(/risk_cap_lot_adjusted/)).toHaveLength(3);
    expect(screen.getAllByText(/Simulated buy/).length).toBeGreaterThanOrEqual(2);
    expect(screen.getByText(/Qty: 100.00/)).toBeInTheDocument();
    expect(screen.getByText(/Plan: #1/)).toBeInTheDocument();
    expect(screen.getByText(/Report: #9/)).toBeInTheDocument();
    expect(screen.getByText(/Lot: #2/)).toBeInTheDocument();
    expect(screen.getByText(/Reason: entry_rules_passed/)).toBeInTheDocument();
    expect(screen.getByText(/feishu sent · attempts 1/)).toBeInTheDocument();
    expect(screen.getByText('Notification dispatches')).toBeInTheDocument();
    expect(screen.getByText(/No real order is sent/)).toBeInTheDocument();
    expect(screen.getAllByText('2026-08-21 10:00:00 (UTC+8)').length).toBeGreaterThan(0);
    expect(screen.getByText('2026-08-21 18:00:00 (UTC+8)')).toBeInTheDocument();
    expect(screen.getByText(/scheduler wakes roughly every 5 minute/)).toBeInTheDocument();
    expect(screen.getByRole('option', { name: 'JP' })).toBeInTheDocument();
    fireEvent.click(within(screen.getByRole('navigation', { name: 'Aggregate holdings pagination' })).getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(mocks.getHoldings).toHaveBeenLastCalledWith(undefined, { page: 2, limit: 50, scopeId: 'primary' }));
    fireEvent.change(screen.getAllByRole('combobox')[0], { target: { value: 'model-b' } });
    await waitFor(() => expect(mocks.getAccounts).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50, scopeId: 'primary' }));
    expect(mocks.getAdjustments).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50 });
    expect(mocks.getHoldings).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50, scopeId: 'primary' });
    expect(mocks.getOperations).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50, scopeId: 'primary' });
    expect(mocks.getPlans).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50, scopeId: 'primary' });
    expect(mocks.getDispatches).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50 });
    expect(mocks.getReportPlanReconciliations).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50 });
    expect(mocks.getRiskSnapshots).toHaveBeenLastCalledWith('model-b', { page: 1, limit: 50 });
  });

  it('renders live reconciliation serializer payload from nested report/details without blank critical cells', async () => {
    mocks.getReportPlanReconciliations.mockResolvedValue(toCamelCase({
      total: 1,
      items: [{
        id: 501,
        source_report_id: 1,
        reconciliation_key: 'analysis:1:paper-hybrid-v3',
        source_model: 'model-a',
        plan_version: 'paper-hybrid-v3',
        status: 'retry_wait',
        classification: 'transient_error',
        attempt_count: 2,
        max_attempts: 3,
        next_retry_at: '2026-08-21T07:20:00',
        last_attempt_at: '2026-08-21T07:11:00',
        last_error: 'temporary',
        decision_signal_id: 12,
        paper_plan_id: 23,
        details: {
          trigger_source: 'scheduled_reconciler',
        },
        report: {
          id: 1,
          code: '600511',
          name: '标的1',
          model_used: 'model-a',
          report_type: 'full',
        },
      }],
    }));

    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('analysis:1:paper-hybrid-v3')).toBeInTheDocument();
    expect(screen.getByText('标的1')).toBeInTheDocument();
    expect(screen.getByText(/report #1 · 600511 · market not exposed/)).toBeInTheDocument();
    expect(screen.getByText(/scheduled_reconciler · paper-hybrid-v3/)).toBeInTheDocument();
    expect(screen.getByText(/Plan #23/)).toBeInTheDocument();
    expect(screen.getByText(/signal #12/)).toBeInTheDocument();
    expect(screen.getByText('temporary')).toBeInTheDocument();
    const row = screen.getByText('analysis:1:paper-hybrid-v3').closest('tr');
    expect(row?.textContent).not.toContain('--');
  });

  it('renders staged exit contract, low-payoff correction, and legacy source target safely', async () => {
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Current operational lifecycle')).toBeInTheDocument();

    const stagedRow = screen.getByText(/#10 · 2026-08-20/).closest('tr') as HTMLElement;
    expect(stagedRow).toBeTruthy();
    expect(within(stagedRow).getByText(/Staged exits · weighted target 130.00 · RR 1.63/)).toBeInTheDocument();
    expect(within(stagedRow).getByText('tp1')).toBeInTheDocument();
    expect(within(stagedRow).getByText('tp2')).toBeInTheDocument();
    expect(within(stagedRow).getAllByText('120.00 / 50%').length).toBeGreaterThan(0);
    expect(within(stagedRow).getAllByText('140.00 / 50%').length).toBeGreaterThan(0);
    expect(within(stagedRow).getByText('done')).toBeInTheDocument();
    expect(within(stagedRow).getByText('pending')).toBeInTheDocument();
    expect(within(stagedRow).getByText(/Source plan stop \/ target 35.00 \/ 42.00/)).toBeInTheDocument();
    expect(within(stagedRow).getByText(/Weighted target \/ RR 130.00 \/ 1.63/)).toBeInTheDocument();
    expect(within(stagedRow).getByText(/exit leg: tp2/)).toBeInTheDocument();

    const lowPayoffRow = screen.getByText(/#12 · 2026-08-20/).closest('tr') as HTMLElement;
    expect(lowPayoffRow).toBeTruthy();
    expect(within(lowPayoffRow).getByText(/pending · plan_contract_low_payoff/)).toBeInTheDocument();
    expect(within(lowPayoffRow).getByText(/Low-payoff correction: pending · risk_reward_below_policy, target_too_close/)).toBeInTheDocument();
    expect(within(lowPayoffRow).getByText(/Source plan stop \/ target 0.95 \/ 1.01/)).toBeInTheDocument();

    const legacyRow = screen.getByText(/#11 · 2026-08-20/).closest('tr') as HTMLElement;
    expect(legacyRow).toBeTruthy();
    expect(within(legacyRow).queryByText(/Staged exits/)).not.toBeInTheDocument();
    expect(within(legacyRow).queryByText(/Weighted target \/ RR/)).not.toBeInTheDocument();
    expect(within(legacyRow).getByText(/Source plan stop \/ target 1.20 \/ 1.40/)).toBeInTheDocument();
  });

  it('initializes accounts and records fund adjustments with confirmation and required reason', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Paper fund management')).toBeInTheDocument();
    fireEvent.change(screen.getByPlaceholderText('Initial capital'), { target: { value: '200000' } });
    fireEvent.change(screen.getAllByRole('combobox')[2], { target: { value: 'jp' } });
    fireEvent.click(screen.getByRole('button', { name: 'Initialize' }));
    await waitFor(() => expect(mocks.createAccount).toHaveBeenCalledWith({ sourceModel: 'model-a', market: 'jp', initialCapital: 200000 }));

    fireEvent.change(screen.getAllByPlaceholderText('Amount')[0], { target: { value: '1000' } });
    await waitFor(() => expect(screen.getAllByPlaceholderText('Amount')[0]).toHaveValue(1000));
    fireEvent.click(screen.getAllByRole('button', { name: 'Record paper flow' })[0]);
    expect(await screen.findByText('A fund-adjustment reason is required for audit.')).toBeInTheDocument();
    fireEvent.change(screen.getAllByPlaceholderText('Reason required')[0], { target: { value: 'rebalance cash' } });
    fireEvent.click(screen.getAllByRole('button', { name: 'Record paper flow' })[0]);
    fireEvent.click(screen.getAllByRole('button', { name: 'Record paper flow' })[0]);
    await waitFor(() => expect(mocks.adjustAccount).toHaveBeenCalled());
    expect(mocks.adjustAccount).toHaveBeenCalledTimes(1);
    expect(mocks.adjustAccount.mock.calls[0][0]).toBe(7);
    expect(mocks.adjustAccount.mock.calls[0][1]).toMatchObject({ adjustmentType: 'deposit', amount: 1000, reason: 'rebalance cash' });
    expect(mocks.adjustAccount.mock.calls[0][1].idempotencyKey).toMatch(/^web-/);
    confirmSpy.mockRestore();
  });

  it('requires manual risk reason and submits hold/resume with idempotency', async () => {
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Manual hold / resume (paper risk ledger)')).toBeInTheDocument();
    fireEvent.click(screen.getAllByRole('button', { name: 'Manual hold' })[0]);
    expect(await screen.findByText('Manual risk actions require a reason.')).toBeInTheDocument();
    fireEvent.change(screen.getAllByPlaceholderText('Manual action reason (required)')[0], { target: { value: 'operator check' } });
    fireEvent.click(screen.getAllByRole('button', { name: 'Manual hold' })[0]);
    await waitFor(() => expect(mocks.manualHold).toHaveBeenCalledWith(expect.objectContaining({
      accountId: 7,
      reason: 'operator check',
      idempotencyKey: expect.stringMatching(/^web-/),
    })));
    fireEvent.change(screen.getAllByPlaceholderText('Manual action reason (required)')[0], { target: { value: 'metrics recovered' } });
    fireEvent.click(screen.getAllByRole('button', { name: 'Request resume' })[0]);
    await waitFor(() => expect(mocks.manualResume).toHaveBeenCalledWith(expect.objectContaining({
      accountId: 7,
      reason: 'metrics recovered',
      idempotencyKey: expect.stringMatching(/^web-/),
    })));
    confirmSpy.mockRestore();
  });

  it('renders missing processing/auction/risk backend contracts as fail-closed instead of healthy', async () => {
    mocks.getReanalysisRequests.mockRejectedValue(new Error('404 reanalysis'));
    mocks.getReportPlanReconciliations.mockRejectedValue(new Error('404 reconciliation'));
    mocks.getAuctionObservations.mockRejectedValue(new Error('404 auction'));
    mocks.getRiskSnapshots.mockRejectedValue(new Error('404 snapshots'));
    mocks.getRiskLocks.mockRejectedValue(new Error('404 locks'));
    mocks.getRiskEvents.mockRejectedValue(new Error('404 events'));

    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Opening confirmation and strategy refresh')).toBeInTheDocument();
    expect(screen.getByText(/Opening-confirmation observations backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/Reanalysis requests backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/Report-to-plan processing evidence backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/riskSnapshots backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/riskLocks backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/riskEvents backend contract is unavailable or failed/)).toBeInTheDocument();
    expect(screen.getByText(/No account risk snapshots; account state cannot be treated as healthy/)).toBeInTheDocument();
  });

  it('syncs a paper position to a real portfolio account with a stable idempotency key', async () => {
    const promptSpy = vi.spyOn(window, 'prompt')
      .mockReturnValueOnce('101')
      .mockReturnValueOnce('buy')
      .mockReturnValueOnce('100')
      .mockReturnValueOnce('40')
      .mockReturnValueOnce('2026-08-21')
      .mockReturnValueOnce('1.5');
    const confirmSpy = vi.spyOn(window, 'confirm').mockReturnValue(true);

    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    const syncButton = (await screen.findAllByRole('button', { name: 'Sync to portfolio' }))[0];
    fireEvent.click(syncButton);

    await waitFor(() => expect(mocks.syncToPortfolio).toHaveBeenCalledWith({
      paperPositionId: 2,
      portfolioAccountId: 101,
      side: 'buy',
      quantity: 100,
      price: 40,
      tradeDate: '2026-08-21',
      fee: 1.5,
      idempotencyKey: 'paper-to-portfolio-2-101-buy-2026-08-21-100-40-1.5',
    }));
    expect(promptSpy.mock.calls[0][1]).toBe('101');
    expect(promptSpy.mock.calls[0][0]).toContain('CN real');
    promptSpy.mockRestore();
    confirmSpy.mockRestore();
  });

  it('keeps intraday marked configured when the API process does not own scheduling', async () => {
    mocks.getStatus.mockResolvedValue({
      enabled: true,
      intradayEnabled: true,
      intervalMinutes: 5,
      intervalSeconds: 300,
      minFactorDomains: 2,
      minCompositeScore: 0.6,
      minRiskReward: 1.2,
      maxQuoteStaleSeconds: 600,
      localSchedulerEnabled: false,
      localSchedulerRunning: false,
      localSchedulerOwner: 'api_process',
      schedulerEnabled: true,
      schedulerRunning: true,
      schedulerHeartbeatFresh: true,
      schedulerTaskStates: {
        paper_trading_intraday_monitor: {
          heartbeat: {
            fresh: true,
            status: 'alive',
            ownerId: 'analyse-worker',
            ownerKind: 'analyse',
            observedAt: '2026-08-21T02:00:00',
            ageSeconds: 9,
            lastSuccessAt: '2026-08-21T01:55:00',
            details: {
              lastRunSummary: {
                status: 'succeeded',
                finishedAt: '2026-08-21T01:55:00',
                durationSeconds: 1.5,
                targetIdentityCount: 0,
                quoteFetchAttempts: 0,
                quoteFetchSuccesses: 0,
                skippedClosedMarket: 0,
                failedClosed: 0,
                observations: 0,
              },
            },
          },
          lease: {
            active: true,
            status: 'running',
            ownerId: 'analyse-worker',
            ownerKind: 'analyse',
            leaseUntil: '2026-08-21T02:02:00',
          },
        },
      },
      schedulerLastRunSummaries: {
        paper_trading_intraday_monitor: {
          status: 'succeeded',
          finishedAt: '2026-08-21T01:55:00',
          durationSeconds: 1.5,
          targetIdentityCount: 0,
          quoteFetchAttempts: 0,
          quoteFetchSuccesses: 0,
          skippedClosedMarket: 0,
          failedClosed: 0,
          observations: 0,
        },
      },
      intradayMonitorLastRunSummary: {
        status: 'succeeded',
        finishedAt: '2026-08-21T01:55:00',
        durationSeconds: 1.5,
        targetIdentityCount: 0,
        quoteFetchSuccesses: 0,
        skippedClosedMarket: 0,
        failedClosed: 0,
      },
      scheduleTimes: ['15:10'],
      latestObservationBucket: '2026-08-21T02:00:00',
    });

    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);

    expect(await screen.findByText('Configured')).toBeInTheDocument();
    expect(screen.getByText(/Local API scheduler/)).toBeInTheDocument();
    expect(screen.getByText(/paper_trading_intraday_monitor/)).toBeInTheDocument();
    expect(screen.getAllByText(/analyse-worker/).length).toBeGreaterThan(0);
    expect(screen.getByText(/Running · 5m/)).toBeInTheDocument();
    expect(screen.getByText(/Intraday monitor last actual run/)).toBeInTheDocument();
    expect(screen.getByText(/Latest observation bucket/)).toBeInTheDocument();
    expect(screen.getAllByText(/Duration/).length).toBeGreaterThan(0);
    expect(screen.getAllByText(/Quote successes/).length).toBeGreaterThan(0);
    expect(screen.getByText(/analyse container/)).toBeInTheDocument();
    expect(screen.queryByText('Not scheduled')).not.toBeInTheDocument();
  });

  it('silently refreshes while visible', async () => {
    const visibilitySpy = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('visible');
    const intervalSpy = vi.spyOn(window, 'setInterval');

    const view = render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);
    expect(await screen.findByText(/refreshes silently every 60 seconds/)).toBeInTheDocument();
    await waitFor(() => expect(mocks.getStatus).toHaveBeenCalledTimes(1));
    const refreshTimer = intervalSpy.mock.calls.find(([, delay]) => delay === 60_000);
    expect(refreshTimer).toBeDefined();

    await act(async () => {
      (refreshTimer?.[0] as () => void)();
    });
    await waitFor(() => expect(mocks.getStatus).toHaveBeenCalledTimes(2));

    view.unmount();
    intervalSpy.mockRestore();
    visibilitySpy.mockRestore();
  });

  it('routes list reads to the selected experiment scope in read-only mode', async () => {
    mocks.getScopes.mockResolvedValue([
      { scopeId: 'primary', label: '主账户', kind: 'primary' },
      { scopeId: 'experiment:exp1:a', label: 'exp1 · A组', kind: 'experiment', experimentId: 'exp1', arm: 'a' },
      { scopeId: 'experiment:exp1:b', label: 'exp1 · B组', kind: 'experiment', experimentId: 'exp1', arm: 'b' },
    ]);

    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);
    const scopeSelect = await screen.findByLabelText('Execution scope (read-only comparison)');
    await waitFor(() => expect(mocks.getAccounts).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50, scopeId: 'primary' }));

    fireEvent.change(scopeSelect, { target: { value: 'experiment:exp1:b' } });

    await waitFor(() => expect(mocks.getAccounts).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50, scopeId: 'experiment:exp1:b' }));
    expect(mocks.getPositions).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50, scopeId: 'experiment:exp1:b' });
    expect(mocks.getHoldings).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50, scopeId: 'experiment:exp1:b' });
    // 只读对照横幅出现，运行按钮被禁用，不允许在实验分组上触发监控运行。
    expect(screen.getByText(/Read-only comparison:/)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: /Run on daily bars/ })).toBeDisabled();
    // 非范围端点（如资金调整）不携带 scope_id，仍读主库。
    expect(mocks.getAdjustments).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50 });
  });

  it('hides the scope selector when only the primary scope exists', async () => {
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);
    await screen.findByText(/No real order is sent/);
    expect(screen.queryByLabelText('Execution scope (read-only comparison)')).not.toBeInTheDocument();
  });

  it('keeps the top cards on their own first-page reads while audit tables page', async () => {
    render(<UiLanguageProvider><PaperTradingPage /></UiLanguageProvider>);
    await screen.findByText(/No real order is sent/);
    await waitFor(() => expect(mocks.getScopes).toHaveBeenCalledTimes(1));
    expect(mocks.getAccounts).toHaveBeenCalledWith(undefined, { page: 1, limit: 500, scopeId: 'primary' });
    expect(mocks.getPlans).toHaveBeenCalledWith(undefined, { page: 1, limit: 100, scopeId: 'primary' });
    expect(mocks.getPositions).toHaveBeenCalledWith(undefined, { page: 1, limit: 500, scopeId: 'primary', status: 'open' });
    expect(mocks.getPositions).toHaveBeenCalledWith(undefined, { page: 1, limit: 20, scopeId: 'primary', status: 'closed' });
    expect(mocks.getHoldings).toHaveBeenCalledWith(undefined, { page: 1, limit: 500, scopeId: 'primary' });
    // 审计表停在第一页时，顶部直接复用同一份风险快照，不重复请求。
    expect(mocks.getRiskSnapshots).toHaveBeenCalledTimes(1);

    const snapshotBox = screen.getByText('Latest risk snapshots and capacity').parentElement as HTMLElement;
    fireEvent.click(within(snapshotBox).getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(mocks.getRiskSnapshots).toHaveBeenCalledWith(undefined, { page: 2, limit: 50 }));
    // 审计表翻到第 2 页后，顶部概览另取第一页，不跟着翻页。
    await waitFor(() => expect(mocks.getRiskSnapshots).toHaveBeenLastCalledWith(undefined, { page: 1, limit: 50 }));
  });
});

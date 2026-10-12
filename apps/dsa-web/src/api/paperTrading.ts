import apiClient from './index';
import { toCamelCase } from './utils';
import type { PaperAuctionObservation, PaperCapitalAccount, PaperCapitalAdjustment, PaperDispatch, PaperEvaluation, PaperHolding, PaperMonitorStatus, PaperObservation, PaperOperation, PaperPerformance, PaperPlan, PaperPosition, PaperReanalysisRequest, PaperReportPlanReconciliation, PaperResolution, PaperRiskEvent, PaperRiskLock, PaperRiskRevision, PaperRiskSnapshot, PaperSyncLink, PaperTradeEvent, PaperTradingScope } from '../types/paperTrading';

type ListResponse<T> = { total: number; items: T[] };
type PageParams = { page?: number; limit?: number; scopeId?: string };
type PlanParams = PageParams & { status?: string };
type PositionParams = PageParams & { status?: 'open' | 'closed' };
type AccountParams = PageParams & { market?: string };
type AdjustmentParams = PageParams & { accountId?: number; adjustmentType?: 'deposit' | 'withdrawal' };
type DispatchParams = PageParams & { status?: string };
type ReconciliationParams = PageParams & { status?: string };
type AccountCreateInput = { sourceModel: string; market: string; initialCapital?: number };
type AdjustmentInput = { adjustmentType: 'deposit' | 'withdrawal'; amount: number; reason: string; idempotencyKey: string };
type RiskManualInput = { accountId: number; reason: string; idempotencyKey: string };
type PortfolioImportInput = { portfolioPositionId: number; sourceModels: string[]; tradeDate: string; idempotencyKey: string };
type PaperToPortfolioInput = { paperPositionId: number; portfolioAccountId: number; side: 'buy' | 'sell'; quantity: number; price: number; tradeDate: string; fee?: number; idempotencyKey: string; note?: string };
type MutationResponse<T> = { created: boolean; item: T; account?: PaperCapitalAccount | null };
type ListMutationResponse<T> = { created: boolean; items: T[] };

const modelParams = (model?: string) => (model ? { model } : {});
const scopeParams = (scopeId?: string) => (scopeId && scopeId !== 'primary' ? { scope_id: scopeId } : {});

export const paperTradingApi = {
  getStatus: async (): Promise<PaperMonitorStatus> => {
    const response = await apiClient.get('/api/v1/paper-trading/status');
    return toCamelCase<PaperMonitorStatus>(response.data);
  },
  getScopes: async (): Promise<PaperTradingScope[]> => {
    const response = await apiClient.get('/api/v1/paper-trading/scopes');
    return toCamelCase<{ items: PaperTradingScope[] }>(response.data).items;
  },
  getPlans: async (model?: string, page: PlanParams = {}): Promise<ListResponse<PaperPlan>> => {
    const response = await apiClient.get('/api/v1/paper-trading/plans', { params: { ...modelParams(model), ...scopeParams(page.scopeId), ...(page.status ? { status: page.status } : {}), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperPlan>>(response.data);
  },
  getOperations: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperOperation>> => {
    const response = await apiClient.get('/api/v1/paper-trading/operations', { params: { ...modelParams(model), ...scopeParams(page.scopeId), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperOperation>>(response.data);
  },
  getPositions: async (model?: string, page: PositionParams = {}): Promise<ListResponse<PaperPosition>> => {
    const response = await apiClient.get('/api/v1/paper-trading/positions', { params: { ...modelParams(model), ...scopeParams(page.scopeId), ...(page.status ? { status: page.status } : {}), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperPosition>>(response.data);
  },
  getHoldings: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperHolding>> => {
    const response = await apiClient.get('/api/v1/paper-trading/holdings', { params: { ...modelParams(model), ...scopeParams(page.scopeId), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperHolding>>(response.data);
  },
  getEvents: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperTradeEvent>> => {
    const response = await apiClient.get('/api/v1/paper-trading/events', { params: { ...modelParams(model), ...scopeParams(page.scopeId), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperTradeEvent>>(response.data);
  },
  getResolutions: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperResolution>> => {
    const response = await apiClient.get('/api/v1/paper-trading/resolutions', { params: { ...modelParams(model), ...scopeParams(page.scopeId), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperResolution>>(response.data);
  },
  getRiskRevisions: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperRiskRevision>> => {
    const response = await apiClient.get('/api/v1/paper-trading/risk-revisions', { params: { ...modelParams(model), ...scopeParams(page.scopeId), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperRiskRevision>>(response.data);
  },
  getEvaluations: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperEvaluation>> => {
    const response = await apiClient.get('/api/v1/paper-trading/evaluations', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperEvaluation>>(response.data);
  },
  getObservations: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperObservation>> => {
    const response = await apiClient.get('/api/v1/paper-trading/observations', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperObservation>>(response.data);
  },
  getReanalysisRequests: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperReanalysisRequest>> => {
    const response = await apiClient.get('/api/v1/paper-trading/reanalysis-requests', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperReanalysisRequest>>(response.data);
  },
  getReportPlanReconciliations: async (model?: string, page: ReconciliationParams = {}): Promise<ListResponse<PaperReportPlanReconciliation>> => {
    const response = await apiClient.get('/api/v1/paper-trading/report-plan-reconciliations', {
      params: {
        ...modelParams(model),
        ...(page.status ? { status: page.status } : {}),
        page: page.page ?? 1,
        limit: page.limit ?? 50,
      },
    });
    return toCamelCase<ListResponse<PaperReportPlanReconciliation>>(response.data);
  },
  getAuctionObservations: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperAuctionObservation>> => {
    const response = await apiClient.get('/api/v1/paper-trading/auction-observations', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperAuctionObservation>>(response.data);
  },
  getRiskSnapshots: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperRiskSnapshot>> => {
    const response = await apiClient.get('/api/v1/paper-trading/risk-snapshots', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperRiskSnapshot>>(response.data);
  },
  getRiskLocks: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperRiskLock>> => {
    const response = await apiClient.get('/api/v1/paper-trading/account-risk/locks', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperRiskLock>>(response.data);
  },
  getRiskEvents: async (model?: string, page: PageParams = {}): Promise<ListResponse<PaperRiskEvent>> => {
    const response = await apiClient.get('/api/v1/paper-trading/risk-events', { params: { ...modelParams(model), page: page.page ?? 1, limit: page.limit ?? 50 } });
    return toCamelCase<ListResponse<PaperRiskEvent>>(response.data);
  },
  getPerformance: async (model?: string): Promise<PaperPerformance> => {
    const response = await apiClient.get('/api/v1/paper-trading/performance', { params: modelParams(model) });
    return toCamelCase<{ metrics: PaperPerformance }>(response.data).metrics;
  },
  getAccounts: async (model?: string, page: AccountParams = {}): Promise<ListResponse<PaperCapitalAccount>> => {
    const response = await apiClient.get('/api/v1/paper-trading/accounts', {
      params: {
        ...modelParams(model),
        ...scopeParams(page.scopeId),
        ...(page.market ? { market: page.market } : {}),
        page: page.page ?? 1,
        limit: page.limit ?? 50,
      },
    });
    return toCamelCase<ListResponse<PaperCapitalAccount>>(response.data);
  },
  createAccount: async (input: AccountCreateInput): Promise<MutationResponse<PaperCapitalAccount>> => {
    const response = await apiClient.post('/api/v1/paper-trading/accounts', {
      source_model: input.sourceModel,
      market: input.market,
      ...(input.initialCapital == null ? {} : { initial_capital: input.initialCapital }),
    });
    return toCamelCase<MutationResponse<PaperCapitalAccount>>(response.data);
  },
  adjustAccount: async (accountId: number, input: AdjustmentInput): Promise<MutationResponse<PaperCapitalAdjustment>> => {
    const response = await apiClient.post(`/api/v1/paper-trading/accounts/${accountId}/adjustments`, {
      adjustment_type: input.adjustmentType,
      amount: input.amount,
      reason: input.reason,
      idempotency_key: input.idempotencyKey,
    });
    return toCamelCase<MutationResponse<PaperCapitalAdjustment>>(response.data);
  },
  manualHold: async (input: RiskManualInput): Promise<MutationResponse<PaperRiskEvent>> => {
    const response = await apiClient.post('/api/v1/paper-trading/risk/manual-hold', {
      account_id: input.accountId,
      reason: input.reason,
      idempotency_key: input.idempotencyKey,
    });
    return toCamelCase<MutationResponse<PaperRiskEvent>>(response.data);
  },
  manualResume: async (input: RiskManualInput): Promise<MutationResponse<PaperRiskEvent>> => {
    const response = await apiClient.post('/api/v1/paper-trading/risk/manual-resume', {
      account_id: input.accountId,
      reason: input.reason,
      idempotency_key: input.idempotencyKey,
    });
    return toCamelCase<MutationResponse<PaperRiskEvent>>(response.data);
  },
  syncFromPortfolio: async (input: PortfolioImportInput): Promise<ListMutationResponse<PaperSyncLink>> => {
    const response = await apiClient.post('/api/v1/paper-trading/sync/from-portfolio', {
      portfolio_position_id: input.portfolioPositionId,
      source_models: input.sourceModels,
      trade_date: input.tradeDate,
      idempotency_key: input.idempotencyKey,
    });
    return toCamelCase<ListMutationResponse<PaperSyncLink>>(response.data);
  },
  syncToPortfolio: async (input: PaperToPortfolioInput): Promise<MutationResponse<PaperSyncLink>> => {
    const response = await apiClient.post('/api/v1/paper-trading/sync/to-portfolio', {
      paper_position_id: input.paperPositionId,
      portfolio_account_id: input.portfolioAccountId,
      side: input.side,
      quantity: input.quantity,
      price: input.price,
      trade_date: input.tradeDate,
      fee: input.fee ?? 0,
      idempotency_key: input.idempotencyKey,
      note: input.note,
    });
    return toCamelCase<MutationResponse<PaperSyncLink>>(response.data);
  },
  getAdjustments: async (model?: string, page: AdjustmentParams = {}): Promise<ListResponse<PaperCapitalAdjustment>> => {
    const response = await apiClient.get('/api/v1/paper-trading/adjustments', {
      params: {
        ...modelParams(model),
        ...(page.accountId ? { account_id: page.accountId } : {}),
        ...(page.adjustmentType ? { adjustment_type: page.adjustmentType } : {}),
        page: page.page ?? 1,
        limit: page.limit ?? 50,
      },
    });
    return toCamelCase<ListResponse<PaperCapitalAdjustment>>(response.data);
  },
  getDispatches: async (model?: string, page: DispatchParams = {}): Promise<ListResponse<PaperDispatch>> => {
    const response = await apiClient.get('/api/v1/paper-trading/dispatches', {
      params: {
        ...modelParams(model),
        ...(page.status ? { status: page.status } : {}),
        page: page.page ?? 1,
        limit: page.limit ?? 50,
      },
    });
    return toCamelCase<ListResponse<PaperDispatch>>(response.data);
  },
  run: async (mode: 'eod' | 'intraday' = 'eod'): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/api/v1/paper-trading/run', { mode });
    return toCamelCase<{ stats: Record<string, unknown> }>(response.data).stats;
  },
};

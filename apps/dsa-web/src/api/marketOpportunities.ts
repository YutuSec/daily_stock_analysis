import apiClient from './index';
import { toCamelCase } from './utils';
import type {
  MarketOpportunityAnalysisJob,
  MarketOpportunityActionResponse,
  MarketOpportunityCandidate,
  MarketOpportunityEvaluationResponse,
  MarketOpportunityFilters,
  MarketOpportunityListResponse,
  MarketOpportunityRun,
  MarketOpportunityRunDetail,
  MarketOpportunityStatusResponse,
  MarketOpportunityTheme,
} from '../types/marketOpportunities';

const cleanParams = (params: Record<string, unknown>) => (
  Object.fromEntries(
    Object.entries(params).filter(([, value]) => value !== undefined && value !== null && value !== ''),
  )
);

const toQueryParams = (filters: MarketOpportunityFilters = {}) => cleanParams({
  run_id: filters.runId,
  market: filters.market,
  mode: filters.mode,
  status: filters.status,
  model: filters.model,
  theme_type: filters.themeType,
  trade_date_from: filters.tradeDateFrom,
  trade_date_to: filters.tradeDateTo,
  page: filters.page,
  limit: filters.limit,
});

export const marketOpportunitiesApi = {
  async getStatus(): Promise<MarketOpportunityStatusResponse> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/status');
    return toCamelCase<MarketOpportunityStatusResponse>(response.data);
  },

  async getRuns(filters: MarketOpportunityFilters = {}): Promise<MarketOpportunityListResponse<MarketOpportunityRun>> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/runs', {
      params: toQueryParams(filters),
    });
    return toCamelCase<MarketOpportunityListResponse<MarketOpportunityRun>>(response.data);
  },

  async getRun(id: number): Promise<MarketOpportunityRunDetail> {
    const response = await apiClient.get<Record<string, unknown>>(`/api/v1/market-opportunities/runs/${id}`);
    return toCamelCase<MarketOpportunityRunDetail>(response.data);
  },

  async getThemes(filters: MarketOpportunityFilters = {}): Promise<MarketOpportunityListResponse<MarketOpportunityTheme>> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/themes', {
      params: toQueryParams(filters),
    });
    return toCamelCase<MarketOpportunityListResponse<MarketOpportunityTheme>>(response.data);
  },

  async getCandidates(filters: MarketOpportunityFilters = {}): Promise<MarketOpportunityListResponse<MarketOpportunityCandidate>> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/candidates', {
      params: toQueryParams(filters),
    });
    return toCamelCase<MarketOpportunityListResponse<MarketOpportunityCandidate>>(response.data);
  },

  async getAnalysisJobs(filters: MarketOpportunityFilters = {}): Promise<MarketOpportunityListResponse<MarketOpportunityAnalysisJob>> {
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/analysis-jobs', {
      params: toQueryParams(filters),
    });
    return toCamelCase<MarketOpportunityListResponse<MarketOpportunityAnalysisJob>>(response.data);
  },

  async getEvaluation(filters: Pick<MarketOpportunityFilters, 'runId' | 'market'> & { windows?: number[] } = {}): Promise<MarketOpportunityEvaluationResponse> {
    const windows = filters.windows ?? [1, 3, 5, 10, 20, 40];
    const response = await apiClient.get<Record<string, unknown>>('/api/v1/market-opportunities/evaluation', {
      params: cleanParams({
        run_id: filters.runId,
        market: filters.market,
        windows: windows.join(','),
      }),
    });
    return toCamelCase<MarketOpportunityEvaluationResponse>(response.data);
  },

  async discover(input: { market?: string; reportId?: number } = {}): Promise<MarketOpportunityActionResponse> {
    const response = await apiClient.post<Record<string, unknown>>('/api/v1/market-opportunities/discover', {
      market: input.market ?? 'cn',
      ...(input.reportId ? { report_id: input.reportId } : {}),
    });
    return toCamelCase<MarketOpportunityActionResponse>(response.data);
  },

  async analyzeRun(runId: number, limit = 20): Promise<MarketOpportunityActionResponse> {
    const response = await apiClient.post<Record<string, unknown>>(`/api/v1/market-opportunities/runs/${runId}/analyze`, {
      limit,
    });
    return toCamelCase<MarketOpportunityActionResponse>(response.data);
  },

  async updateMode(input: { mode: 'off' | 'observe' | 'paper'; expectedVersion: number; reason?: string }): Promise<MarketOpportunityStatusResponse> {
    const response = await apiClient.put<Record<string, unknown>>('/api/v1/market-opportunities/mode', {
      mode: input.mode,
      expected_version: input.expectedVersion,
      reason: input.reason,
    });
    return toCamelCase<MarketOpportunityStatusResponse>(response.data);
  },

  async clearMode(input: { expectedVersion: number; reason?: string }): Promise<MarketOpportunityStatusResponse> {
    const response = await apiClient.delete<Record<string, unknown>>('/api/v1/market-opportunities/mode', {
      data: {
        expected_version: input.expectedVersion,
        reason: input.reason,
      },
    });
    return toCamelCase<MarketOpportunityStatusResponse>(response.data);
  },
};

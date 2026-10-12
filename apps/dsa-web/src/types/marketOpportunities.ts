export interface MarketOpportunityModelState {
  model: string;
  status: string;
  jobId?: number | null;
  taskId?: string | null;
  queryId?: string | null;
  analysisHistoryId?: number | null;
  decisionSignalId?: number | null;
  paperPlanId?: number | null;
  paperReconciliation?: Record<string, unknown>;
  planExecutable?: boolean | null;
  planStatus?: string | null;
  confirmationProgress?: {
    current?: number | null;
    required?: number | null;
    completed?: boolean;
    reason?: string | null;
  } | null;
  followup?: {
    status?: string | null;
    result?: string | null;
    reason?: string | null;
    dueDate?: string | null;
    expiresOn?: string | null;
    spawnedRunId?: number | null;
    spawnedOn?: string | null;
    confirmationCurrent?: number | null;
    confirmationRequired?: number | null;
  } | null;
  executionIntent?: 'paper_candidate' | 'research_only' | string | null;
  research?: MarketOpportunityResearchState | null;
  action?: string | null;
  actionLabel?: string | null;
  failureReason?: string | null;
  noPlanReason?: string | null;
  updatedAt?: string | null;
}

export interface MarketOpportunityResearchReview {
  status?: string | null;
  result?: string | null;
  reason?: string | null;
  dueDate?: string | null;
  expiresOn?: string | null;
  spawnedRunId?: number | null;
  spawnedOn?: string | null;
  closedOn?: string | null;
  [key: string]: unknown;
}

export interface MarketOpportunityResearchState {
  kind?: 'initial' | 'review' | string | null;
  outcome?: string | null;
  bundleId?: string | null;
  review?: MarketOpportunityResearchReview | null;
  reviewOf?: Record<string, unknown> | null;
}

export interface MarketOpportunityRun {
  id: number;
  runKey: string;
  market: string;
  tradeDate?: string | null;
  sourceReportId?: number | null;
  marketRegimeId?: number | null;
  marketRegime?: string | null;
  mode: string;
  status: string;
  policyVersion: string;
  configSnapshot: Record<string, unknown>;
  provenance: Record<string, unknown> & {
    observationHealth?: {
      healthy?: boolean;
      reason?: string | null;
      sourceSnapshotId?: string | number | null;
    } | null;
  };
  themeCount: number;
  selectedThemeCount: number;
  candidateCount: number;
  selectedCandidateCount: number;
  llmBudget: number;
  errorMessage?: string | null;
  healthStatus?: string | null;
  healthReason?: string | null;
  startedAt?: string | null;
  completedAt?: string | null;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface MarketOpportunityMissingEvidence {
  field?: string | null;
  reason?: string | null;
  date?: string | null;
  source?: string | null;
  [key: string]: unknown;
}

export interface MarketOpportunityTheme {
  id: number;
  runId: number;
  themeKey: string;
  themeType: string;
  themeName: string;
  themeCode?: string | null;
  rank?: number | null;
  changePct?: number | null;
  status: string;
  score?: number | null;
  scoreComponents: Record<string, unknown>;
  rejectionReasons: unknown[];
  dataQuality: Record<string, unknown>;
  constituentCount: number;
  eligibleConstituentCount: number;
  createdAt?: string | null;
}

export interface MarketOpportunityCandidate {
  id: number;
  runId: number;
  candidateKey: string;
  market: string;
  exchange: string;
  stockCode: string;
  symbol: string;
  stockName?: string | null;
  primaryThemeKey?: string | null;
  relatedThemes: unknown[];
  status: 'observing' | 'selected' | 'rejected' | string;
  rank?: number | null;
  score?: number | null;
  scoreComponents: Record<string, unknown> & {
    evidenceStage?: 'cold_start' | 'established' | string;
    setupType?: 'pullback' | 'trend_continuation' | 'unconfirmed' | string;
    strategyComparison?: {
      legacySelected?: boolean | null;
      legacyScore?: number | null;
      legacyReasons?: unknown[];
      currentSelected?: boolean | null;
      policyVersion?: string | null;
      [key: string]: unknown;
    } | null;
  };
  rejectionReasons: unknown[];
  dataQuality: Record<string, unknown> & {
    missingEvidence?: MarketOpportunityMissingEvidence[] | string[] | null;
    executionProxy?: Record<string, unknown> | null;
    evaluationSafety?: Record<string, unknown> | null;
  };
  quoteSnapshot: Record<string, unknown>;
  asOf?: string | null;
  validUntil?: string | null;
  snapshotHash?: string | null;
  createdAt?: string | null;
  analysisJobs: MarketOpportunityAnalysisJob[];
  modelStates: MarketOpportunityModelState[];
  noPlanReason?: string | null;
}

export interface MarketOpportunityAnalysisJob {
  id: number;
  runId: number;
  candidateId: number;
  jobKey: string;
  model: string;
  status: string;
  attemptCount: number;
  maxAttempts: number;
  taskId?: string | null;
  queryId?: string | null;
  analysisHistoryId?: number | null;
  decisionSignalId?: number | null;
  paperPlanId?: number | null;
  failureReason?: string | null;
  evidence: Record<string, unknown>;
  createdAt?: string | null;
  updatedAt?: string | null;
}

export interface MarketOpportunityStatusResponse {
  mode: string;
  enabled: boolean;
  modeSource?: string | null;
  modeVersion?: number | null;
  modeUpdatedAt?: string | null;
  modeUpdatedBy?: string | null;
  modeDegraded?: boolean;
  modeProvenance?: Record<string, unknown>;
  configuredModels: string[];
  limits: Record<string, unknown> & {
    maxObservationCandidates?: number | null;
    discoveryMaxThemes?: number | null;
    maxTotalCandidates?: number | null;
    maxLlmCallsPerRun?: number | null;
  };
  observeGate: Record<string, unknown> & {
    totalRunDays?: number | null;
    healthyRunDays?: number | null;
    completedTradingDays?: number | null;
    requiredTradingDays?: number | null;
    countingBasis?: string | null;
    excludedDates?: Array<{ date?: string | null; reason?: string | null; [key: string]: unknown }>;
  };
  message: string;
}

export interface MarketOpportunityListResponse<T> {
  total: number;
  page: number;
  limit: number;
  items: T[];
  configuredModels?: string[];
}

export interface MarketOpportunityRunDetail extends MarketOpportunityRun {
  themes: MarketOpportunityTheme[];
  candidates: MarketOpportunityCandidate[];
  analysisJobs: MarketOpportunityAnalysisJob[];
  configuredModels: string[];
  llmSubmittedJobCount?: number | null;
}

export interface MarketOpportunityActionResponse {
  status: string;
  result: Record<string, unknown>;
}

export interface MarketOpportunityExecutableEntry {
  status?: string | null;
  entryDate?: string | null;
  earliestSellDate?: string | null;
  price?: number | null;
  tradability?: Record<string, unknown> | null;
  [key: string]: unknown;
}

export interface MarketOpportunityPaperExecution {
  jobs?: number | null;
  reports?: number | null;
  plans?: number | null;
  positions?: number | null;
  tradeEvents?: number | null;
  totalFees?: number | null;
  totalCommission?: number | null;
  totalTax?: number | null;
  totalSlippageAmount?: number | null;
  grossRealizedPnl?: number | null;
  netRealizedPnl?: number | null;
  [key: string]: unknown;
}

export interface MarketOpportunityCandidateMetric {
  candidateId?: number | null;
  runId?: number | null;
  stockCode?: string | null;
  symbol?: string | null;
  stockName?: string | null;
  candidateStatus?: string | null;
  returnBasis?: string | null;
  entryPrice?: number | null;
  entryStatus?: string | null;
  executableEntry?: MarketOpportunityExecutableEntry | null;
  windows?: Record<string, Record<string, unknown>>;
  executableWindows?: Record<string, Record<string, unknown>>;
  paperExecution?: MarketOpportunityPaperExecution | null;
  [key: string]: unknown;
}

export interface MarketOpportunityEvaluationResponse {
  windows: number[];
  candidateCount: number;
  candidateMetrics: MarketOpportunityCandidateMetric[];
  summary: Record<string, unknown>;
  themeSummary: Record<string, unknown>;
  modelExecutionSummary: Record<string, unknown>;
}

export interface MarketOpportunityFilters {
  runId?: number;
  market?: string;
  mode?: string;
  status?: string;
  model?: string;
  themeType?: string;
  tradeDateFrom?: string;
  tradeDateTo?: string;
  page?: number;
  limit?: number;
}

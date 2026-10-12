import { fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { UiLanguageProvider } from '../../contexts/UiLanguageContext';
import MarketOpportunitiesPage from '../MarketOpportunitiesPage';

const {
  mockGetStatus,
  mockGetRuns,
  mockGetRun,
  mockGetCandidates,
  mockGetEvaluation,
  mockDiscover,
  mockAnalyzeRun,
  mockUpdateMode,
  mockClearMode,
  mockGetConfig,
  mockUpdateConfig,
} = vi.hoisted(() => ({
  mockGetStatus: vi.fn(),
  mockGetRuns: vi.fn(),
  mockGetRun: vi.fn(),
  mockGetCandidates: vi.fn(),
  mockGetEvaluation: vi.fn(),
  mockDiscover: vi.fn(),
  mockAnalyzeRun: vi.fn(),
  mockUpdateMode: vi.fn(),
  mockClearMode: vi.fn(),
  mockGetConfig: vi.fn(),
  mockUpdateConfig: vi.fn(),
}));

vi.mock('../../api/marketOpportunities', () => ({
  marketOpportunitiesApi: {
    getStatus: mockGetStatus,
    getRuns: mockGetRuns,
    getRun: mockGetRun,
    getCandidates: mockGetCandidates,
    getEvaluation: mockGetEvaluation,
    discover: mockDiscover,
    analyzeRun: mockAnalyzeRun,
    updateMode: mockUpdateMode,
    clearMode: mockClearMode,
  },
}));

vi.mock('../../api/systemConfig', () => ({
  systemConfigApi: {
    getConfig: mockGetConfig,
    update: mockUpdateConfig,
  },
  SystemConfigConflictError: class SystemConfigConflictError extends Error {},
  SystemConfigValidationError: class SystemConfigValidationError extends Error {},
}));

const configuredModels = [
  'deepseek/deepseek-v4-pro',
  'anthropic/claude-sonnet-5',
  'custom/model-x',
];

const run = {
  id: 7,
  runKey: 'cn:2026-08-31:market',
  market: 'cn',
  tradeDate: '2026-08-31',
  sourceReportId: 99,
  marketRegimeId: 3,
  marketRegime: 'normal',
  mode: 'observe',
  status: 'completed',
  healthStatus: 'observing',
  healthReason: null,
  policyVersion: 'opportunity-policy-v1',
  configSnapshot: {},
  provenance: {
    source: 'test',
    observationCandidateCount: 30,
    observationHealth: {
      healthy: true,
      reason: 'zero_candidates',
      sourceSnapshotId: 'snapshot-7',
    },
  },
  themeCount: 1,
  selectedThemeCount: 1,
  candidateCount: 42,
  selectedCandidateCount: 8,
  llmBudget: 16,
  startedAt: '2026-08-31T15:10:00',
  completedAt: '2026-08-31T15:20:00',
  createdAt: '2026-08-31T15:10:00',
  updatedAt: '2026-08-31T15:20:00',
};

const candidate = {
  id: 11,
  runId: 7,
  candidateKey: 'cn:sz:002173',
  market: 'cn',
  exchange: 'SZ',
  stockCode: '002173',
  symbol: '002173.SZ',
  stockName: '创新医疗',
  primaryThemeKey: 'concept:创新药',
  relatedThemes: ['concept:创新药'],
  status: 'selected',
  rank: 1,
  score: 76,
  scoreComponents: {
    relativeStrength: 18,
    evidenceStage: 'cold_start',
    setupType: 'trend_continuation',
    strategyComparison: {
      legacySelected: false,
      legacyScore: 63,
      legacyReasons: ['persistence_insufficient'],
      currentSelected: true,
      policyVersion: 'opportunity-policy-v3',
    },
  },
  rejectionReasons: [],
  dataQuality: {
    status: 'ok',
    missingEvidence: [{ date: '2026-08-31', field: 'persistence', reason: 'history_not_ready' }],
  },
  quoteSnapshot: { source: 'daily' },
  asOf: '2026-08-31',
  validUntil: '2026-09-01',
  snapshotHash: 'hash',
  createdAt: '2026-08-31T15:20:00',
  analysisJobs: [],
  modelStates: [
    {
      model: configuredModels[0],
      status: 'completed',
      analysisHistoryId: 123,
      decisionSignalId: 456,
      paperPlanId: 789,
      planExecutable: false,
      planStatus: 'non_executable',
      confirmationProgress: {
        current: 1,
        required: 2,
        completed: false,
        reason: 'awaiting_entry_confirmation',
      },
      followup: {
        status: 'spawned',
        dueDate: '2026-09-01',
        spawnedRunId: 8,
      },
      noPlanReason: 'awaiting_entry_confirmation',
      updatedAt: '2026-08-31T15:21:00',
    },
    {
      model: configuredModels[1],
      status: 'missing',
      noPlanReason: 'no_analysis_job',
    },
    {
      model: configuredModels[2],
      status: 'missing',
      noPlanReason: 'no_analysis_job',
    },
  ],
  noPlanReason: 'completed_without_paper_plan',
};

function makeStatus(overrides: Record<string, unknown> = {}) {
  return {
    mode: 'observe',
    enabled: true,
    modeSource: 'database',
    modeVersion: 4,
    modeUpdatedAt: '2026-08-31T15:18:00',
    modeUpdatedBy: 'web-admin',
    modeDegraded: false,
    modeProvenance: { setting: 'MARKET_OPPORTUNITY_MODE', validation: 'strict_enum' },
    configuredModels,
    limits: { maxTotalCandidates: 8, maxObservationCandidates: 30, discoveryMaxThemes: 15 },
    observeGate: {
      requiredTradingDays: 3,
      completedTradingDays: 2,
      totalRunDays: 3,
      healthyRunDays: 2,
      countingBasis: 'healthy_observation_days',
      dates: ['2026-08-28', '2026-08-31'],
      excludedDates: [{ date: '2026-08-29', reason: 'source_unavailable' }],
      positiveTradingDates: ['2026-08-31'],
      completed: false,
    },
    message: 'observe only',
    ...overrides,
  };
}

beforeEach(() => {
  vi.clearAllMocks();
  mockGetStatus.mockResolvedValue(makeStatus());
  mockGetRuns.mockResolvedValue({
    total: 1,
    page: 1,
    limit: 20,
    items: [run],
    configuredModels,
  });
  mockGetRun.mockResolvedValue({
    ...run,
    themes: [{
      id: 1,
      runId: 7,
      themeKey: 'concept:创新药',
      themeType: 'concept',
      themeName: '创新药',
      status: 'selected',
      score: 82.5,
      scoreComponents: { breadth: 20 },
      rejectionReasons: [],
      dataQuality: { status: 'ok' },
      constituentCount: 80,
      eligibleConstituentCount: 12,
      createdAt: '2026-08-31T15:20:00',
    }],
    candidates: [candidate],
    analysisJobs: [],
    configuredModels,
  });
  mockGetCandidates.mockResolvedValue({
    total: 501,
    page: 1,
    limit: 20,
    items: [candidate],
    configuredModels,
  });
  mockGetEvaluation.mockResolvedValue({
    windows: [1, 3, 5, 10, 20, 40],
    candidateCount: 1,
    candidateMetrics: [{
      candidateId: 11,
      stockCode: '002173',
      symbol: '002173.SZ',
      returnBasis: 'signal_close_to_future_close_non_executable',
      entryPrice: 10,
      entryStatus: 'ok',
      executableEntry: {
        status: 'daily_open_proxy_unconfirmed',
        entryDate: '2026-09-01',
        earliestSellDate: '2026-09-02',
        price: 10.1,
        tradability: {
          fillConfirmed: false,
          limitUnfillableStatus: 'cannot_confirm_without_limit_price_or_minute_data',
        },
      },
      windows: {
        1: { status: 'ok', returnPct: 5, returnBasis: 'signal_close_to_future_close_non_executable' },
        20: { status: 'insufficient_data', returnBasis: 'signal_close_to_future_close_non_executable' },
        40: { status: 'insufficient_data', returnBasis: 'signal_close_to_future_close_non_executable' },
      },
      executableWindows: {
        1: { status: 'ok', returnPct: 3.9604, returnBasis: 'next_session_open_daily_proxy_unconfirmed' },
      },
      paperExecution: {
        jobs: 1,
        reports: 1,
        plans: 1,
        positions: 1,
        tradeEvents: 1,
        totalFees: 7,
        totalCommission: 5,
        totalTax: 1,
        totalSlippageAmount: 1,
        netRealizedPnl: 43,
      },
    }],
    summary: {
      candidateCount: 1,
      windows: {
        1: { evaluated: 1, hitRate: 1, avgReturnPct: 2.5 },
        3: { evaluated: 0, hitRate: null, avgReturnPct: null },
        5: { evaluated: 0, hitRate: null, avgReturnPct: null },
        10: { evaluated: 0, hitRate: null, avgReturnPct: null },
        20: { evaluated: 0, hitRate: null, avgReturnPct: null },
        40: { evaluated: 0, hitRate: null, avgReturnPct: null },
      },
    },
    themeSummary: {},
    modelExecutionSummary: {
      'deepseek/deepseek-v4-pro': {
        jobs: 1,
        reports: 1,
        plans: 0,
        realizedPnl: 0,
      },
    },
  });
  mockUpdateMode.mockResolvedValue(makeStatus({
    mode: 'off',
    enabled: false,
    modeVersion: 5,
    message: 'off',
  }));
  mockClearMode.mockResolvedValue(makeStatus({
    mode: 'off',
    enabled: false,
    modeSource: 'environment',
    modeVersion: 6,
    modeUpdatedAt: null,
    modeUpdatedBy: null,
    modeProvenance: { fallback: 'config.market_opportunity_mode' },
    message: 'environment fallback',
  }));
  mockGetConfig.mockResolvedValue({
    configVersion: 'cfg-v1',
    maskToken: '******',
    items: [{ key: 'MARKET_OPPORTUNITY_STYLE', value: '', rawValueExists: true, isMasked: false }],
  });
  mockUpdateConfig.mockResolvedValue({
    success: true,
    configVersion: 'cfg-v2',
    appliedCount: 1,
    skippedMaskedCount: 0,
    reloadTriggered: true,
    updatedKeys: ['MARKET_OPPORTUNITY_STYLE'],
    warnings: [],
  });
});

describe('MarketOpportunitiesPage', () => {
  it('renders dynamic configured model states, runtime provenance, and no-plan reasons', async () => {
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('大盘风口机会池')).toBeInTheDocument();
    expect(await screen.findByText('观察模式')).toBeInTheDocument();
    expect(screen.getByText(/当前来源 database，版本 4/)).toBeInTheDocument();
    expect(screen.getByText(/MARKET_OPPORTUNITY_MODE/)).toBeInTheDocument();
    expect(screen.getByText(/无需重启容器/)).toBeInTheDocument();
    expect(screen.getByText(/健康 3 日是工程验收/)).toBeInTheDocument();
    expect(screen.getByText('2/3')).toBeInTheDocument();
    expect(screen.getByText(/计数口径 健康观察日累计/)).toBeInTheDocument();
    expect(screen.getByText(/总运行日 3，健康运行日 2/)).toBeInTheDocument();
    expect(screen.getByText(/排除日期：2026-08-29 · 数据源不可用/)).toBeInTheDocument();
    expect(await screen.findByText(/当前批次 观察候选 30 · 达到分析条件 8/)).toBeInTheDocument();
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('30');
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('可分析上限 8');
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('主题发现 15');
    expect(screen.getByText(/观察日期 2026-08-28 \/ 2026-08-31/)).toBeInTheDocument();
    expect(screen.getByText(/正向日期 2026-08-31/)).toBeInTheDocument();
    expect(screen.getByText(/候选与自选分离/)).toHaveTextContent('本页没有“加入自选”或真实交易按钮');
    expect(await screen.findByText('前瞻评估')).toBeInTheDocument();
    expect(await screen.findByText('样本 1')).toBeInTheDocument();
    expect(await screen.findByText('平均收益 2.5')).toBeInTheDocument();
    expect(await screen.findByText('20 日窗口')).toBeInTheDocument();
    expect(await screen.findByText('40 日窗口')).toBeInTheDocument();
    expect(screen.getAllByText('尚未成熟，等待足够交易日')).toHaveLength(2);
    expect(await screen.findByText('创新医疗')).toBeInTheDocument();
    expect(screen.getByText('002173.SZ · 002173 · as-of 2026-08-31 · valid until 2026-09-01')).toBeInTheDocument();

    const candidateCard = screen.getByText('创新医疗').closest('details');
    expect(candidateCard).not.toBeNull();
    const scope = within(candidateCard as HTMLElement);
    expect(scope.getByText('可分析 · 达到分析条件')).toBeInTheDocument();
    expect(scope.getByText(/达到分析资格；当前模式不会提交真实买入。/)).toBeInTheDocument();
    expect(scope.getByText('冷启动')).toBeInTheDocument();
    expect(scope.getByText('趋势延续')).toBeInTheDocument();
    expect(scope.getByText(/旧策略不选 · 新策略会选 · 旧分 63 · 策略 opportunity-policy-v3/)).toBeInTheDocument();
    expect(scope.getByText(/缺失证据：2026-08-31 · 持续性证据 · 历史样本尚未准备好/)).toBeInTheDocument();
    expect(scope.getByText('执行评估证据')).toBeInTheDocument();
    expect(scope.getByText(/收益口径/)).toHaveTextContent('信号收盘到未来收盘，仅作观察，不代表可执行收益');
    expect(scope.getByText(/信号收盘 1日/)).toHaveTextContent('5');
    expect(scope.getByText(/次日开盘代理/)).toHaveTextContent('日线开盘代理，未确认真实成交');
    expect(scope.getByText(/T\+1 最早卖出/)).toHaveTextContent('2026-09-02');
    expect(scope.getByText(/可成交确认/)).toHaveTextContent('否');
    expect(scope.getByText(/涨跌停可成交/)).toHaveTextContent('缺少涨跌停价格或分钟线，无法确认');
    expect(scope.getByText(/执行代理 1日/)).toHaveTextContent('3.9604');
    expect(scope.getByText(/执行口径/)).toHaveTextContent('下一交易日开盘代理，未确认真实成交');
    expect(scope.getByText(/纸面仓位/)).toHaveTextContent('1');
    expect(scope.getByText(/费用/)).toHaveTextContent('7');
    expect(scope.getByText(/佣金/)).toHaveTextContent('5');
    expect(scope.getByText(/净已实现盈亏/)).toHaveTextContent('43');
    configuredModels.forEach((model) => {
      expect(scope.getByText(model)).toBeInTheDocument();
    });
    expect(scope.getByText('计划已生成，但不可执行 · 不可执行')).toBeInTheDocument();
    expect(scope.getByText('交易日确认 1/2 · 未完成')).toBeInTheDocument();
    expect(scope.getByText('次交易日复核：已派生 · 到期 2026-09-01 · run #8')).toBeInTheDocument();
    expect(scope.getByText('等待下一交易日确认，当前不会买入')).toBeInTheDocument();
    expect(screen.getByText(/健康状态 观察中/)).toBeInTheDocument();
    expect(screen.getByText(/观察健康 健康/)).toBeInTheDocument();
    expect(screen.getByText(/快照 snapshot-7/)).toBeInTheDocument();
    expect(screen.getByText('仅发现')).toBeInTheDocument();
    expect(screen.getByText('分析候选（模拟链路）')).toBeDisabled();
    expect(screen.getByText('候选第 1 页 · 当前加载 1 / 总数 501')).toBeInTheDocument();

    await waitFor(() => {
      expect(mockGetCandidates).toHaveBeenCalledWith({
        runId: 7,
        status: undefined,
        model: undefined,
        page: 1,
        limit: 20,
      });
      expect(mockGetEvaluation).toHaveBeenCalledWith({ runId: 7, windows: [1, 3, 5, 10, 20, 40] });
    });

    fireEvent.click(screen.getByText('下一页候选'));

    await waitFor(() => {
      expect(mockGetCandidates).toHaveBeenCalledWith({
        runId: 7,
        status: undefined,
        model: undefined,
        page: 2,
        limit: 20,
      });
    });
  });

  it('updates mode with the current required expectedVersion', async () => {
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    const modeSelect = await screen.findByLabelText('运行模式（热切换）') as HTMLSelectElement;
    await waitFor(() => {
      expect(modeSelect.value).toBe('observe');
    });
    fireEvent.change(modeSelect, { target: { value: 'off' } });
    const saveButton = screen.getByText('保存模式');
    await waitFor(() => {
      expect(saveButton).not.toBeDisabled();
    });
    fireEvent.click(saveButton);

    await waitFor(() => {
      expect(mockUpdateMode).toHaveBeenCalledWith({
        mode: 'off',
        expectedVersion: 4,
        reason: 'web market opportunity mode switch',
      });
    });
    expect(await screen.findByText(/机会池模式已切换为 off/)).toBeInTheDocument();
  });

  it('refreshes authoritative status and rolls back the draft after a 409 conflict', async () => {
    mockUpdateMode.mockRejectedValueOnce({
      response: {
        status: 409,
        data: { detail: { error: 'runtime_setting_version_conflict', current_version: 5 } },
      },
      message: 'Conflict',
    });
    mockGetStatus
      .mockResolvedValueOnce(makeStatus({
        observeGate: { requiredTradingDays: 3, completedTradingDays: 3, completed: true },
      }))
      .mockResolvedValueOnce(makeStatus({
        mode: 'paper',
        modeVersion: 5,
        modeUpdatedAt: '2026-08-31T15:31:00',
        modeUpdatedBy: 'another-admin',
        observeGate: { requiredTradingDays: 3, completedTradingDays: 3, completed: true },
        message: 'paper',
      }));

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    const modeSelect = await screen.findByLabelText('运行模式（热切换）') as HTMLSelectElement;
    await waitFor(() => {
      expect(modeSelect.value).toBe('observe');
    });
    fireEvent.change(modeSelect, { target: { value: 'off' } });
    const saveButton = screen.getByText('保存模式');
    await waitFor(() => {
      expect(saveButton).not.toBeDisabled();
    });
    fireEvent.click(saveButton);

    expect(await screen.findByText(/状态已被其他入口更新/)).toBeInTheDocument();
    await waitFor(() => {
      expect(modeSelect.value).toBe('paper');
    });
  });

  it('clears database override with the current expectedVersion and shows environment fallback', async () => {
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    const clearButton = await screen.findByText('回退环境值');
    await waitFor(() => {
      expect(clearButton).not.toBeDisabled();
    });
    fireEvent.click(clearButton);

    await waitFor(() => {
      expect(mockClearMode).toHaveBeenCalledWith({
        expectedVersion: 4,
        reason: 'web clear market opportunity mode override',
      });
    });
    expect(await screen.findByText(/已清除 DB 覆盖值/)).toBeInTheDocument();
  });

  it('blocks paper save while observe gate is incomplete', async () => {
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    const modeSelect = await screen.findByLabelText('运行模式（热切换）') as HTMLSelectElement;
    await waitFor(() => {
      expect(modeSelect.value).toBe('observe');
    });
    fireEvent.change(modeSelect, { target: { value: 'paper' } });

    await waitFor(() => {
      expect(screen.getByText('保存模式')).toBeDisabled();
      expect(screen.getByText(/paper 仍受观察门槛保护/)).toBeInTheDocument();
    });
    expect(mockUpdateMode).not.toHaveBeenCalled();
  });

  it('shows degraded provenance and disables unsafe mode mutations', async () => {
    mockGetStatus.mockResolvedValueOnce(makeStatus({
      mode: 'off',
      enabled: false,
      modeSource: 'degraded',
      modeVersion: null,
      modeUpdatedAt: null,
      modeUpdatedBy: null,
      modeDegraded: true,
      modeProvenance: { reason: 'runtime_setting_read_failed', errorType: 'OperationalError' },
      observeGate: { requiredTradingDays: 3, completedTradingDays: 0, completed: false },
      message: 'fail closed',
    }));

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText(/运行时配置存储 degraded/)).toBeInTheDocument();
    expect(screen.getByText(/runtime_setting_read_failed/)).toBeInTheDocument();
    expect(screen.getByText('保存模式')).toBeDisabled();
    expect(screen.getByText('回退环境值')).toBeDisabled();
  });

  it('distinguishes an exposed but empty canonical date list from a missing date field', async () => {
    mockGetStatus.mockResolvedValueOnce(makeStatus({
      observeGate: {
        requiredTradingDays: 3,
        completedTradingDays: 0,
        dates: [],
        completed: false,
      },
    }));

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('0/3')).toBeInTheDocument();
    const observeDateMessage = screen.getByText(/尚无计入观察门禁的日期。/);
    expect(observeDateMessage).toBeInTheDocument();
    expect(observeDateMessage).not.toHaveTextContent('观察日期未暴露。');
    expect(screen.queryByText(/正向观察日期未暴露。/)).not.toBeInTheDocument();
  });

  it('counts a healthy zero-candidate run as observation readiness without inventing a signal', async () => {
    mockGetStatus.mockResolvedValueOnce(makeStatus({
      observeGate: {
        requiredTradingDays: 3,
        completedTradingDays: 1,
        totalRunDays: 1,
        healthyRunDays: 1,
        dates: ['2026-09-14'],
        completed: false,
      },
    }));
    mockGetRun.mockResolvedValueOnce({
      ...run,
      candidateCount: 0,
      selectedCandidateCount: 0,
      healthStatus: 'healthy_empty',
      healthReason: 'zero_candidates',
      provenance: {
        observationCandidateCount: 0,
        observationHealth: {
          healthy: true,
          reason: 'zero_candidates',
          sourceSnapshotId: 'empty-14',
        },
      },
      themes: [],
      candidates: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 0,
      page: 1,
      limit: 20,
      items: [],
      configuredModels,
    });
    mockGetEvaluation.mockResolvedValueOnce({
      windows: [1, 3, 5, 10, 20, 40],
      candidateCount: 0,
      candidateMetrics: [],
      summary: { candidateCount: 0, windows: {} },
      themeSummary: {},
      modelExecutionSummary: {},
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('1/3')).toBeInTheDocument();
    expect(screen.getByText(/总运行日 1，健康运行日 1/)).toBeInTheDocument();
    expect(await screen.findByText(/暂无候选。健康零候选会计入工程观察天数/)).toBeInTheDocument();
  });


  it('shows real board snapshot credit and partial candidate quality without treating it as buy authorization', async () => {
    const partialRejectedCandidate = {
      ...candidate,
      id: 16,
      stockName: '局部缺数股票',
      status: 'rejected',
      noPlanReason: 'candidate_rejected:insufficient_daily_bars',
      rejectionReasons: ['candidate_rejected:insufficient_daily_bars'],
      dataQuality: {
        status: 'partial',
        missingEvidence: ['insufficient_daily_bars'],
      },
      modelStates: [],
    };
    mockGetStatus.mockResolvedValueOnce(makeStatus({
      observeGate: {
        requiredTradingDays: 3,
        completedTradingDays: 1,
        totalRunDays: 1,
        healthyRunDays: 1,
        countingBasis: 'healthy_board_observation_days',
        dates: ['2026-09-15'],
        excludedDates: [],
        completed: false,
      },
    }));
    mockGetRun.mockResolvedValueOnce({
      ...run,
      tradeDate: '2026-09-15',
      healthStatus: 'risk_blocked',
      healthReason: 'risk_off',
      provenance: {
        observationCandidateCount: 30,
        observationHealth: {
          healthy: true,
          reason: null,
          sourceSnapshotId: 'board-2026-09-15',
          countingBasis: 'healthy_board_observation_days',
          discoveryQuality: {
            candidateCount: 30,
            candidateDataIncompleteCount: 2,
            themeDataIncompleteCount: 1,
            status: 'partial',
            scope: 'discovery_candidates',
          },
        },
        recordedObservationHealth: {
          healthy: false,
          reason: 'risk_off',
          sourceSnapshotId: 'old-risk-run',
        },
      },
      candidateCount: 31,
      selectedCandidateCount: 8,
      candidates: [partialRejectedCandidate],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 1,
      page: 1,
      limit: 20,
      items: [partialRejectedCandidate],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('已拒绝 · 已拒绝')).toBeInTheDocument();
    expect(screen.getByText('1/3')).toBeInTheDocument();
    expect(screen.getByText(/计数口径 健康完整板块快照累计/)).toBeInTheDocument();
    expect(screen.getByText(/总运行日 1，健康运行日 1/)).toHaveTextContent('真实健康完整板块快照按交易日累计一次');
    expect(screen.getByText(/观察日期 2026-09-15/)).toBeInTheDocument();
    expect(screen.getByText(/发现质量 部分完整 · 发现候选 30/)).toHaveTextContent('候选数据不完整 2，主题数据不完整 1');
    expect(screen.getByText(/候选数据部分不完整：候选 2 个、主题 1 个/)).toHaveTextContent('这些候选保持拒绝，不会授权买入');
    expect(screen.getByText(/观察健康 健康 ·/)).toHaveTextContent('原因 --');
    expect(screen.getByText(/观察健康 健康 ·/)).toHaveTextContent('快照 board-2026-09-15');
    expect(screen.getByText(/观察健康 健康 ·/)).toHaveTextContent('口径 健康完整板块快照累计');
    expect(screen.getByText(/原始运行健康 已排除/)).toHaveTextContent('风险市：继续观察，不允许新增模拟入场');
    expect(screen.getByText(/拒绝原因：日线样本不足/)).toBeInTheDocument();
  });

  it('keeps risk-off runs observable while making clear they are not buyable', async () => {
    const riskCandidate = {
      ...candidate,
      id: 12,
      status: 'observing',
      scoreComponents: {
        evidence_stage: 'cold_start',
        setup_type: 'pullback',
        strategy_comparison: {
          legacy_selected: false,
          legacy_score: 58,
          legacy_reasons: ['risk_off'],
          current_selected: false,
          policy_version: 'opportunity-policy-v3',
        },
      },
      dataQuality: {
        missing_evidence: ['risk budget blocked'],
      },
      modelStates: [],
    };
    mockGetRun.mockResolvedValueOnce({
      ...run,
      marketRegime: 'risk_off',
      healthStatus: 'risk_blocked',
      healthReason: 'risk_off',
      candidates: [riskCandidate],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 1,
      page: 1,
      limit: 20,
      items: [riskCandidate],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('观察池 · 观察中')).toBeInTheDocument();
    expect(screen.getByText(/风险市只允许保留观察证据，页面不把候选显示为可买。/)).toBeInTheDocument();
    expect(screen.getByText(/缺失证据：风险预算阻断/)).toBeInTheDocument();
    expect(screen.getByText('缩量回踩')).toBeInTheDocument();
  });

  it('explains observing candidates that wait for analysis budget as qualified for the basic pool', async () => {
    const budgetCandidate = {
      ...candidate,
      id: 13,
      status: 'observing',
      noPlanReason: 'candidate_observing',
      rejectionReasons: ['analysis_candidate_budget'],
      modelStates: [],
    };
    mockGetRun.mockResolvedValueOnce({
      ...run,
      healthStatus: 'observing',
      healthReason: null,
      candidates: [budgetCandidate],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 1,
      page: 1,
      limit: 20,
      items: [budgetCandidate],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('观察池 · 观察中')).toBeInTheDocument();
    expect(screen.getByText(/已达到基础条件，正在等待本批次分析名额/)).toBeInTheDocument();
    expect(screen.getByText(/拒绝原因：达到基础条件，等待本批次分析名额/)).toBeInTheDocument();
  });

  it('translates backend short reasons and candidate prefixes', async () => {
    const rejectedCandidate = {
      ...candidate,
      id: 14,
      stockName: '短原因股票',
      status: 'rejected',
      noPlanReason: 'job_not_created',
      rejectionReasons: [
        'candidate_rejected:below_analysis_threshold',
        'theme_history_warming_up',
        'insufficient_daily_bars',
        'low_20d_amount',
        'entry_setup_unconfirmed',
        'insufficient_future_bars',
        'missing_entry_price',
      ],
      dataQuality: {
        missingEvidence: [
          'historical_collection_not_supported',
          'market_regime_risk_off',
          'observation_health_unverified',
          'frozen_membership_no_observation_credit',
        ],
      },
      modelStates: [{
        model: configuredModels[0],
        status: 'missing',
        noPlanReason: 'candidate_observing:job_not_created',
      }],
    };
    mockGetRun.mockResolvedValueOnce({
      ...run,
      candidates: [rejectedCandidate],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 1,
      page: 1,
      limit: 20,
      items: [rejectedCandidate],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('短原因股票')).toBeInTheDocument();
    const restSection = screen.getByTestId('opportunity-rest');
    const scope = within(restSection);
    expect(scope.getByText('已拒绝 · 已拒绝')).toBeInTheDocument();
    expect(scope.getByText(/历史采集暂不支持/)).toBeInTheDocument();
    expect(scope.getByText(/大盘风险状态偏高，仅保留观察/)).toBeInTheDocument();
    expect(scope.getByText(/观察健康状态未验证/)).toBeInTheDocument();
    expect(scope.getByText(/历史冻结成分回放，不计入观察天数/)).toBeInTheDocument();
    expect(scope.getAllByText(/低于分析阈值/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/题材历史样本累计中/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/日线样本不足/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/20日成交额偏低/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/入场形态未确认/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/未来样本不足，等待窗口到期/).length).toBeGreaterThan(0);
    expect(scope.getAllByText(/缺少入场价格/).length).toBeGreaterThan(0);
    expect(scope.getAllByText('未创建分析任务').length).toBeGreaterThan(0);
  });

  it('keeps the legacy contract readable when new observation fields are absent', async () => {
    mockGetStatus.mockResolvedValueOnce(makeStatus({
      limits: { maxTotalCandidates: 8 },
      observeGate: {
        requiredTradingDays: 3,
        completedTradingDays: 0,
        completed: false,
      },
    }));
    mockGetRun.mockResolvedValueOnce({
      ...run,
      healthStatus: 'source_unavailable',
      healthReason: 'theme_constituents_unavailable',
      provenance: { source: 'legacy' },
      themes: [],
      candidates: [candidate],
      analysisJobs: [],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('0/3')).toBeInTheDocument();
    expect(screen.getByText(/观察日期未暴露。/)).toBeInTheDocument();
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('观察池上限 --');
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('可分析上限 8');
    expect(screen.getByText(/观察池上限/)).toHaveTextContent('主题发现 --');
    expect(await screen.findByText(/健康状态 数据源不可用/)).toBeInTheDocument();
    expect(screen.getAllByText(/板块成分数据源不可用，可在后续任务重试/).length).toBeGreaterThan(0);
  });

  it('saves the opportunity-pool screening style through system config without touching the paper main account', async () => {
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    const styleSelect = await screen.findByLabelText('筛选风格（中/短/长）') as HTMLSelectElement;
    await waitFor(() => {
      expect(styleSelect.value).toBe('');
    });
    fireEvent.change(styleSelect, { target: { value: 'short_swing' } });
    const saveStyleButton = screen.getByText('保存风格');
    await waitFor(() => {
      expect(saveStyleButton).not.toBeDisabled();
    });
    fireEvent.click(saveStyleButton);

    await waitFor(() => {
      expect(mockUpdateConfig).toHaveBeenCalledWith({
        configVersion: 'cfg-v1',
        items: [{ key: 'MARKET_OPPORTUNITY_STYLE', value: 'short_swing' }],
      });
    });
    expect(await screen.findByText(/机会池筛选风格已保存为「短线 · short_swing」/)).toBeInTheDocument();
  });

  it('shows the default next-run line and analysis exceptions when the run time is unconfigured', async () => {
    mockGetRun.mockResolvedValueOnce({
      ...run,
      status: 'blocked',
      healthStatus: 'failed',
      errorMessage: 'sync_analysis_exception:TimeoutError:llm timeout',
      candidates: [],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 0,
      page: 1,
      limit: 20,
      items: [],
      configuredModels,
    });
    mockGetEvaluation.mockResolvedValueOnce({
      windows: [1, 3, 5, 10, 20, 40],
      candidateCount: 0,
      candidateMetrics: [],
      summary: { candidateCount: 0, windows: {} },
      themeSummary: {},
      modelExecutionSummary: {},
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText(/下一次：随定时分析批次/)).toBeInTheDocument();
    expect(screen.getByText(/已阻断（分析调用异常（TimeoutError · llm timeout））/)).toBeInTheDocument();
    expect(screen.getByText(/本次没有候选送 LLM 分析/)).toBeInTheDocument();
    expect(screen.getByText('暂无待复核或待确认的候选。')).toBeInTheDocument();
  });

  it('collapses the observation pool and settings by default and summarizes translated rest reasons', async () => {
    const rejected = {
      ...candidate,
      id: 21,
      stockName: '缺数股票',
      status: 'rejected',
      noPlanReason: 'candidate_rejected:daily_data_unavailable',
      rejectionReasons: ['candidate_rejected:daily_data_unavailable', 'candidate_rejected:st_or_delisting'],
      modelStates: [],
    };
    const observing = {
      ...candidate,
      id: 22,
      stockName: '观察股票',
      status: 'observing',
      noPlanReason: 'candidate_observing',
      rejectionReasons: [],
      modelStates: [],
    };
    mockGetRun.mockResolvedValueOnce({
      ...run,
      candidates: [candidate, rejected, observing],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });
    mockGetCandidates.mockResolvedValueOnce({
      total: 3,
      page: 1,
      limit: 20,
      items: [rejected, observing],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText(/送 LLM 分析的 1 只/)).toBeInTheDocument();
    const rest = screen.getByTestId('opportunity-rest');
    expect(rest).not.toHaveAttribute('open');
    expect(within(rest).getByText(/其余 2 只（观察 1 · 淘汰 1）：日线不足 1 · ST 1/)).toBeInTheDocument();
    const coreRow = screen.getByTestId('opportunity-core-row');
    expect(within(coreRow).getByText('执行评估证据')).toBeInTheDocument();
    const settings = screen.getByLabelText('展开设置与说明').closest('details') as HTMLElement | null;
    expect(settings ?? document.getElementById('market-opportunity-settings')).not.toBeNull();
    expect(settings ?? document.getElementById('market-opportunity-settings')).not.toHaveAttribute('open');
  });

  it('lists review and confirmation obligations in the follow-up queue', async () => {
    mockGetRun.mockResolvedValueOnce({
      ...run,
      candidates: [{
        ...candidate,
        modelStates: [
          {
            ...candidate.modelStates[0],
            executionIntent: 'research_only',
            action: 'add',
            actionLabel: '加仓',
            research: { kind: 'initial', outcome: 'bullish', review: null, reviewOf: null },
          },
          {
            model: configuredModels[2],
            status: 'completed',
            paperPlanId: 790,
            planExecutable: false,
            planStatus: 'pending',
            confirmationProgress: { current: 1, required: 2, completed: false, reason: 'awaiting_entry_confirmation' },
            followup: { status: 'spawned', dueDate: '2026-09-01', spawnedRunId: 9 },
          },
        ],
      }],
      themes: [],
      analysisJobs: [],
      configuredModels,
    });

    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('待复核 / 待确认')).toBeInTheDocument();
    expect(screen.getByText('创新医疗 · deepseek-v4-pro · 09-01 确认 1/2（模拟计划 #789）')).toBeInTheDocument();
    expect(screen.getByText('创新医疗 · model-x · 09-01 确认 1/2（模拟计划 #790）')).toBeInTheDocument();
    expect(screen.getByText('已启动（运行 #8）')).toBeInTheDocument();
    expect(screen.getByText('已启动（运行 #9）')).toBeInTheDocument();
  });

  it('hides the style selector when system config is unavailable', async () => {
    mockGetConfig.mockRejectedValueOnce(new Error('config unavailable'));
    render(
      <UiLanguageProvider>
        <MarketOpportunitiesPage />
      </UiLanguageProvider>,
    );

    expect(await screen.findByText('大盘风口机会池')).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByLabelText('筛选风格（中/短/长）')).not.toBeInTheDocument();
    });
  });
});

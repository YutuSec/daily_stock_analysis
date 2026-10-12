/**
 * Bilingual labels for paper-trading raw codes.
 *
 * The backend stores machine-readable codes (thesis reasons, validator reasons,
 * lock states, event types, ...). The page is the only consumer that must show
 * them to a human, so the mapping lives here instead of in the backend payload.
 * Unknown codes are never dropped and never throw: they fall back to the raw
 * code with a small label so an operator still sees that something is missing.
 */

export type LabelLocale = 'zh' | 'en';

type LabelTable = { zh: string; en: string };

const pick = (table: LabelTable, locale: LabelLocale): string => (locale === 'zh' ? table.zh : table.en);

const sourceKindLabels: Record<string, LabelTable> = {
  market_opportunity: { zh: '机会池', en: 'Opportunity pool' },
  analysis: { zh: '自选/普通分析', en: 'Watchlist / regular analysis' },
  legacy: { zh: '历史基线', en: 'Legacy baseline' },
};

const styleLabels: Record<string, LabelTable> = {
  short_swing: { zh: '短线', en: 'Short swing' },
  position: { zh: '中线', en: 'Position' },
  trend: { zh: '长线', en: 'Trend' },
};

const actionLabels: Record<string, LabelTable> = {
  buy: { zh: '买入', en: 'Buy' },
  add: { zh: '加仓', en: 'Add' },
  hold: { zh: '持有', en: 'Hold' },
  watch: { zh: '观望', en: 'Watch' },
  avoid: { zh: '回避', en: 'Avoid' },
  sell: { zh: '卖出', en: 'Sell' },
  reduce: { zh: '减仓', en: 'Reduce' },
  stop_loss: { zh: '止损', en: 'Stop loss' },
  take_profit: { zh: '止盈', en: 'Take profit' },
  wait: { zh: '等待', en: 'Wait' },
  unknown: { zh: '未知', en: 'Unknown' },
};

const planStatusLabels: Record<string, LabelTable> = {
  active: { zh: '有效', en: 'Active' },
  non_executable: { zh: '不执行', en: 'Not executable' },
  superseded: { zh: '已被替代', en: 'Superseded' },
  expired: { zh: '已过期', en: 'Expired' },
  triggered: { zh: '已触发', en: 'Triggered' },
  closed: { zh: '已结束', en: 'Closed' },
  blocked: { zh: '阻塞', en: 'Blocked' },
};

const regimeLabels: Record<string, LabelTable> = {
  normal: { zh: '正常', en: 'Normal' },
  cautious: { zh: '谨慎', en: 'Cautious' },
  risk_off: { zh: '风险偏高', en: 'Risk off' },
  unavailable: { zh: '不可用', en: 'Unavailable' },
};

const marketReasonLabels: Record<string, LabelTable> = {
  market_context_stale: { zh: '大盘判断已过期', en: 'Market judgement is stale' },
  market_context_calendar_unavailable: { zh: '交易日历不可用', en: 'Trading calendar unavailable' },
  market_context_risk_off: { zh: '大盘风险偏高', en: 'Market risk off' },
  market_context_unavailable: { zh: '大盘判断不可用', en: 'Market judgement unavailable' },
  market_context_cautious: { zh: '大盘判断谨慎', en: 'Market judgement cautious' },
};

const lockStateLabels: Record<string, LabelTable> = {
  normal: { zh: '正常', en: 'Normal' },
  warning: { zh: '预警', en: 'Warning' },
  reduce_only: { zh: '只减仓', en: 'Reduce only' },
  cooldown: { zh: '冷却中', en: 'Cooling down' },
  manual_hold: { zh: '人工暂停', en: 'Manual hold' },
  halted: { zh: '熔断', en: 'Halted' },
  entry_suspended: { zh: '暂停开仓', en: 'Entry suspended' },
};

const lockReasonLabels: Record<string, LabelTable> = {
  nonpositive_equity: { zh: '账户权益不为正', en: 'Account equity is not positive' },
  incomplete_valuation: { zh: '估值不完整（存在缺价持仓）', en: 'Valuation incomplete (unpriced holdings)' },
  valuation_incomplete: { zh: '估值不完整（存在缺价持仓）', en: 'Valuation incomplete (unpriced holdings)' },
  stale_valuation: { zh: '估值行情已过期', en: 'Valuation mark is stale' },
  daily_loss_limit_breached: { zh: '触发单日亏损上限', en: 'Daily loss limit breached' },
  daily_loss_breached: { zh: '触发单日亏损上限', en: 'Daily loss limit breached' },
  drawdown_limit_breached: { zh: '触发回撤上限', en: 'Drawdown limit breached' },
  losing_streak_cooldown: { zh: '连续亏损进入冷却', en: 'Losing-streak cooldown' },
  daily_trade_limit_reached: { zh: '达到单日成交笔数上限', en: 'Daily trade count limit reached' },
  daily_open_add_limit_reached: { zh: '达到单日开仓/加仓上限', en: 'Daily open/add limit reached' },
  sector_concentration_cap_exceeded: { zh: '行业集中度超过上限', en: 'Sector concentration cap exceeded' },
  correlation_concentration_cap_exceeded: { zh: '相关性集中度超过上限', en: 'Correlation concentration cap exceeded' },
  unknown_concentration_cap_exceeded: { zh: '未知分类集中度超过上限', en: 'Unknown-bucket concentration cap exceeded' },
  market_context_risk_off: { zh: '大盘风险偏高，暂停新开仓', en: 'Market risk off; new entry paused' },
  market_context_capacity_cap: { zh: '大盘风险容量上限', en: 'Market-context capacity cap' },
  manual_hold: { zh: '人工暂停', en: 'Manual hold' },
};

const eventTypeLabels: Record<string, LabelTable> = {
  manual_hold: { zh: '人工暂停', en: 'Manual hold' },
  manual_resume: { zh: '人工恢复', en: 'Manual resume' },
  entry_blocked: { zh: '新开仓被拦截', en: 'Entry blocked' },
  entry_suspended: { zh: '暂停开仓', en: 'Entry suspended' },
  exit_blocked: { zh: '卖出被拦截', en: 'Exit blocked' },
  state_change: { zh: '风险状态变更', en: 'Risk state change' },
};

const thesisReasonLabels: Record<string, LabelTable> = {
  accepted_entry_thesis: { zh: '入场论点已确认', en: 'Entry thesis accepted' },
  accepted_non_bullish_freezes_unfilled_entry: { zh: '结论不是看多，未成交的入场已冻结', en: 'Conclusion is not bullish; unfilled entry is frozen' },
  awaiting_entry_confirmation: { zh: '等待连续交易日确认', en: 'Awaiting consecutive-session confirmation' },
  entry_evidence_invalid: { zh: '买入结论缺少有效入场证据', en: 'Buy conclusion lacks valid entry evidence' },
  accepted_holding_risk_refresh: { zh: '持仓风控已按新报告刷新', en: 'Holding risk refreshed from the new report' },
  accepted_verified_intraday_reanalysis: { zh: '盘中重分析已核实', en: 'Intraday reanalysis verified' },
  accepted_confirmed_defensive_flip: { zh: '防守转向已确认', en: 'Defensive flip confirmed' },
  awaiting_defensive_confirmation: { zh: '等待防守转向确认', en: 'Awaiting defensive confirmation' },
  strategy_report_outside_confirmation_window: { zh: '报告不在确认窗口内', en: 'Report is outside the confirmation window' },
  confirmation_session_unavailable_or_non_trading: { zh: '非交易日或交易日历不可用，不计入确认', en: 'Non-trading session or calendar unavailable; not counted' },
  stale_report_older_than_strategy_observation: { zh: '报告早于已记录观察，已被替代', en: 'Report predates a recorded observation; superseded' },
  strategy_thesis_disabled: { zh: '策略论点确认未开启', en: 'Strategy-thesis confirmation is disabled' },
  missing_strategy_identity: { zh: '缺少策略身份', en: 'Missing strategy identity' },
  evidence_recorded: { zh: '已记录证据', en: 'Evidence recorded' },
};

const planReasonLabels: Record<string, LabelTable> = {
  invalid_report_payload: { zh: '报告载荷无效', en: 'Invalid report payload' },
  unsupported_or_non_bullish_action: { zh: '结论不是买入', en: 'Conclusion is not a buy' },
  unsupported_market: { zh: '该市场不支持模拟成交', en: 'Market not supported for paper fills' },
  ambiguous_or_invalid_stock_identity: { zh: '股票身份不明确或无效', en: 'Ambiguous or invalid stock identity' },
  missing_source_model: { zh: '缺少来源模型', en: 'Missing source model' },
  no_supported_entry_rule: { zh: '没有可执行的入场规则', en: 'No supported entry rule' },
  no_supported_price_entry_rule: { zh: '没有可执行的价格入场规则', en: 'No supported price entry rule' },
  missing_valid_stop_loss: { zh: '缺少有效止损', en: 'Missing valid stop loss' },
  missing_valid_target_price: { zh: '缺少有效目标价', en: 'Missing valid target price' },
  unbounded_explicit_entry_geometry: { zh: '入场价格区间没有边界', en: 'Entry geometry is unbounded' },
  stop_not_below_entry: { zh: '止损不在入场价下方', en: 'Stop loss is not below entry' },
  target_not_above_entry: { zh: '目标价不在入场价上方', en: 'Target is not above entry' },
  invalid_exit_leg_entry_reference: { zh: '卖出腿引用了无效的入场腿', en: 'Exit leg references an invalid entry leg' },
  exit_leg_target_not_above_entry: { zh: '卖出腿目标不在入场价上方', en: 'Exit-leg target is not above entry' },
  take_profit_final_target_mismatch: { zh: '止盈腿与最终目标价不一致', en: 'Take-profit leg does not match the final target' },
  invalid_entry_risk_reward: { zh: '入场盈亏比无效', en: 'Invalid entry risk/reward' },
  risk_reward_below_policy: { zh: '盈亏比低于下限', en: 'Risk/reward below policy floor' },
  requires_same_model_reanalysis: { zh: '需要同模型重分析', en: 'Requires same-model reanalysis' },
  incompatible_report_horizon_for_trading_style: { zh: '报告周期与交易风格不匹配', en: 'Report horizon incompatible with trading style' },
  strategy_thesis_not_accepted: { zh: '策略论点尚未确认', en: 'Strategy thesis not accepted yet' },
  fail_closed: { zh: '契约缺失，按保守处理', en: 'Missing contract; failed closed' },
};

const intentLabels: Record<string, LabelTable> = {
  model_reduce: { zh: '模型减仓', en: 'Model reduce' },
  model_buy: { zh: '模型买入', en: 'Model buy' },
  model_sell: { zh: '模型卖出', en: 'Model sell' },
  hold: { zh: '持有', en: 'Hold' },
  stop_loss: { zh: '止损', en: 'Stop loss' },
  take_profit: { zh: '止盈', en: 'Take profit' },
  t1_pending_exit: { zh: 'T+1 待卖出', en: 'T+1 pending exit' },
  suppressed: { zh: '已压制', en: 'Suppressed' },
};

const reconciliationLabels: Record<string, LabelTable> = {
  completed: { zh: '已完成', en: 'Completed' },
  retry_wait: { zh: '等待重试', en: 'Retry wait' },
  terminal: { zh: '终止', en: 'Terminal' },
  stock_report_plan_reconciled: { zh: '个股报告已生成计划', en: 'Stock report reconciled to a plan' },
  missing_decision_signal: { zh: '缺少决策信号', en: 'Missing decision signal' },
  market_review_not_stock_plan: { zh: '大盘复盘不生成个股计划', en: 'Market review does not create stock plans' },
  transient_error: { zh: '临时错误', en: 'Transient error' },
};

const resolutionStatusLabels: Record<string, LabelTable> = {
  candidate: { zh: '候选', en: 'Candidate' },
  selected: { zh: '已选定', en: 'Selected' },
  suppressed: { zh: '已压制', en: 'Suppressed' },
  triggered: { zh: '已触发', en: 'Triggered' },
  closed: { zh: '已结束', en: 'Closed' },
  failed_closed: { zh: '契约缺失，按保守处理', en: 'Failed closed' },
};

const closeReasonLabels: Record<string, LabelTable> = {
  stop_loss: { zh: '止损', en: 'Stop loss' },
  take_profit: { zh: '止盈', en: 'Take profit' },
  target_reached: { zh: '到达目标价', en: 'Target reached' },
  expiry: { zh: '到期退出（持有上限）', en: 'Expired (holding cap)' },
  time_exit: { zh: '到期退出（持有上限）', en: 'Expired (holding cap)' },
  model_sell: { zh: '模型卖出', en: 'Model sell' },
  model_reduce: { zh: '模型减仓', en: 'Model reduce' },
  risk_revision: { zh: '风险修订', en: 'Risk revision' },
  model_risk_stop_breached: { zh: '触及修订后止损', en: 'Revised stop breached' },
  model_risk_target_reached: { zh: '触及修订后止盈', en: 'Revised target reached' },
};

const syncStatusLabels: Record<string, LabelTable> = {
  paper_only: { zh: '仅模拟（未同步）', en: 'Paper only (not synced)' },
  synced: { zh: '已同步至持仓', en: 'Synced to portfolio' },
  sync_pending: { zh: '等待同步', en: 'Sync pending' },
  sync_failed: { zh: '同步失败', en: 'Sync failed' },
};

const lookups: Record<string, Record<string, LabelTable>> = {
  source: sourceKindLabels,
  style: styleLabels,
  action: actionLabels,
  planStatus: planStatusLabels,
  regime: regimeLabels,
  marketReason: marketReasonLabels,
  lockState: lockStateLabels,
  lockReason: lockReasonLabels,
  eventType: eventTypeLabels,
  thesisReason: thesisReasonLabels,
  planReason: planReasonLabels,
  intent: intentLabels,
  reconciliation: reconciliationLabels,
  resolutionStatus: resolutionStatusLabels,
  syncStatus: syncStatusLabels,
  closeReason: closeReasonLabels,
};

export type LabelGroup = keyof typeof lookups;

/** Translate a backend code; unknown codes keep their raw text instead of hiding. */
export const label = (group: LabelGroup, code: string | null | undefined, locale: LabelLocale): string => {
  if (code == null || code === '') return locale === 'zh' ? '未记录' : 'Not recorded';
  const table = lookups[group];
  const entry = table[code] ?? table[code.toLowerCase()];
  if (entry) return pick(entry, locale);
  return code;
};

export const formatNumber = (value?: number | null): string => (
  value == null || Number.isNaN(value) ? '—' : value.toFixed(2)
);

export const formatPercent = (value?: number | null): string => (
  value == null || Number.isNaN(value) ? '—' : `${value.toFixed(2)}%`
);

export const formatMoney = (value?: number | null, currency?: string | null): string => {
  if (value == null || Number.isNaN(value)) return '—';
  const formatted = value.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return currency ? `${currency} ${formatted}` : formatted;
};

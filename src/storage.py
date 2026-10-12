# -*- coding: utf-8 -*-
"""
===================================
A股自选股智能分析系统 - 存储层
===================================

职责：
1. 管理 SQLite 数据库连接（单例模式）
2. 定义 ORM 数据模型
3. 提供数据存取接口
4. 实现智能更新逻辑（断点续传）
"""

import atexit
from contextlib import contextmanager
import hashlib
import json
import logging
import re
import threading
import time
from datetime import datetime, date, timedelta, timezone, time as dt_time
from typing import Optional, List, Dict, Any, TYPE_CHECKING, Tuple, Callable, TypeVar, Union
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import (
    create_engine,
    Column,
    String,
    Float,
    Boolean,
    Date,
    DateTime,
    Integer,
    ForeignKey,
    Index,
    UniqueConstraint,
    Text,
    text,
    select,
    and_,
    or_,
    delete,
    update,
    desc,
    event,
    exists,
    func,
    inspect,
    literal,
    MetaData,
    Table,
)
from sqlalchemy.dialects.postgresql import insert as postgresql_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import (
    declarative_base,
    sessionmaker,
    Session,
)
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.schema import CreateTable

from src.agent.provider_trace import PROVIDER_TRACE_RETENTION_LIMIT
from src.config import get_config
from src.schemas.decision_profile import extract_legacy_decision_profile
from src.utils.sniper_points import extract_sniper_points, parse_sniper_value
from src.schemas.decision_action import apply_position_aware_result_contract
from src.utils.sanitize import sanitize_diagnostic_text

logger = logging.getLogger(__name__)
T = TypeVar("T")
CURRENT_SCHEMA_VERSION = "2026-06-05-create-all-baseline"
INTELLIGENCE_ITEM_NULL_SCOPE_VALUE = "__dsa_null_scope__"
POSTGRES_SCHEMA_ADVISORY_LOCK_ID = 724019302

# SQLAlchemy ORM 基类
Base = declarative_base()

if TYPE_CHECKING:
    from src.search_service import SearchResponse


def _postgres_identifier(name: str) -> str:
    """Return a deterministic PostgreSQL-safe identifier (maximum 63 bytes)."""
    encoded = name.encode("utf-8")
    if len(encoded) <= 63:
        return name
    suffix = hashlib.sha256(encoded).hexdigest()[:8]
    prefix_budget = 63 - len(suffix) - 1
    prefix = encoded[:prefix_budget]
    while True:
        try:
            return f"{prefix.decode('utf-8')}_{suffix}"
        except UnicodeDecodeError:
            prefix = prefix[:-1]


def create_application_schema(bind: Any) -> None:
    """Create the ORM schema while respecting PostgreSQL identifier limits.

    Some long, legacy SQLite index names exceed PostgreSQL's 63-byte limit.
    A metadata copy lets PostgreSQL use deterministic shortened physical names
    without changing SQLite index names or ORM query behavior.
    """
    if bind.dialect.name != "postgresql":
        Base.metadata.create_all(bind=bind)
        return

    postgres_metadata = MetaData()
    for table in Base.metadata.sorted_tables:
        table.to_metadata(postgres_metadata)
    for table in postgres_metadata.tables.values():
        for index in table.indexes:
            if index.name:
                index.name = _postgres_identifier(index.name)
        for constraint in table.constraints:
            if constraint.name:
                constraint.name = _postgres_identifier(constraint.name)
    postgres_metadata.create_all(bind=bind)


def utc_naive_now() -> datetime:
    """Return current UTC time without tzinfo for SQLite DateTime columns."""
    return datetime.now(timezone.utc).replace(tzinfo=None)


def to_utc_naive_datetime(value: datetime) -> datetime:
    """Normalize aware datetimes to UTC-naive; treat naive values as UTC-naive."""
    if value.tzinfo is not None and value.utcoffset() is not None:
        return value.astimezone(timezone.utc).replace(tzinfo=None)
    return value


# === 数据模型定义 ===

class DatabaseSchemaMigration(Base):
    """Applied database schema version marker."""

    __tablename__ = 'schema_migrations'

    version = Column(String(64), primary_key=True)
    description = Column(String(255), nullable=False)
    applied_at = Column(DateTime, default=datetime.now, nullable=False, index=True)


class RuntimeSetting(Base):
    """Shared runtime setting override consumed by all running processes."""

    __tablename__ = 'runtime_settings'

    key = Column(String(128), primary_key=True)
    value = Column(String(255), nullable=False)
    version = Column(Integer, nullable=False, default=1)
    provenance_json = Column(Text, nullable=False, default='{}')
    updated_by = Column(String(128))
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)


class RuntimeSettingAudit(Base):
    """Append-only audit trail for shared runtime setting changes."""

    __tablename__ = 'runtime_setting_audit'

    id = Column(Integer, primary_key=True, autoincrement=True)
    key = Column(String(128), nullable=False, index=True)
    old_value = Column(String(255))
    new_value = Column(String(255))
    old_version = Column(Integer)
    new_version = Column(Integer)
    action = Column(String(32), nullable=False, default='update', index=True)
    actor = Column(String(128))
    reason = Column(String(255))
    provenance_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_runtime_setting_audit_key_time', 'key', 'created_at'),
        UniqueConstraint('key', 'new_version', name='uix_runtime_setting_audit_key_new_version'),
    )


class StockDaily(Base):
    """
    股票日线数据模型

    存储每日行情数据和计算的技术指标
    支持多股票、多日期的唯一约束
    """
    __tablename__ = 'stock_daily'

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 股票代码（如 600519, 000001）
    code = Column(String(10), nullable=False, index=True)

    # 交易日期
    date = Column(Date, nullable=False, index=True)
    
    # OHLC 数据
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    
    # 成交数据
    volume = Column(Float)  # 成交量（股）
    amount = Column(Float)  # 成交额（元）
    pct_chg = Column(Float)  # 涨跌幅（%）
    
    # 技术指标
    ma5 = Column(Float)
    ma10 = Column(Float)
    ma20 = Column(Float)
    volume_ratio = Column(Float)  # 量比
    
    # 数据来源
    data_source = Column(String(50))  # 记录数据来源（如 AkshareFetcher）
    
    # 更新时间
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    
    # 唯一约束：同一股票同一日期只能有一条数据
    __table_args__ = (
        UniqueConstraint('code', 'date', name='uix_code_date'),
        Index('ix_code_date', 'code', 'date'),
    )
    
    def __repr__(self):
        return f"<StockDaily(code={self.code}, date={self.date}, close={self.close})>"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        from data_provider.volume_units import cached_volume_shares, cached_volume_unit_known
        return {
            'code': self.code,
            'date': self.date,
            'open': self.open,
            'high': self.high,
            'low': self.low,
            'close': self.close,
            'volume': cached_volume_shares(self.volume, self.data_source),
            'volume_unit': 'shares' if cached_volume_unit_known(self.data_source) else 'unknown',
            'amount': self.amount,
            'pct_chg': self.pct_chg,
            'ma5': self.ma5,
            'ma10': self.ma10,
            'ma20': self.ma20,
            'volume_ratio': self.volume_ratio,
            'data_source': self.data_source,
        }


class NewsIntel(Base):
    """
    新闻情报数据模型

    存储搜索到的新闻情报条目，用于后续分析与查询
    """
    __tablename__ = 'news_intel'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联用户查询操作
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))

    # 搜索上下文
    dimension = Column(String(32), index=True)  # latest_news / risk_check / earnings / market_analysis / industry
    query = Column(String(255))
    provider = Column(String(32), index=True)

    # 新闻内容
    title = Column(String(300), nullable=False)
    snippet = Column(Text)
    url = Column(String(1000), nullable=False)
    source = Column(String(100))
    published_date = Column(DateTime, index=True)

    # 入库时间
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    query_source = Column(String(32), index=True)  # bot/web/cli/system
    requester_platform = Column(String(20))
    requester_user_id = Column(String(64))
    requester_user_name = Column(String(64))
    requester_chat_id = Column(String(64))
    requester_message_id = Column(String(64))
    requester_query = Column(String(255))

    __table_args__ = (
        UniqueConstraint('url', name='uix_news_url'),
        Index('ix_news_code_pub', 'code', 'published_date'),
    )

    def __repr__(self) -> str:
        return f"<NewsIntel(code={self.code}, title={self.title[:20]}...)>"


class IntelligenceSource(Base):
    """可配置资讯源。"""

    __tablename__ = 'intelligence_sources'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(100), nullable=False, unique=True, index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    url = Column(String(1000), nullable=False)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    description = Column(Text)
    last_status = Column(String(32))
    last_error = Column(Text)
    last_fetched_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_intel_source_scope', 'scope_type', 'scope_value', 'market'),
    )


class IntelligenceItem(Base):
    """沉淀后的资讯 / 情报条目。"""

    __tablename__ = 'intelligence_items'

    id = Column(Integer, primary_key=True, autoincrement=True)
    source_id = Column(Integer, ForeignKey('intelligence_sources.id', ondelete='SET NULL'), nullable=True, index=True)
    source_name = Column(String(100), index=True)
    source_type = Column(String(32), nullable=False, default='rss', index=True)
    title = Column(String(300), nullable=False)
    summary = Column(Text)
    url = Column(String(1000), nullable=False, index=True)
    source = Column(String(100))
    published_at = Column(DateTime, index=True)
    fetched_at = Column(DateTime, default=datetime.now, index=True)
    scope_type = Column(String(32), nullable=False, default='market', index=True)
    scope_value = Column(String(64), nullable=False, default=INTELLIGENCE_ITEM_NULL_SCOPE_VALUE, index=True)
    market = Column(String(32), nullable=False, default='cn', index=True)
    raw_payload = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'source_id',
            'url',
            'scope_type',
            'scope_value',
            'market',
            name='uix_intel_item_source_scope_url',
        ),
        Index('ix_intel_item_scope_time', 'scope_type', 'scope_value', 'market', 'published_at'),
        Index('ix_intel_item_fetch_time', 'fetched_at'),
    )


class FundamentalSnapshot(Base):
    """
    基本面上下文快照（P0 write-only）。

    仅用于写入，主链路不依赖读取该表，便于后续回测/画像扩展。
    """
    __tablename__ = 'fundamental_snapshot'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(64), nullable=False, index=True)
    code = Column(String(10), nullable=False, index=True)
    payload = Column(Text, nullable=False)
    source_chain = Column(Text)
    coverage = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_fundamental_snapshot_query_code', 'query_id', 'code'),
        Index('ix_fundamental_snapshot_created', 'created_at'),
    )

    def __repr__(self) -> str:
        return f"<FundamentalSnapshot(query_id={self.query_id}, code={self.code})>"


class AnalysisHistory(Base):
    """
    分析结果历史记录模型

    保存每次分析结果，支持按 query_id/股票代码检索
    """
    __tablename__ = 'analysis_history'

    id = Column(Integer, primary_key=True, autoincrement=True)

    # 关联查询链路
    query_id = Column(String(64), index=True)

    # 股票信息
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)
    # 实际完成本次分析的模型。独立列用于回测按模型精确筛选；
    # raw_result.model_used 继续保留，兼容历史详情与导出。
    model_used = Column(String(255), index=True)

    # 核心结论
    sentiment_score = Column(Integer)
    operation_advice = Column(String(20))
    # Historical LLM results may contain an explanatory trend sentence rather
    # than a short enum. SQLite never enforced VARCHAR(50); Text preserves those
    # rows during PostgreSQL migration without truncating user reports.
    trend_prediction = Column(Text)
    analysis_summary = Column(Text)

    # 详细数据
    raw_result = Column(Text)
    news_content = Column(Text)
    context_snapshot = Column(Text)

    # 狙击点位（用于回测）
    ideal_buy = Column(Float)
    secondary_buy = Column(Float)
    stop_loss = Column(Float)
    take_profit = Column(Float)

    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_code_time', 'code', 'created_at'),
    )

    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            'id': self.id,
            'query_id': self.query_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'model_used': self.model_used,
            'sentiment_score': self.sentiment_score,
            'operation_advice': self.operation_advice,
            'trend_prediction': self.trend_prediction,
            'analysis_summary': self.analysis_summary,
            'raw_result': self.raw_result,
            'news_content': self.news_content,
            'context_snapshot': self.context_snapshot,
            'ideal_buy': self.ideal_buy,
            'secondary_buy': self.secondary_buy,
            'stop_loss': self.stop_loss,
            'take_profit': self.take_profit,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


REPORT_EXECUTION_INTENT_PAPER_CANDIDATE = "paper_candidate"
REPORT_EXECUTION_INTENT_RESEARCH_ONLY = "research_only"
REPORT_EXECUTION_INTENTS = frozenset({
    REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    REPORT_EXECUTION_INTENT_RESEARCH_ONLY,
})
REPORT_EXECUTION_SCOPE_PRIMARY = "primary"
REPORT_EXECUTION_POLICY_VERSION = "report-execution-intent-v1"
REPORT_EXECUTION_INTENT_BASELINE_VERSION = "2026-09-28-report-execution-intent-baseline"
_REPORT_EXECUTION_EXPERIMENT_SCOPE = re.compile(r"^experiment:[a-z0-9][a-z0-9_-]{0,63}:[abc]$")


class AnalysisReportExecutionIntent(Base):
    """Trusted execution purpose written in the same transaction as a report.

    Rows are immutable. Model output, API payloads and later provenance never
    create or change them; paper consumers authorize only from this row.
    """

    __tablename__ = 'analysis_report_execution_intents'

    id = Column(Integer, primary_key=True, autoincrement=True)
    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        unique=True,
    )
    execution_intent = Column(String(32), nullable=False, index=True)
    execution_scope = Column(String(96), nullable=False, index=True)
    decision_as_of = Column(Date)
    execution_policy_version = Column(String(64), nullable=False)
    intent_source = Column(String(64), nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False)


@event.listens_for(AnalysisReportExecutionIntent, "before_update")
def _reject_report_execution_intent_update(_mapper, _connection, _target) -> None:
    raise ValueError("report execution intent is immutable after the first report save")


def normalize_report_execution_scope(value: Any) -> str:
    scope = str(value or "").strip()
    if scope == REPORT_EXECUTION_SCOPE_PRIMARY or _REPORT_EXECUTION_EXPERIMENT_SCOPE.fullmatch(scope):
        return scope
    raise ValueError(f"invalid report execution scope: {scope!r}")


def normalize_report_execution_intent(value: Any) -> Dict[str, Any]:
    """Validate backend-owned execution metadata before the first report save."""

    if not isinstance(value, dict):
        raise ValueError("report execution intent must be a mapping")
    intent = str(value.get("execution_intent") or "").strip()
    if intent not in REPORT_EXECUTION_INTENTS:
        raise ValueError(f"invalid report execution intent: {intent!r}")
    source = str(value.get("intent_source") or "").strip()
    if not source or len(source) > 64:
        raise ValueError("report execution intent requires intent_source of at most 64 characters")
    as_of = value.get("decision_as_of")
    if isinstance(as_of, datetime):
        as_of = as_of.date()
    elif isinstance(as_of, str):
        as_of = date.fromisoformat(as_of.strip()) if as_of.strip() else None
    elif as_of is not None and not isinstance(as_of, date):
        raise ValueError("decision_as_of must be a date")
    policy_version = str(value.get("execution_policy_version") or REPORT_EXECUTION_POLICY_VERSION).strip()
    if len(policy_version) > 64:
        raise ValueError("execution_policy_version must be at most 64 characters")
    return {
        "execution_intent": intent,
        "execution_scope": normalize_report_execution_scope(
            value.get("execution_scope") or REPORT_EXECUTION_SCOPE_PRIMARY
        ),
        "decision_as_of": as_of,
        "execution_policy_version": policy_version,
        "intent_source": source,
    }


def _report_execution_intent_field(intent: Any, name: str) -> Optional[str]:
    raw = intent.get(name) if isinstance(intent, dict) else getattr(intent, name, None)
    return str(raw).strip() if raw is not None else None


def evaluate_report_execution_intent(intent: Any, *, execution_scope: str) -> Dict[str, Any]:
    """The single authorization rule for report-derived paper execution.

    A missing row is never authorized: legacy reports receive an explicit
    baseline row once, and every later report must declare its purpose at
    first save.
    """

    scope = normalize_report_execution_scope(execution_scope)
    if intent is None:
        reason = "execution_intent_missing"
    else:
        value = _report_execution_intent_field(intent, "execution_intent")
        if value == REPORT_EXECUTION_INTENT_RESEARCH_ONLY:
            reason = "research_only"
        elif value != REPORT_EXECUTION_INTENT_PAPER_CANDIDATE:
            reason = "execution_intent_unknown"
        elif _report_execution_intent_field(intent, "execution_scope") != scope:
            reason = "execution_scope_mismatch"
        else:
            reason = None
    return {
        "authorized": reason is None,
        "reason": reason,
        "execution_scope": scope,
        "execution_intent": _report_execution_intent_field(intent, "execution_intent") if intent is not None else None,
        "report_execution_scope": _report_execution_intent_field(intent, "execution_scope") if intent is not None else None,
        "execution_policy_version": (
            _report_execution_intent_field(intent, "execution_policy_version") if intent is not None else None
        ),
    }


def report_execution_intent_clause(
    report_id_column: Any,
    *,
    execution_intent: str,
    execution_scope: str,
) -> Any:
    """SQL filter for reports whose frozen purpose exactly matches."""

    if execution_intent not in REPORT_EXECUTION_INTENTS:
        raise ValueError(f"invalid report execution intent: {execution_intent!r}")
    return exists().where(
        AnalysisReportExecutionIntent.analysis_history_id == report_id_column,
        AnalysisReportExecutionIntent.execution_intent == execution_intent,
        AnalysisReportExecutionIntent.execution_scope == normalize_report_execution_scope(execution_scope),
    )


def authorized_report_execution_clause(report_id_column: Any, *, execution_scope: str) -> Any:
    """SQL form of ``evaluate_report_execution_intent`` for list consumers."""

    return report_execution_intent_clause(
        report_id_column,
        execution_intent=REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
        execution_scope=execution_scope,
    )


def report_execution_intent_allows_signals(intent: Any) -> bool:
    """Research-only reports never enter the shared DecisionSignal layer.

    Rows without intent keep legacy signal compatibility only; paper
    execution still rejects them through ``evaluate_report_execution_intent``.
    """

    if intent is None:
        return True
    return _report_execution_intent_field(intent, "execution_intent") == REPORT_EXECUTION_INTENT_PAPER_CANDIDATE


class AnalysisAttempt(Base):
    """One attempted stock analysis, including failures that do not create a report."""

    __tablename__ = 'analysis_attempts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    query_id = Column(String(64), index=True)
    code = Column(String(10), nullable=False, index=True)
    name = Column(String(50))
    report_type = Column(String(16), index=True)
    model_used = Column(String(255), index=True)
    status = Column(String(16), nullable=False, default="started", index=True)
    error_message = Column(Text)
    analysis_history_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_analysis_attempt_code_model_time', 'code', 'model_used', 'created_at'),
    )

    def to_dict(self) -> Dict[str, Any]:
        return {
            'id': self.id,
            'query_id': self.query_id,
            'code': self.code,
            'name': self.name,
            'report_type': self.report_type,
            'model_used': self.model_used,
            'status': self.status,
            'error_message': self.error_message,
            'analysis_history_id': self.analysis_history_id,
            'created_at': self.created_at.isoformat() if self.created_at else None,
        }


class BacktestResult(Base):
    """单条分析记录的回测结果。"""

    __tablename__ = 'backtest_results'

    id = Column(Integer, primary_key=True, autoincrement=True)

    analysis_history_id = Column(
        Integer,
        ForeignKey('analysis_history.id'),
        nullable=False,
        index=True,
    )

    # 冗余字段，便于按股票筛选
    code = Column(String(10), nullable=False, index=True)
    analysis_date = Column(Date, index=True)

    # 回测参数
    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')

    # 状态
    eval_status = Column(String(32), nullable=False, default='pending')
    evaluated_at = Column(DateTime, default=datetime.now, index=True)

    # 建议快照（避免未来分析字段变化导致回测不可解释）
    operation_advice = Column(String(20))
    position_recommendation = Column(String(8))  # long/cash

    # 价格与收益
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    # 方向与结果
    direction_expected = Column(String(16))  # up/down/flat/not_down
    direction_correct = Column(Boolean, nullable=True)
    outcome = Column(String(16))  # win/loss/neutral

    # 目标价命中（仅 long 且配置了止盈/止损时有意义）
    stop_loss = Column(Float)
    take_profit = Column(Float)
    hit_stop_loss = Column(Boolean)
    hit_take_profit = Column(Boolean)
    first_hit = Column(String(16))  # take_profit/stop_loss/ambiguous/neither/not_applicable
    first_hit_date = Column(Date)
    first_hit_trading_days = Column(Integer)

    # 模拟执行（long-only）
    simulated_entry_price = Column(Float)
    simulated_exit_price = Column(Float)
    simulated_exit_reason = Column(String(24))  # stop_loss/take_profit/window_end/cash/ambiguous_stop_loss
    simulated_return_pct = Column(Float)

    # Frozen holding-decision evaluation. These fields are derived only from
    # the analysis payload saved at report generation time; backtesting never
    # reads the user's current portfolio.
    holding_state = Column(String(16))  # holding/empty/unknown
    holding_action = Column(String(16))  # buy/add/hold/reduce/sell/avoid
    holding_action_applicable = Column(Boolean, nullable=False, default=False)
    holding_action_evaluable = Column(Boolean, nullable=False, default=False)
    holding_eval_status = Column(String(24), nullable=False, default='unavailable')
    holding_direction_expected = Column(String(16))  # not_down/down
    holding_decision_correct = Column(Boolean, nullable=True)
    holding_outcome = Column(String(16))  # win/loss/neutral
    holding_decision_return_pct = Column(Float)

    __table_args__ = (
        UniqueConstraint(
            'analysis_history_id',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_analysis_window_version',
        ),
        Index('ix_backtest_code_date', 'code', 'analysis_date'),
    )


class BacktestSummary(Base):
    """回测汇总指标（按股票或全局）。"""

    __tablename__ = 'backtest_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)

    scope = Column(String(16), nullable=False, index=True)  # overall/stock
    code = Column(String(16), index=True)

    eval_window_days = Column(Integer, nullable=False, default=10)
    engine_version = Column(String(16), nullable=False, default='v1')
    computed_at = Column(DateTime, default=datetime.now, index=True)

    # 计数
    total_evaluations = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    insufficient_count = Column(Integer, default=0)
    long_count = Column(Integer, default=0)
    cash_count = Column(Integer, default=0)

    win_count = Column(Integer, default=0)
    loss_count = Column(Integer, default=0)
    neutral_count = Column(Integer, default=0)

    # 准确率/胜率
    direction_accuracy_pct = Column(Float)
    win_rate_pct = Column(Float)
    neutral_rate_pct = Column(Float)

    # 收益
    avg_stock_return_pct = Column(Float)
    avg_simulated_return_pct = Column(Float)

    # 目标价触发统计（仅 long 且配置止盈/止损时统计）
    stop_loss_trigger_rate = Column(Float)
    take_profit_trigger_rate = Column(Float)
    ambiguous_rate = Column(Float)
    avg_days_to_first_hit = Column(Float)

    # Holding-decision metric. Legacy reports without a frozen holding action
    # remain unavailable and are excluded from the accuracy denominator.
    direction_evaluable_count = Column(Integer, default=0)
    holding_applicable_count = Column(Integer, default=0)
    holding_evaluable_count = Column(Integer, default=0)
    holding_unavailable_count = Column(Integer, default=0)
    holding_correct_count = Column(Integer, default=0)
    holding_incorrect_count = Column(Integer, default=0)
    holding_neutral_count = Column(Integer, default=0)
    holding_decision_accuracy_pct = Column(Float)
    avg_holding_decision_return_pct = Column(Float)

    # 诊断字段（JSON 字符串）
    advice_breakdown_json = Column(Text)
    diagnostics_json = Column(Text)

    __table_args__ = (
        UniqueConstraint(
            'scope',
            'code',
            'eval_window_days',
            'engine_version',
            name='uix_backtest_summary_scope_code_window_version',
        ),
    )


class PortfolioAccount(Base):
    """Portfolio account metadata."""

    __tablename__ = 'portfolio_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    owner_id = Column(String(64), index=True)
    name = Column(String(64), nullable=False)
    broker = Column(String(64))
    market = Column(String(8), nullable=False, default='cn', index=True)  # cn/hk/us
    base_currency = Column(String(8), nullable=False, default='CNY')
    is_active = Column(Boolean, nullable=False, default=True, index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        Index('ix_portfolio_account_owner_active', 'owner_id', 'is_active'),
    )


class PortfolioTrade(Base):
    """Executed trade events used as the source of truth for replay."""

    __tablename__ = 'portfolio_trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    trade_uid = Column(String(128))
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    trade_date = Column(Date, nullable=False, index=True)
    side = Column(String(8), nullable=False)  # buy/sell
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, default=0.0)
    tax = Column(Float, default=0.0)
    note = Column(String(255))
    dedup_hash = Column(String(64), index=True)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('account_id', 'trade_uid', name='uix_portfolio_trade_uid'),
        UniqueConstraint('account_id', 'dedup_hash', name='uix_portfolio_trade_dedup_hash'),
        Index('ix_portfolio_trade_account_date', 'account_id', 'trade_date'),
    )


class PortfolioCashLedger(Base):
    """Cash in/out events."""

    __tablename__ = 'portfolio_cash_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    direction = Column(String(8), nullable=False)  # in/out
    amount = Column(Float, nullable=False)
    currency = Column(String(8), nullable=False, default='CNY')
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_cash_account_date', 'account_id', 'event_date'),
    )


class PortfolioCorporateAction(Base):
    """Corporate actions that impact cash or share quantity."""

    __tablename__ = 'portfolio_corporate_actions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    effective_date = Column(Date, nullable=False, index=True)
    action_type = Column(String(24), nullable=False)  # cash_dividend/split_adjustment
    cash_dividend_per_share = Column(Float)
    split_ratio = Column(Float)
    note = Column(String(255))
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_ca_account_date', 'account_id', 'effective_date'),
    )


class PortfolioPosition(Base):
    """Latest replayed position snapshot for each symbol in one account."""

    __tablename__ = 'portfolio_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    quantity = Column(Float, nullable=False, default=0.0)
    avg_cost = Column(Float, nullable=False, default=0.0)
    total_cost = Column(Float, nullable=False, default=0.0)
    last_price = Column(Float, nullable=False, default=0.0)
    market_value_base = Column(Float, nullable=False, default=0.0)
    unrealized_pnl_base = Column(Float, nullable=False, default=0.0)
    valuation_currency = Column(String(8), nullable=False, default='CNY')
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'symbol',
            'market',
            'currency',
            'cost_method',
            name='uix_portfolio_position_account_symbol_market_currency',
        ),
    )


class PortfolioPositionLot(Base):
    """Lot-level remaining quantities used by FIFO replay."""

    __tablename__ = 'portfolio_position_lots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')
    symbol = Column(String(16), nullable=False, index=True)
    market = Column(String(8), nullable=False, default='cn')
    currency = Column(String(8), nullable=False, default='CNY')
    open_date = Column(Date, nullable=False, index=True)
    remaining_quantity = Column(Float, nullable=False, default=0.0)
    unit_cost = Column(Float, nullable=False, default=0.0)
    source_trade_id = Column(Integer, ForeignKey('portfolio_trades.id'))
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_portfolio_lot_account_symbol', 'account_id', 'symbol'),
    )


class PortfolioDailySnapshot(Base):
    """Daily account snapshot generated by read-time replay."""

    __tablename__ = 'portfolio_daily_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), nullable=False, index=True)
    snapshot_date = Column(Date, nullable=False, index=True)
    cost_method = Column(String(8), nullable=False, default='fifo')  # fifo/avg
    base_currency = Column(String(8), nullable=False, default='CNY')
    total_cash = Column(Float, nullable=False, default=0.0)
    total_market_value = Column(Float, nullable=False, default=0.0)
    total_equity = Column(Float, nullable=False, default=0.0)
    unrealized_pnl = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    fee_total = Column(Float, nullable=False, default=0.0)
    tax_total = Column(Float, nullable=False, default=0.0)
    fx_stale = Column(Boolean, nullable=False, default=False)
    payload = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'account_id',
            'snapshot_date',
            'cost_method',
            name='uix_portfolio_snapshot_account_date_method',
        ),
    )


class PortfolioFxRate(Base):
    """Cached FX rates used for cross-currency portfolio conversion."""

    __tablename__ = 'portfolio_fx_rates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    from_currency = Column(String(8), nullable=False, index=True)
    to_currency = Column(String(8), nullable=False, index=True)
    rate_date = Column(Date, nullable=False, index=True)
    rate = Column(Float, nullable=False)
    source = Column(String(32), nullable=False, default='manual')
    is_stale = Column(Boolean, nullable=False, default=False)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (
        UniqueConstraint(
            'from_currency',
            'to_currency',
            'rate_date',
            name='uix_portfolio_fx_pair_date',
        ),
    )


class ConversationMessage(Base):
    """
    Agent 对话历史记录表
    """
    __tablename__ = 'conversation_messages'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), index=True, nullable=False)
    role = Column(String(20), nullable=False)  # user, assistant, system
    content = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.now, index=True)


class ConversationSummary(Base):
    """Rolling summary for visible Agent chat history."""

    __tablename__ = 'conversation_summaries'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, unique=True, index=True)
    summary = Column(Text, nullable=False)
    covered_message_id = Column(Integer, nullable=False, default=0)
    source_message_count = Column(Integer, nullable=False, default=0)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)


class AgentProviderTurn(Base):
    """Provider protocol trace required for thinking/tool-call roundtrip."""

    __tablename__ = 'agent_provider_turns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    session_id = Column(String(100), nullable=False, index=True)
    run_id = Column(String(64), nullable=False, index=True)
    provider = Column(String(64), nullable=False, index=True)
    model = Column(String(160), nullable=False, index=True)
    anchor_user_message_id = Column(Integer, nullable=False, index=True)
    anchor_assistant_message_id = Column(Integer, nullable=False, index=True)
    messages_json = Column(Text, nullable=False)
    contains_reasoning = Column(Boolean, nullable=False, default=False)
    contains_tool_calls = Column(Boolean, nullable=False, default=False)
    contains_thinking_blocks = Column(Boolean, nullable=False, default=False)
    must_roundtrip = Column(Boolean, nullable=False, default=False, index=True)
    estimated_tokens = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_agent_provider_turn_bucket', 'session_id', 'provider', 'model', 'must_roundtrip'),
    )


class LLMUsage(Base):
    """One row per litellm.completion() call — token-usage audit log."""

    __tablename__ = 'llm_usage'

    id = Column(Integer, primary_key=True, autoincrement=True)
    # 'analysis' | 'agent' | 'market_review'
    call_type = Column(String(32), nullable=False, index=True)
    model = Column(String(128), nullable=False)
    stock_code = Column(String(16), nullable=True)
    provider = Column(String(64), nullable=True)
    prompt_tokens = Column(Integer, nullable=False, default=0)
    completion_tokens = Column(Integer, nullable=False, default=0)
    total_tokens = Column(Integer, nullable=False, default=0)

    # Sanitized provider usage snapshot; raw prompts, messages, headers, and
    # tokenizer free-text fields are intentionally not persisted here.
    provider_usage_json = Column(Text, nullable=True)
    provider_usage_schema_name = Column(String(64), nullable=True)
    provider_usage_schema_version = Column(String(32), nullable=True)
    provider_usage_observed_at = Column(String(32), nullable=True)

    # Normalized telemetry values are derived from provider usage and may stay
    # NULL when the provider payload is absent or explicitly invalid.
    normalized_prompt_tokens = Column(Integer, nullable=True)
    normalized_completion_tokens = Column(Integer, nullable=True)
    normalized_total_tokens = Column(Integer, nullable=True)
    normalized_cache_read_tokens = Column(Integer, nullable=True)
    normalized_cache_write_tokens = Column(Integer, nullable=True)
    normalized_cache_miss_tokens = Column(Integer, nullable=True)
    normalized_uncached_input_tokens = Column(Integer, nullable=True)
    normalized_cache_eligible_input_tokens = Column(Integer, nullable=True)
    normalized_cache_hit_ratio = Column(Float, nullable=True)
    normalized_cache_write_ratio = Column(Float, nullable=True)
    cache_capability = Column(String(32), nullable=True)
    cache_eligibility = Column(String(32), nullable=True)
    cache_observation = Column(String(32), nullable=True)
    estimated_prefix_tokens = Column(Integer, nullable=True)
    provider_reported_prompt_tokens = Column(Integer, nullable=True)
    provider_reported_cached_tokens = Column(Integer, nullable=True)
    provider_min_cache_tokens = Column(Integer, nullable=True)
    eligibility_confidence = Column(String(32), nullable=True)

    # Kept nullable for schema compatibility; new writes do not store provider
    # or proxy tokenizer free-text values.
    tokenizer_name = Column(String(128), nullable=True)
    tokenizer_version = Column(String(64), nullable=True)

    # HMAC fingerprints let deployments compare message shapes without storing
    # raw prompt/message content.
    messages_hmac = Column(String(64), nullable=True)
    system_message_hmac = Column(String(64), nullable=True)
    user_message_hmac = Column(String(64), nullable=True)
    hmac_key_version = Column(String(64), nullable=True)
    hmac_domain = Column(String(32), nullable=True)
    hash_scope = Column(String(32), nullable=True)

    # P0.5a internal legacy message stability audit. These diagnostics are
    # stored locally only and are not returned by public usage APIs.
    language = Column(String(16), nullable=True)
    market_group = Column(String(16), nullable=True)
    analysis_mode = Column(String(64), nullable=True)
    legacy_prompt_mode = Column(String(32), nullable=True)
    skill_config_hmac = Column(String(64), nullable=True)
    transport = Column(String(64), nullable=True)
    message_count = Column(Integer, nullable=True)
    estimated_total_prompt_tokens = Column(Integer, nullable=True)
    approx_common_prefix_chars = Column(Integer, nullable=True)
    approx_common_prefix_tokens = Column(Integer, nullable=True)
    known_dynamic_marker_positions = Column(Text, nullable=True)
    called_at = Column(DateTime, default=datetime.now, index=True)


_LLM_USAGE_TELEMETRY_COLUMN_SQL: Dict[str, str] = {
    "provider_usage_json": "TEXT",
    "provider": "VARCHAR(64)",
    "provider_usage_schema_name": "VARCHAR(64)",
    "provider_usage_schema_version": "VARCHAR(32)",
    "provider_usage_observed_at": "VARCHAR(32)",
    "normalized_prompt_tokens": "INTEGER",
    "normalized_completion_tokens": "INTEGER",
    "normalized_total_tokens": "INTEGER",
    "normalized_cache_read_tokens": "INTEGER",
    "normalized_cache_write_tokens": "INTEGER",
    "normalized_cache_miss_tokens": "INTEGER",
    "normalized_uncached_input_tokens": "INTEGER",
    "normalized_cache_eligible_input_tokens": "INTEGER",
    "normalized_cache_hit_ratio": "FLOAT",
    "normalized_cache_write_ratio": "FLOAT",
    "cache_capability": "VARCHAR(32)",
    "cache_eligibility": "VARCHAR(32)",
    "cache_observation": "VARCHAR(32)",
    "estimated_prefix_tokens": "INTEGER",
    "provider_reported_prompt_tokens": "INTEGER",
    "provider_reported_cached_tokens": "INTEGER",
    "provider_min_cache_tokens": "INTEGER",
    "eligibility_confidence": "VARCHAR(32)",
    "tokenizer_name": "VARCHAR(128)",
    "tokenizer_version": "VARCHAR(64)",
    "messages_hmac": "VARCHAR(64)",
    "system_message_hmac": "VARCHAR(64)",
    "user_message_hmac": "VARCHAR(64)",
    "hmac_key_version": "VARCHAR(64)",
    "hmac_domain": "VARCHAR(32)",
    "hash_scope": "VARCHAR(32)",
    "language": "VARCHAR(16)",
    "market_group": "VARCHAR(16)",
    "analysis_mode": "VARCHAR(64)",
    "legacy_prompt_mode": "VARCHAR(32)",
    "skill_config_hmac": "VARCHAR(64)",
    "transport": "VARCHAR(64)",
    "message_count": "INTEGER",
    "estimated_total_prompt_tokens": "INTEGER",
    "approx_common_prefix_chars": "INTEGER",
    "approx_common_prefix_tokens": "INTEGER",
    "known_dynamic_marker_positions": "TEXT",
}
_LLM_USAGE_INTEGER_TELEMETRY_COLUMNS = {
    column
    for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items()
    if column_type == "INTEGER"
}
_LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS = {"tokenizer_name", "tokenizer_version"}
_LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR = "prompt_cache_telemetry_disabled"
_LLM_PROMPT_CACHE_TELEMETRY_COLUMNS = {
    "provider_usage_json",
    "provider_usage_schema_name",
    "provider_usage_schema_version",
    "provider_usage_observed_at",
    "normalized_cache_read_tokens",
    "normalized_cache_write_tokens",
    "normalized_cache_miss_tokens",
    "normalized_uncached_input_tokens",
    "normalized_cache_eligible_input_tokens",
    "normalized_cache_hit_ratio",
    "normalized_cache_write_ratio",
    "cache_capability",
    "cache_eligibility",
    "cache_observation",
    "estimated_prefix_tokens",
    "provider_reported_cached_tokens",
    "provider_min_cache_tokens",
    "eligibility_confidence",
}


class AlertRuleRecord(Base):
    """Persisted alert rule managed through the Alert API."""

    __tablename__ = 'alert_rules'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    target_scope = Column(String(32), nullable=False, default='single_symbol', index=True)
    target = Column(String(64), nullable=False, index=True)
    alert_type = Column(String(32), nullable=False, index=True)
    parameters = Column(Text, nullable=False, default='{}')
    severity = Column(String(16), nullable=False, default='warning', index=True)
    enabled = Column(Boolean, nullable=False, default=True, index=True)
    source = Column(String(16), nullable=False, default='api', index=True)
    cooldown_policy = Column(Text)
    notification_policy = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_rule_type_target', 'alert_type', 'target'),
    )


class AlertTriggerRecord(Base):
    """Alert trigger history row.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_triggers'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    target = Column(String(64), nullable=False, index=True)
    observed_value = Column(Float)
    threshold = Column(Float)
    reason = Column(Text)
    data_source = Column(String(64))
    data_timestamp = Column(DateTime, index=True)
    triggered_at = Column(DateTime, default=datetime.now, index=True)
    status = Column(String(16), nullable=False, default='triggered', index=True)
    diagnostics = Column(Text)

    __table_args__ = (
        Index('ix_alert_trigger_rule_time', 'rule_id', 'triggered_at'),
    )


class AlertNotificationRecord(Base):
    """Notification attempt row for alert triggers.

    P1 exposes read APIs and table shape; runtime writer integration lands in
    later phases.
    """

    __tablename__ = 'alert_notifications'

    id = Column(Integer, primary_key=True, autoincrement=True)
    trigger_id = Column(Integer, index=True)
    channel = Column(String(32), nullable=False, index=True)
    attempt = Column(Integer, nullable=False, default=1)
    success = Column(Boolean, nullable=False, default=False, index=True)
    error_code = Column(String(64))
    retryable = Column(Boolean, nullable=False, default=False)
    latency_ms = Column(Integer)
    diagnostics = Column(Text)
    created_at = Column(DateTime, default=datetime.now, index=True)

    __table_args__ = (
        Index('ix_alert_notification_trigger_channel', 'trigger_id', 'channel'),
    )


class AlertCooldownRecord(Base):
    """Persisted alert cooldown state for DB-managed alert rules."""

    __tablename__ = 'alert_cooldowns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    rule_id = Column(Integer, index=True)
    # Reserved for future non-DB/expanded-scope rules; P4 queries by rule_id.
    rule_key = Column(String(255), index=True)
    target = Column(String(64), nullable=False, index=True)
    severity = Column(String(16), nullable=False, default='warning', index=True)
    last_triggered_at = Column(DateTime, index=True)
    cooldown_until = Column(DateTime, index=True)
    reason = Column(Text)
    state = Column(String(16), nullable=False, default='active', index=True)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now, index=True)

    __table_args__ = (
        UniqueConstraint('rule_id', 'target', 'severity', name='uix_alert_cooldown_rule_target_severity'),
    )


class DecisionSignalRecord(Base):
    """Persisted AI decision signal asset for Issue #1390 P1."""

    __tablename__ = 'decision_signals'

    id = Column(Integer, primary_key=True, autoincrement=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    market = Column(String(8), nullable=False, index=True)
    source_type = Column(String(32), nullable=False, index=True)
    source_agent = Column(String(64))
    source_report_id = Column(Integer, index=True)
    trace_id = Column(String(64), index=True)
    decision_profile = Column(String(16), index=True)
    market_phase = Column(String(24), index=True)
    trigger_source = Column(String(64), nullable=False, index=True)
    action = Column(String(16), nullable=False, index=True)
    action_label = Column(String(32))
    confidence = Column(Float)
    score = Column(Integer)
    horizon = Column(String(16), index=True)
    entry_low = Column(Float)
    entry_high = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    invalidation = Column(Text)
    watch_conditions = Column(Text)
    reason = Column(Text)
    risk_summary = Column(Text)
    catalyst_summary = Column(Text)
    evidence_json = Column(Text)
    data_quality_summary_json = Column(Text)
    plan_quality = Column(String(16), nullable=False, default='unknown', index=True)
    status = Column(String(16), nullable=False, default='active', index=True)
    expires_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)
    metadata_json = Column(Text)

    __table_args__ = (
        Index('ix_decision_signal_stock_status_time', 'stock_code', 'status', 'created_at'),
        Index('ix_decision_signal_market_status_time', 'market', 'status', 'created_at'),
        Index(
            'ix_decision_signal_report_type_market_stock_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_report_type_market_stock_profile_action_horizon_phase',
            'source_report_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase',
            'trace_id',
            'source_type',
            'market',
            'stock_code',
            'decision_profile',
            'action',
            'horizon',
            'market_phase',
        ),
        Index(
            'ix_decision_signal_market_stock_profile_created',
            'market',
            'stock_code',
            'decision_profile',
            'created_at',
        ),
    )


class DecisionSignalOutcomeRecord(Base):
    """Signal-level forward outcome for Issue #1390 P5."""

    __tablename__ = 'decision_signal_outcomes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, index=True)
    horizon = Column(String(16), nullable=False, index=True)
    engine_version = Column(String(32), nullable=False, index=True)
    eval_status = Column(String(24), nullable=False, default='unable', index=True)
    outcome = Column(String(16), index=True)
    direction_expected = Column(String(16), index=True)
    direction_correct = Column(Boolean)
    unable_reason = Column(String(64), index=True)
    anchor_date = Column(Date, index=True)
    eval_window_days = Column(Integer)
    start_price = Column(Float)
    end_close = Column(Float)
    max_high = Column(Float)
    min_low = Column(Float)
    stock_return_pct = Column(Float)

    action = Column(String(16), index=True)
    market = Column(String(8), index=True)
    market_phase = Column(String(24), index=True)
    source_type = Column(String(32), index=True)
    source_agent = Column(String(64), index=True)
    plan_quality = Column(String(16), index=True)
    data_quality_level = Column(String(24), index=True)
    holding_state = Column(String(16), nullable=False, default='unknown', index=True)

    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)

    __table_args__ = (
        UniqueConstraint('signal_id', 'horizon', 'engine_version', name='uix_decision_signal_outcome_key'),
        Index('ix_decision_signal_outcome_stats_action', 'engine_version', 'action', 'horizon'),
        Index('ix_decision_signal_outcome_stats_market', 'engine_version', 'market', 'horizon'),
    )


class DecisionSignalFeedbackRecord(Base):
    """Latest user feedback for a decision signal."""

    __tablename__ = 'decision_signal_feedback'

    id = Column(Integer, primary_key=True, autoincrement=True)
    signal_id = Column(Integer, nullable=False, unique=True, index=True)
    feedback_value = Column(String(16), nullable=False, index=True)
    reason_code = Column(String(64), index=True)
    note = Column(Text)
    source = Column(String(16), nullable=False, default='api', index=True)
    created_at = Column(DateTime, default=utc_naive_now, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, index=True)


class PaperExecutionPlan(Base):
    """Versioned, auditable paper-only execution plan derived from a report."""

    __tablename__ = 'paper_execution_plans'

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_key = Column(String(160), nullable=False, unique=True, index=True)
    plan_version = Column(String(32), nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    report_date = Column(Date, nullable=False, index=True)
    report_created_at = Column(DateTime, nullable=False)
    valid_for_date = Column(Date, index=True)
    action = Column(String(16), nullable=False, index=True)
    executable = Column(Boolean, nullable=False, default=False, index=True)
    status = Column(String(24), nullable=False, default='active', index=True)
    rules_json = Column(Text, nullable=False, default='[]')
    descriptive_conditions_json = Column(Text, nullable=False, default='[]')
    stop_loss = Column(Float)
    target_price = Column(Float)
    plan_allocation_budget = Column(Float)
    plan_risk_budget = Column(Float)
    budget_snapshot_equity = Column(Float)
    budget_snapshot_at = Column(DateTime)
    trading_style = Column(String(32), index=True)
    trading_style_policy_version = Column(String(32))
    trading_style_settings_json = Column(Text)
    market_regime_context_id = Column(Integer, ForeignKey('daily_market_regimes.id'), index=True)
    market_regime = Column(String(32), index=True)
    market_regime_policy_version = Column(String(32))
    max_holding_sessions = Column(Integer, nullable=False, default=10)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_plan_identity_status', 'market', 'stock_code', 'status'),
        Index('ix_paper_plan_source_identity', 'source_report_id', 'decision_signal_id'),
    )


class PaperPlanEvaluation(Base):
    """One idempotent end-of-day evaluation for a plan and trading date."""

    __tablename__ = 'paper_plan_evaluations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    evaluation_key = Column(String(220), nullable=False, default='', unique=True, index=True)
    plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    position_id = Column(Integer, ForeignKey('paper_positions.id'), index=True)
    entry_leg_id = Column(String(64), nullable=False, default='legacy_entry', index=True)
    evaluation_date = Column(Date, nullable=False, index=True)
    result = Column(String(24), nullable=False, index=True)
    reasons_json = Column(Text, nullable=False, default='[]')
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_eval_plan_date_identity', 'plan_id', 'evaluation_date', 'position_id', 'entry_leg_id'),
    )


class PaperPlanObservation(Base):
    """Idempotent intraday paper-monitor observation with quote provenance."""

    __tablename__ = 'paper_plan_observations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    observation_key = Column(String(220), nullable=False, unique=True, index=True)
    plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    position_id = Column(Integer, ForeignKey('paper_positions.id'), index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    mode = Column(String(16), nullable=False, default='intraday', index=True)
    observed_at = Column(DateTime, nullable=False, index=True)
    bucket_started_at = Column(DateTime, nullable=False, index=True)
    quote_price = Column(Float)
    quote_source = Column(String(64))
    quote_provider_timestamp = Column(String(64))
    quote_fetched_at = Column(String(64))
    resolution_key = Column(String(220), index=True)
    resolution_bucket_json = Column(Text)
    logical_order_key = Column(String(220), index=True)
    result = Column(String(32), nullable=False, index=True)
    action = Column(String(16), nullable=False, index=True)
    composite_score = Column(Float)
    required_passed = Column(Boolean, nullable=False, default=False)
    factors_passed = Column(Integer, nullable=False, default=0)
    factors_available = Column(Integer, nullable=False, default=0)
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_observation_identity_time', 'market', 'stock_code', 'bucket_started_at'),
        Index('ix_paper_observation_model_time', 'source_model', 'bucket_started_at'),
    )


class PaperResolution(Base):
    """Versioned resolver decision ledger shared by intraday and EOD paths."""

    __tablename__ = 'paper_resolutions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    resolution_key = Column(String(220), nullable=False, unique=True, index=True)
    policy_version = Column(String(32), nullable=False, index=True)
    mode = Column(String(32), nullable=False, index=True)
    status = Column(String(32), nullable=False, default='candidate', index=True)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    currency = Column(String(8))
    bucket_started_at = Column(DateTime, index=True)
    evaluation_date = Column(Date, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    selected_intent = Column(String(32), index=True)
    requested_quantity = Column(Float)
    executed_quantity = Column(Float)
    unfilled_quantity = Column(Float)
    candidate_intents_json = Column(Text, nullable=False, default='[]')
    suppressed_intents_json = Column(Text, nullable=False, default='[]')
    selected_intent_json = Column(Text, nullable=False, default='{}')
    source_identities_json = Column(Text, nullable=False, default='[]')
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_resolution_scope_time', 'source_model', 'market', 'stock_code', 'mode', 'bucket_started_at', 'evaluation_date'),
    )


class PaperReanalysisRequest(Base):
    """Durable once-only reanalysis request for paper-only monitor decisions."""

    __tablename__ = 'paper_reanalysis_requests'

    id = Column(Integer, primary_key=True, autoincrement=True)
    request_key = Column(String(260), nullable=False, unique=True, index=True)
    trigger_type = Column(String(32), nullable=False, index=True)
    status = Column(String(24), nullable=False, default='pending', index=True)
    source_plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    session_date = Column(Date, nullable=False, index=True)
    due_at = Column(DateTime, index=True)
    observed_at = Column(DateTime, index=True)
    claimed_at = Column(DateTime, index=True)
    submitted_at = Column(DateTime, index=True)
    completed_at = Column(DateTime, index=True)
    task_id = Column(String(64), index=True)
    query_id = Column(String(64), index=True)
    replacement_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    replacement_plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), index=True)
    failure_reason = Column(String(128))
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_reanalysis_scope_status', 'source_model', 'market', 'stock_code', 'session_date', 'status'),
        Index('ix_paper_reanalysis_plan_trigger', 'source_plan_id', 'trigger_type', 'session_date'),
    )


class PaperCapitalAccount(Base):
    """Shared paper-only capital account scoped by model, market, and currency."""

    __tablename__ = 'paper_capital_accounts'

    id = Column(Integer, primary_key=True, autoincrement=True)
    account_key = Column(String(360), nullable=False, unique=True, index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    currency = Column(String(8), nullable=False, index=True)
    initial_capital = Column(Float, nullable=False)
    cash = Column(Float, nullable=False)
    external_cash_flow = Column(Float, nullable=False, default=0.0)
    invested_capital = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    exposure = Column(Float, nullable=False, default=0.0)
    open_position_count = Column(Integer, nullable=False, default=0)
    risk_per_trade_pct = Column(Float, nullable=False, default=1.0)
    max_position_pct = Column(Float, nullable=False, default=20.0)
    max_total_exposure_pct = Column(Float, nullable=False, default=80.0)
    max_open_positions = Column(Integer, nullable=False, default=5)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_capital_scope', 'source_model', 'market', 'currency'),
    )


class PaperCapitalAdjustment(Base):
    """Immutable paper-only funding journal for capital accounts."""

    __tablename__ = 'paper_capital_adjustments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    idempotency_key = Column(String(128), nullable=False, unique=True, index=True)
    payload_hash = Column(String(64), nullable=False)
    account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), nullable=False, index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    currency = Column(String(8), nullable=False, index=True)
    adjustment_type = Column(String(16), nullable=False, index=True)
    amount = Column(Float, nullable=False)
    reason = Column(String(255))
    before_cash = Column(Float, nullable=False)
    after_cash = Column(Float, nullable=False)
    before_book_equity = Column(Float, nullable=False)
    after_book_equity = Column(Float, nullable=False)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_capital_adjustment_account_time', 'account_id', 'created_at'),
        Index('ix_paper_capital_adjustment_scope_time', 'source_model', 'market', 'currency', 'created_at'),
    )


class PaperAccountRiskSnapshot(Base):
    """Persistent model-scoped account risk snapshot for entry gating."""

    __tablename__ = 'paper_account_risk_snapshots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_key = Column(String(220), nullable=False, unique=True, index=True)
    policy_version = Column(String(32), nullable=False, index=True)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), nullable=False, index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    currency = Column(String(8), nullable=False)
    session_date = Column(Date, nullable=False, index=True)
    bucket_started_at = Column(DateTime, nullable=False, index=True)
    observed_at = Column(DateTime, nullable=False, index=True)
    session_start_equity = Column(Float)
    current_equity = Column(Float)
    peak_equity = Column(Float)
    cash = Column(Float)
    exposure = Column(Float)
    realized_pnl = Column(Float)
    unrealized_pnl = Column(Float)
    total_pnl = Column(Float)
    daily_pnl = Column(Float)
    daily_loss_pct = Column(Float)
    drawdown_pct = Column(Float)
    losing_logical_exit_streak = Column(Integer, nullable=False, default=0)
    daily_trade_count = Column(Integer, nullable=False, default=0)
    daily_open_add_count = Column(Integer, nullable=False, default=0)
    daily_reduce_close_count = Column(Integer, nullable=False, default=0)
    valuation_complete = Column(Boolean, nullable=False, default=False, index=True)
    valuation_stale = Column(Boolean, nullable=False, default=True)
    total_utilization_pct = Column(Float)
    symbol_utilization_pct = Column(Float)
    sector_utilization_pct = Column(Float)
    correlation_utilization_pct = Column(Float)
    unknown_utilization_pct = Column(Float)
    active_state = Column(String(32), nullable=False, default='normal', index=True)
    active_reasons_json = Column(Text, nullable=False, default='[]')
    permissions_json = Column(Text, nullable=False, default='{}')
    concentration_json = Column(Text, nullable=False, default='{}')
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_risk_snapshot_scope_time', 'source_model', 'market', 'currency', 'session_date', 'bucket_started_at'),
    )


class PaperAccountRiskEvent(Base):
    """Immutable idempotent account risk transition and manual-control event."""

    __tablename__ = 'paper_account_risk_events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_key = Column(String(220), nullable=False, unique=True, index=True)
    idempotency_key = Column(String(128), index=True)
    payload_hash = Column(String(64))
    event_type = Column(String(32), nullable=False, index=True)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), nullable=False, index=True)
    snapshot_id = Column(Integer, ForeignKey('paper_account_risk_snapshots.id'), index=True)
    resolution_key = Column(String(220), index=True)
    logical_order_key = Column(String(220), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    currency = Column(String(8), nullable=False)
    old_state = Column(String(32))
    new_state = Column(String(32), nullable=False, index=True)
    active_reasons_json = Column(Text, nullable=False, default='[]')
    metric = Column(String(64))
    threshold = Column(Float)
    source = Column(String(64), nullable=False, default='risk_gate')
    recovery_condition_json = Column(Text, nullable=False, default='{}')
    evidence_json = Column(Text, nullable=False, default='{}')
    notification_status = Column(String(24))
    notification_error = Column(String(255))
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_risk_event_scope_time', 'source_model', 'market', 'currency', 'created_at'),
    )


class PaperPosition(Base):
    """Persistent simulated position. This model has no broker/order fields."""

    __tablename__ = 'paper_positions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    entry_leg_id = Column(String(64), nullable=False, default='legacy_entry', index=True)
    execution_index = Column(Integer, nullable=False, default=1, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    status = Column(String(16), nullable=False, default='open', index=True)
    opened_on = Column(Date, nullable=False, index=True)
    opened_at = Column(DateTime, index=True)
    opened_price = Column(Float, nullable=False)
    normalized_notional = Column(Float, nullable=False, default=100000.0)
    quantity = Column(Float, nullable=False)
    initial_quantity = Column(Float)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), index=True)
    currency = Column(String(8))
    allocated_capital = Column(Float)
    initial_allocated_capital = Column(Float)
    remaining_cost = Column(Float)
    realized_quantity = Column(Float, nullable=False, default=0.0)
    realized_proceeds = Column(Float, nullable=False, default=0.0)
    realized_fees = Column(Float, nullable=False, default=0.0)
    realized_pnl = Column(Float, nullable=False, default=0.0)
    lot_size = Column(Integer)
    risk_amount = Column(Float)
    risk_per_share = Column(Float)
    sizing_reason = Column(String(64))
    legacy_fixed_notional = Column(Boolean, nullable=False, default=False)
    trading_style = Column(String(32), index=True)
    trading_style_policy_version = Column(String(32))
    trading_style_settings_json = Column(Text)
    market_regime_context_id = Column(Integer, ForeignKey('daily_market_regimes.id'), index=True)
    market_regime = Column(String(32), index=True)
    market_regime_policy_version = Column(String(32))
    sync_origin = Column(String(32), nullable=False, default='paper_generated', index=True)
    sync_status = Column(String(32), nullable=False, default='paper_only', index=True)
    source_portfolio_position_id = Column(Integer, index=True)
    last_synced_at = Column(DateTime, index=True)
    available_quantity = Column(Float)
    frozen_quantity = Column(Float)
    pending_exit_reason = Column(String(64), index=True)
    pending_exit_on = Column(Date, index=True)
    pending_exit_price = Column(Float)
    pending_exit_event_id = Column(Integer, index=True)
    sellability_rule = Column(String(32), nullable=False, default='unknown_t1_fail_closed', index=True)
    stop_loss = Column(Float)
    target_price = Column(Float)
    max_holding_sessions = Column(Integer, nullable=False)
    closed_on = Column(Date, index=True)
    closed_price = Column(Float)
    close_reason = Column(String(32), index=True)
    close_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    simulated_return_pct = Column(Float)
    simulated_pnl = Column(Float)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_position_identity_status', 'market', 'stock_code', 'status'),
        Index('uix_paper_position_plan_leg_execution', 'plan_id', 'entry_leg_id', 'execution_index', unique=True),
    )


class PaperTradeEvent(Base):
    """Immutable paper-ledger event with source attribution snapshots."""

    __tablename__ = 'paper_trade_events'

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_key = Column(String(192), nullable=False, unique=True, index=True)
    plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    position_id = Column(Integer, ForeignKey('paper_positions.id'), nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    event_type = Column(String(16), nullable=False, index=True)
    event_date = Column(Date, nullable=False, index=True)
    price = Column(Float, nullable=False)
    quantity = Column(Float)
    requested_quantity = Column(Float)
    executed_quantity = Column(Float)
    unfilled_quantity = Column(Float)
    gross_amount = Column(Float)
    commission = Column(Float)
    tax = Column(Float)
    slippage_amount = Column(Float)
    fees = Column(Float)
    net_amount = Column(Float)
    gross_realized_pnl = Column(Float)
    net_realized_pnl = Column(Float)
    fill_price = Column(Float)
    fill_evidence_grade = Column(String(32))
    resolution_key = Column(String(220), index=True)
    logical_order_key = Column(String(220), index=True)
    instrument_assumption = Column(String(64))
    quote_provider_timestamp = Column(String(64))
    quote_fetched_at = Column(String(64))
    reason = Column(String(64), nullable=False)
    triggering_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), index=True)
    currency = Column(String(8))
    allocated_capital = Column(Float)
    cash_after = Column(Float)
    exposure_after = Column(Float)
    realized_pnl_after = Column(Float)
    details_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)


class PaperSignalConsumption(Base):
    """Once-only consumption guard for aggregate model defensive signals."""

    __tablename__ = 'paper_signal_consumptions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    consumption_key = Column(String(220), nullable=False, unique=True, index=True)
    signal_id = Column(Integer, ForeignKey('decision_signals.id'), nullable=False, index=True)
    capital_account_id = Column(Integer, ForeignKey('paper_capital_accounts.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    currency = Column(String(8), nullable=False)
    holding_intent_kind = Column(String(32), nullable=False, index=True)
    resolution_key = Column(String(220), nullable=False, index=True)
    logical_order_key = Column(String(220), index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        UniqueConstraint(
            'capital_account_id',
            'source_model',
            'market',
            'stock_code',
            'currency',
            'signal_id',
            'holding_intent_kind',
            name='uix_paper_signal_consumption_identity',
        ),
    )


class PaperRiskRevision(Base):
    """Immutable per-lot stop/target revision ledger."""

    __tablename__ = 'paper_risk_revisions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    revision_key = Column(String(220), nullable=False, unique=True, index=True)
    policy_version = Column(String(32), nullable=False, index=True)
    position_id = Column(Integer, ForeignKey('paper_positions.id'), nullable=False, index=True)
    plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    revision_version = Column(Integer, nullable=False, default=1, index=True)
    result = Column(String(32), nullable=False, index=True)
    reason = Column(String(96), nullable=False)
    old_stop_loss = Column(Float)
    new_stop_loss = Column(Float)
    applied_stop_loss = Column(Float)
    old_target_price = Column(Float)
    new_target_price = Column(Float)
    applied_target_price = Column(Float)
    evaluated_at = Column(DateTime, nullable=False, index=True)
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_risk_revision_position_time', 'position_id', 'evaluated_at'),
    )


class PaperStrategyThesis(Base):
    """Current accepted strategy thesis for one model/symbol/style scope."""

    __tablename__ = 'paper_strategy_theses'

    id = Column(Integer, primary_key=True, autoincrement=True)
    thesis_key = Column(String(220), nullable=False, unique=True, index=True)
    policy_version = Column(String(32), nullable=False, index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    stock_name = Column(String(64))
    trading_style = Column(String(32), nullable=False, index=True)
    state = Column(String(32), nullable=False, default='watching', index=True)
    effective_action = Column(String(16), nullable=False, default='watch', index=True)
    raw_action = Column(String(16), index=True)
    action_bucket = Column(String(16), nullable=False, default='neutral', index=True)
    confirmation_count = Column(Integer, nullable=False, default=0)
    contradiction_count = Column(Integer, nullable=False, default=0)
    accepted_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    accepted_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    last_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    last_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    entry_low = Column(Float)
    entry_high = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    thesis_text = Column(Text)
    invalidation_text = Column(Text)
    evidence_json = Column(Text, nullable=False, default='{}')
    accepted_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_strategy_thesis_scope', 'source_model', 'market', 'stock_code', 'trading_style'),
    )


class PaperStrategyTransition(Base):
    """Append-only audit of report-to-thesis acceptance decisions."""

    __tablename__ = 'paper_strategy_transitions'

    id = Column(Integer, primary_key=True, autoincrement=True)
    transition_key = Column(String(220), nullable=False, unique=True, index=True)
    thesis_id = Column(Integer, ForeignKey('paper_strategy_theses.id'), index=True)
    thesis_key = Column(String(220), nullable=False, index=True)
    policy_version = Column(String(32), nullable=False, index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    trading_style = Column(String(32), nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    raw_action = Column(String(16), index=True)
    previous_state = Column(String(32))
    new_state = Column(String(32), nullable=False, index=True)
    result = Column(String(32), nullable=False, index=True)
    reason = Column(String(96), nullable=False)
    plan_authorized = Column(Boolean, nullable=False, default=False, index=True)
    risk_revision_authorized = Column(Boolean, nullable=False, default=False)
    freeze_entry_authorized = Column(Boolean, nullable=False, default=False)
    confirmation_count = Column(Integer, nullable=False, default=0)
    contradiction_count = Column(Integer, nullable=False, default=0)
    entry_low = Column(Float)
    entry_high = Column(Float)
    stop_loss = Column(Float)
    target_price = Column(Float)
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)

    __table_args__ = (
        Index('ix_paper_strategy_transition_scope_time', 'source_model', 'market', 'stock_code', 'created_at'),
    )


class PaperSyncLink(Base):
    """Idempotent explicit synchronization between portfolio and paper ledgers."""

    __tablename__ = 'paper_sync_links'

    id = Column(Integer, primary_key=True, autoincrement=True)
    idempotency_key = Column(String(160), nullable=False, unique=True, index=True)
    payload_hash = Column(String(64), nullable=False)
    direction = Column(String(32), nullable=False, index=True)
    status = Column(String(32), nullable=False, default='synced', index=True)
    source_model = Column(String(255), nullable=False, index=True)
    market = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    portfolio_account_id = Column(Integer, ForeignKey('portfolio_accounts.id'), index=True)
    portfolio_position_id = Column(Integer, index=True)
    portfolio_trade_id = Column(Integer, ForeignKey('portfolio_trades.id'), index=True)
    paper_plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), index=True)
    paper_position_id = Column(Integer, ForeignKey('paper_positions.id'), index=True)
    paper_event_id = Column(Integer, ForeignKey('paper_trade_events.id'), index=True)
    quantity = Column(Float, nullable=False)
    price = Column(Float, nullable=False)
    fee = Column(Float, nullable=False, default=0.0)
    trade_date = Column(Date, nullable=False, index=True)
    origin = Column(String(32), nullable=False)
    details_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_sync_identity_direction', 'source_model', 'market', 'stock_code', 'direction'),
        Index('ix_paper_sync_portfolio_position', 'portfolio_position_id', 'direction'),
    )


class PaperNotificationDispatch(Base):
    """Idempotent outbound notification state for paper trade events."""

    __tablename__ = 'paper_notification_dispatches'

    id = Column(Integer, primary_key=True, autoincrement=True)
    dispatch_key = Column(String(220), nullable=False, unique=True, index=True)
    event_id = Column(Integer, ForeignKey('paper_trade_events.id'), nullable=False, index=True)
    channel = Column(String(32), nullable=False, index=True)
    status = Column(String(24), nullable=False, default='pending', index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text)
    payload_json = Column(Text, nullable=False, default='{}')
    sent_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_paper_dispatch_channel_status', 'channel', 'status'),
    )


class RuntimeSchedulerHeartbeat(Base):
    """Shared scheduler heartbeat visible across API and worker containers."""

    __tablename__ = 'runtime_scheduler_heartbeats'

    id = Column(Integer, primary_key=True, autoincrement=True)
    heartbeat_key = Column(String(160), nullable=False, unique=True, index=True)
    owner_id = Column(String(128), nullable=False, index=True)
    owner_kind = Column(String(32), nullable=False, index=True)
    task_name = Column(String(96), nullable=False, index=True)
    status = Column(String(32), nullable=False, default='alive', index=True)
    pid = Column(Integer)
    observed_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    last_success_at = Column(DateTime, index=True)
    last_error_at = Column(DateTime, index=True)
    error_message = Column(Text)
    details_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_runtime_scheduler_heartbeat_task_time', 'task_name', 'observed_at'),
    )


class RuntimeSchedulerLease(Base):
    """Cross-process duplicate-bucket claim for scheduled worker execution."""

    __tablename__ = 'runtime_scheduler_leases'

    id = Column(Integer, primary_key=True, autoincrement=True)
    lease_key = Column(String(220), nullable=False, unique=True, index=True)
    owner_id = Column(String(128), nullable=False, index=True)
    owner_kind = Column(String(32), nullable=False, index=True)
    task_name = Column(String(96), nullable=False, index=True)
    bucket_started_at = Column(DateTime, nullable=False, index=True)
    lease_until = Column(DateTime, nullable=False, index=True)
    status = Column(String(32), nullable=False, default='claimed', index=True)
    started_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    finished_at = Column(DateTime, index=True)
    last_heartbeat_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    last_error = Column(Text)
    details_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_runtime_scheduler_lease_task_bucket', 'task_name', 'bucket_started_at'),
    )


class ReportPlanReconciliation(Base):
    """Durable report -> DecisionSignal -> paper-plan processing state."""

    __tablename__ = 'report_plan_reconciliations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    reconciliation_key = Column(String(220), nullable=False, unique=True, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    plan_version = Column(String(32), nullable=False, index=True)
    source_model = Column(String(255), index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    paper_plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), index=True)
    status = Column(String(32), nullable=False, default='pending', index=True)
    classification = Column(String(64), index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)
    next_retry_at = Column(DateTime, index=True)
    last_attempt_at = Column(DateTime, index=True)
    last_error = Column(Text)
    details_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_report_plan_reconcile_status_retry', 'status', 'next_retry_at'),
        Index('ix_report_plan_reconcile_report_version', 'source_report_id', 'plan_version'),
    )


class DailyMarketRegime(Base):
    """Structured market regime consumed by deterministic paper entry gates."""

    __tablename__ = 'daily_market_regimes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    regime_key = Column(String(160), nullable=False, unique=True, index=True)
    region = Column(String(16), nullable=False, index=True)
    session_date = Column(Date, nullable=False, index=True)
    regime = Column(String(32), nullable=False, default='unavailable', index=True)
    allocation_multiplier = Column(Float, nullable=False, default=0.0)
    entry_allowed = Column(Boolean, nullable=False, default=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    source_query_id = Column(String(64), index=True)
    provenance_json = Column(Text, nullable=False, default='{}')
    freshness_expires_at = Column(DateTime, index=True)
    policy_version = Column(String(32), nullable=False, default='market-regime-v1', index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_daily_market_regime_region_date', 'region', 'session_date'),
    )


class MarketOpportunityRun(Base):
    """Durable market-theme opportunity discovery run."""

    __tablename__ = 'market_opportunity_runs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_key = Column(String(220), nullable=False, unique=True, index=True)
    market = Column(String(16), nullable=False, default='cn', index=True)
    trade_date = Column(Date, nullable=False, index=True)
    source_report_id = Column(Integer, ForeignKey('analysis_history.id'), nullable=False, index=True)
    market_regime_id = Column(Integer, ForeignKey('daily_market_regimes.id'), index=True)
    market_regime = Column(String(32), nullable=False, default='unavailable', index=True)
    mode = Column(String(16), nullable=False, default='off', index=True)
    status = Column(String(32), nullable=False, default='pending', index=True)
    policy_version = Column(String(64), nullable=False, default='opportunity-policy-v1', index=True)
    config_snapshot_json = Column(Text, nullable=False, default='{}')
    provenance_json = Column(Text, nullable=False, default='{}')
    theme_count = Column(Integer, nullable=False, default=0)
    selected_theme_count = Column(Integer, nullable=False, default=0)
    candidate_count = Column(Integer, nullable=False, default=0)
    selected_candidate_count = Column(Integer, nullable=False, default=0)
    llm_budget = Column(Integer, nullable=False, default=0)
    error_message = Column(Text)
    started_at = Column(DateTime, index=True)
    completed_at = Column(DateTime, index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        Index('ix_market_opportunity_run_scope', 'market', 'trade_date', 'status'),
        Index('ix_market_opportunity_run_report', 'source_report_id', 'policy_version'),
    )


class MarketOpportunityTheme(Base):
    """Theme-level opportunity score and constituent provenance."""

    __tablename__ = 'market_opportunity_themes'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('market_opportunity_runs.id'), nullable=False, index=True)
    theme_key = Column(String(220), nullable=False, index=True)
    theme_type = Column(String(16), nullable=False, index=True)
    theme_name = Column(String(128), nullable=False, index=True)
    theme_code = Column(String(64))
    rank = Column(Integer)
    change_pct = Column(Float)
    status = Column(String(32), nullable=False, default='rejected', index=True)
    score = Column(Float)
    score_components_json = Column(Text, nullable=False, default='{}')
    rejection_reasons_json = Column(Text, nullable=False, default='[]')
    data_quality_json = Column(Text, nullable=False, default='{}')
    constituent_snapshot_json = Column(Text, nullable=False, default='{}')
    constituent_count = Column(Integer, nullable=False, default=0)
    eligible_constituent_count = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        UniqueConstraint('run_id', 'theme_key', name='uix_market_opportunity_theme_run_key'),
        Index('ix_market_opportunity_theme_status_score', 'status', 'score'),
    )


class MarketOpportunityCandidate(Base):
    """Stock-level opportunity candidate with deterministic score evidence."""

    __tablename__ = 'market_opportunity_candidates'

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey('market_opportunity_runs.id'), nullable=False, index=True)
    theme_id = Column(Integer, ForeignKey('market_opportunity_themes.id'), index=True)
    candidate_key = Column(String(260), nullable=False, index=True)
    market = Column(String(16), nullable=False, default='cn', index=True)
    exchange = Column(String(8), nullable=False, index=True)
    stock_code = Column(String(16), nullable=False, index=True)
    symbol = Column(String(32), nullable=False, index=True)
    stock_name = Column(String(128))
    primary_theme_key = Column(String(220), index=True)
    related_themes_json = Column(Text, nullable=False, default='[]')
    status = Column(String(32), nullable=False, default='rejected', index=True)
    rank = Column(Integer)
    score = Column(Float)
    score_components_json = Column(Text, nullable=False, default='{}')
    rejection_reasons_json = Column(Text, nullable=False, default='[]')
    data_quality_json = Column(Text, nullable=False, default='{}')
    quote_snapshot_json = Column(Text, nullable=False, default='{}')
    as_of = Column(Date, nullable=False, index=True)
    valid_until = Column(Date, index=True)
    snapshot_hash = Column(String(80), index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        UniqueConstraint('run_id', 'candidate_key', name='uix_market_opportunity_candidate_run_key'),
        Index('ix_market_opportunity_candidate_status_score', 'status', 'score'),
        Index('ix_market_opportunity_candidate_identity', 'market', 'exchange', 'stock_code'),
    )


class MarketOpportunityAnalysisJob(Base):
    """Per-model opportunity analysis job ledger."""

    __tablename__ = 'market_opportunity_analysis_jobs'

    id = Column(Integer, primary_key=True, autoincrement=True)
    job_key = Column(String(300), nullable=False, unique=True, index=True)
    run_id = Column(Integer, ForeignKey('market_opportunity_runs.id'), nullable=False, index=True)
    candidate_id = Column(Integer, ForeignKey('market_opportunity_candidates.id'), nullable=False, index=True)
    model = Column(String(255), nullable=False, index=True)
    status = Column(String(32), nullable=False, default='pending', index=True)
    lease_owner = Column(String(128), index=True)
    lease_expires_at = Column(DateTime, index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=2)
    next_retry_at = Column(DateTime, index=True)
    task_id = Column(String(64), index=True)
    query_id = Column(String(64), index=True)
    analysis_history_id = Column(Integer, ForeignKey('analysis_history.id'), index=True)
    decision_signal_id = Column(Integer, ForeignKey('decision_signals.id'), index=True)
    paper_plan_id = Column(Integer, ForeignKey('paper_execution_plans.id'), index=True)
    failure_reason = Column(String(160))
    evidence_json = Column(Text, nullable=False, default='{}')
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        UniqueConstraint('candidate_id', 'model', name='uix_market_opportunity_job_candidate_model'),
        Index('ix_market_opportunity_job_status_retry', 'status', 'next_retry_at'),
        Index('ix_market_opportunity_job_run_model', 'run_id', 'model', 'status'),
    )


# 机会池 run/job 的来源由不可变 key 前缀区分；研究任务的前缀在首次落库时写入，
# 后续状态更新不会改变 job_key，因此外部输入无法把研究任务提升为可执行任务。
MARKET_OPPORTUNITY_DISCOVERY_RUN_KEY_PREFIX = "market-opportunity:"
MARKET_OPPORTUNITY_FOLLOWUP_RUN_KEY_PREFIX = "opportunity-followup:"
MARKET_OPPORTUNITY_RESEARCH_REVIEW_RUN_KEY_PREFIX = "opportunity-research-review:"
MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX = "market-opportunity-research:"


def is_market_opportunity_research_job_key(job_key: Any) -> bool:
    """Return whether a persisted opportunity job was created research-only."""

    return str(job_key or "").startswith(MARKET_OPPORTUNITY_RESEARCH_JOB_KEY_PREFIX)


class MarketOpportunityResearchBudgetSlot(Base):
    """Per-market, per-session research LLM budget ledger.

    Each row occupies one numbered slot. ``reserved`` rows hold a slot for a
    due research review; ``charged`` rows record one submitted model attempt.
    Slots are never released, so retries and restarts cannot exceed the limit.
    """

    __tablename__ = 'market_opportunity_research_budget_slots'

    id = Column(Integer, primary_key=True, autoincrement=True)
    market = Column(String(16), nullable=False, default='cn', index=True)
    session_date = Column(Date, nullable=False, index=True)
    slot_no = Column(Integer, nullable=False)
    job_id = Column(Integer, index=True)
    attempt_no = Column(Integer)
    job_kind = Column(String(16), nullable=False, default='initial')
    state = Column(String(16), nullable=False, default='charged', index=True)
    reservation_key = Column(String(200), unique=True)
    query_id = Column(String(64), index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        UniqueConstraint('market', 'session_date', 'slot_no', name='uix_mo_research_budget_slot'),
        UniqueConstraint('job_id', 'attempt_no', name='uix_mo_research_budget_attempt'),
    )


class BoardObservation(Base):
    """Daily complete board-ranking snapshot for opportunity discovery."""

    __tablename__ = 'board_observations'

    id = Column(Integer, primary_key=True, autoincrement=True)
    snapshot_key = Column(String(220), nullable=False, unique=True, index=True)
    market = Column(String(8), nullable=False, default='cn', index=True)
    trade_date = Column(Date, nullable=False, index=True)
    source = Column(String(64), nullable=False, index=True)
    taxonomy = Column(String(64), nullable=False, index=True)
    status = Column(String(32), nullable=False, default='healthy', index=True)
    reason = Column(String(128))
    coverage_json = Column(Text, nullable=False, default='{}')
    themes_json = Column(Text, nullable=False, default='[]')
    snapshot_hash = Column(String(64), nullable=False, default='', index=True)
    fetched_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    created_at = Column(DateTime, default=utc_naive_now, nullable=False, index=True)
    updated_at = Column(DateTime, default=utc_naive_now, onupdate=utc_naive_now, nullable=False)

    __table_args__ = (
        UniqueConstraint('market', 'trade_date', 'source', 'taxonomy', name='uix_board_observation_identity'),
        Index('ix_board_observation_market_date_status', 'market', 'trade_date', 'status'),
        Index('ix_board_observation_source_taxonomy_date', 'source', 'taxonomy', 'trade_date'),
    )


class _DatabaseManagerMeta(type):
    """Serialize DatabaseManager construction across __new__ and __init__."""

    def __call__(cls, *args, **kwargs):
        with cls._init_lock:
            return super().__call__(*args, **kwargs)


class DatabaseManager(metaclass=_DatabaseManagerMeta):
    """
    数据库管理器 - 单例模式

    职责：
    1. 管理数据库连接池
    2. 提供 Session 上下文管理
    3. 封装数据存取操作
    """

    _instance: Optional['DatabaseManager'] = None
    _init_lock = threading.RLock()
    _initialized: bool = False

    @staticmethod
    def _legacy_paper_opened_at_utc(market: str, opened_on: date) -> datetime:
        """Backfill legacy paper lots to market-close UTC-naive timestamps."""

        market_norm = str(market or "").lower()
        close_times = {
            "cn": (dt_time(15, 0), "Asia/Shanghai"),
            "hk": (dt_time(16, 0), "Asia/Shanghai"),
            "us": (dt_time(16, 0), "America/New_York"),
            "jp": (dt_time(15, 30), "Asia/Tokyo"),
            "kr": (dt_time(15, 30), "Asia/Seoul"),
            "tw": (dt_time(13, 30), "Asia/Taipei"),
        }
        close_time, zone_name = close_times.get(market_norm, (dt_time(15, 0), "Asia/Shanghai"))
        local_dt = datetime.combine(opened_on, close_time, tzinfo=ZoneInfo(zone_name))
        return local_dt.astimezone(timezone.utc).replace(tzinfo=None)

    def __new__(cls, *args, **kwargs):
        """单例模式实现"""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, db_url: Optional[str] = None):
        """
        初始化数据库管理器

        Args:
            db_url: 数据库连接 URL（可选，默认从配置读取）
        """
        if getattr(self, '_initialized', False):
            return

        created_engine = None

        try:
            config = get_config()
            if db_url is None:
                db_url = config.get_db_url()

            self._db_url = db_url
            self._sqlite_wal_enabled = config.sqlite_wal_enabled
            self._sqlite_busy_timeout_ms = config.sqlite_busy_timeout_ms
            self._sqlite_write_retry_max = config.sqlite_write_retry_max
            self._sqlite_write_retry_base_delay = config.sqlite_write_retry_base_delay

            engine_kwargs = {
                "echo": False,
                "pool_pre_ping": True,
            }
            if str(db_url).startswith("sqlite:") and self._sqlite_busy_timeout_ms > 0:
                engine_kwargs["connect_args"] = {
                    "timeout": self._sqlite_busy_timeout_ms / 1000,
                }

            # 创建数据库引擎
            created_engine = create_engine(
                db_url,
                **engine_kwargs,
            )
            self._engine = created_engine
            self._dialect_name = self._engine.url.get_backend_name()
            self._is_sqlite_engine = self._dialect_name == 'sqlite'
            self._sqlite_file_db = self._is_sqlite_engine and self._is_file_sqlite_database()
            self._install_sqlite_pragma_handler()

            # 创建 Session 工厂
            self._SessionLocal = sessionmaker(
                bind=self._engine,
                autocommit=False,
                autoflush=False,
            )

            # 创建所有表。Docker 会同时启动 API 与调度容器；PostgreSQL 下用
            # transaction-scoped advisory lock 串行化首次建表，避免两个进程
            # 在空库上并发 create_all 产生系统目录唯一键冲突。
            if self._dialect_name == "postgresql":
                with self._engine.begin() as connection:
                    connection.execute(
                        text("SELECT pg_advisory_xact_lock(:lock_id)"),
                        {"lock_id": POSTGRES_SCHEMA_ADVISORY_LOCK_ID},
                    )
                    create_application_schema(connection)
            else:
                Base.metadata.create_all(self._engine)
            self._ensure_analysis_attempt_schema()
            self._ensure_llm_usage_telemetry_columns()
            self._ensure_decision_signal_profile_schema()
            self._ensure_analysis_history_model_used_column()
            self._ensure_backtest_metric_columns()
            self._ensure_paper_trading_observation_schema()
            self._ensure_paper_trading_capital_schema()
            self._ensure_paper_positions_multi_lot_schema()
            self._ensure_paper_evaluations_position_leg_schema()
            self._ensure_paper_sync_links_portfolio_position_fk_removed()
            self._ensure_board_observation_schema()
            self._ensure_intelligence_item_scope_values()
            self._ensure_schema_migration_record()
            self._ensure_report_execution_intent_baseline()
            self._ensure_intelligence_items_unique_index()

            self._initialized = True
            safe_db_url = self._engine.url.render_as_string(hide_password=True)
            logger.info("数据库初始化完成: %s", safe_db_url)

            # 注册退出钩子，确保程序退出时关闭数据库连接
            atexit.register(DatabaseManager._cleanup_engine, self._engine)
        except Exception:
            self._initialized = False
            try:
                if created_engine is not None:
                    created_engine.dispose()
            except Exception as cleanup_exc:
                logger.warning("数据库初始化失败后的引擎清理也失败: %s", cleanup_exc)
            self._engine = None
            self._SessionLocal = None
            self.__class__._instance = None
            raise

    def _ensure_analysis_history_model_used_column(self) -> None:
        """Add/backfill the model dimension for existing local SQLite databases."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(AnalysisHistory.__tablename__):
            return

        existing = {
            column["name"]
            for column in inspector.get_columns(AnalysisHistory.__tablename__)
        }
        needs_backfill = "model_used" not in existing
        if needs_backfill:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {AnalysisHistory.__tablename__} "
                        "ADD COLUMN model_used VARCHAR(255)"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "model_used"):
                    raise

        with self._engine.begin() as connection:
            connection.exec_driver_sql(
                "CREATE INDEX IF NOT EXISTS ix_analysis_history_model_used "
                "ON analysis_history (model_used)"
            )
            rows = connection.exec_driver_sql(
                "SELECT id, raw_result FROM analysis_history "
                "WHERE model_used IS NULL AND raw_result IS NOT NULL"
            ).fetchall()
            for row_id, raw_result in rows:
                try:
                    payload = json.loads(raw_result)
                except (TypeError, ValueError):
                    continue
                if not isinstance(payload, dict):
                    continue
                model_used = str(payload.get("model_used") or "").strip()
                if not model_used:
                    continue
                connection.exec_driver_sql(
                    "UPDATE analysis_history SET model_used = ? WHERE id = ?",
                    (model_used[:255], row_id),
                )

    def _ensure_analysis_attempt_schema(self) -> None:
        """Create analysis_attempts for upgraded databases that predate the model."""

        try:
            Base.metadata.create_all(self._engine, tables=[AnalysisAttempt.__table__])
        except Exception as exc:
            logger.warning("analysis_attempts schema ensure failed: %s", exc)

    def _ensure_board_observation_schema(self) -> None:
        """Create board_observations for upgraded databases that predate the model."""

        try:
            Base.metadata.create_all(self._engine, tables=[BoardObservation.__table__])
        except Exception as exc:
            logger.warning("board_observations schema ensure failed: %s", exc)

    def _ensure_backtest_metric_columns(self) -> None:
        """Idempotently add dual-metric columns to existing SQLite databases."""

        if not self._is_sqlite_engine:
            return

        table_columns = {
            BacktestResult.__tablename__: {
                "holding_state": "VARCHAR(16)",
                "holding_action": "VARCHAR(16)",
                "holding_action_applicable": "BOOLEAN NOT NULL DEFAULT 0",
                "holding_action_evaluable": "BOOLEAN NOT NULL DEFAULT 0",
                "holding_eval_status": "VARCHAR(24) NOT NULL DEFAULT 'unavailable'",
                "holding_direction_expected": "VARCHAR(16)",
                "holding_decision_correct": "BOOLEAN",
                "holding_outcome": "VARCHAR(16)",
                "holding_decision_return_pct": "FLOAT",
            },
            BacktestSummary.__tablename__: {
                "direction_evaluable_count": "INTEGER DEFAULT 0",
                "holding_applicable_count": "INTEGER DEFAULT 0",
                "holding_evaluable_count": "INTEGER DEFAULT 0",
                "holding_unavailable_count": "INTEGER DEFAULT 0",
                "holding_correct_count": "INTEGER DEFAULT 0",
                "holding_incorrect_count": "INTEGER DEFAULT 0",
                "holding_neutral_count": "INTEGER DEFAULT 0",
                "holding_decision_accuracy_pct": "FLOAT",
                "avg_holding_decision_return_pct": "FLOAT",
            },
        }
        inspector = inspect(self._engine)
        for table_name, columns in table_columns.items():
            if not inspector.has_table(table_name):
                continue
            existing = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_sql in columns.items():
                if column_name in existing:
                    continue
                try:
                    with self._engine.begin() as connection:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
                        )
                except OperationalError as exc:
                    if not self._is_sqlite_duplicate_column_error(exc, column_name):
                        raise
        # Existing summaries predate the frozen holding contract. They must
        # remain excluded from holding accuracy, but should be counted as
        # unavailable instead of presenting a misleading zero.
        if inspector.has_table(BacktestSummary.__tablename__):
            with self._engine.begin() as connection:
                connection.exec_driver_sql(
                    "UPDATE backtest_summaries "
                    "SET holding_unavailable_count = COALESCE(total_evaluations, 0) "
                    "WHERE COALESCE(total_evaluations, 0) > 0 "
                    "AND COALESCE(holding_unavailable_count, 0) = 0 "
                    "AND COALESCE(holding_applicable_count, 0) = 0 "
                    "AND COALESCE(holding_evaluable_count, 0) = 0 "
                    "AND COALESCE(holding_correct_count, 0) = 0 "
                    "AND COALESCE(holding_incorrect_count, 0) = 0"
                )

    def _ensure_paper_sync_links_portfolio_position_fk_removed(self) -> None:
        """Remove legacy SQLite FK from paper_sync_links to refreshable portfolio_positions."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(PaperSyncLink.__tablename__):
            return
        try:
            with self._engine.connect() as connection:
                foreign_keys = connection.execute(
                    text(f"PRAGMA foreign_key_list({PaperSyncLink.__tablename__})")
                ).fetchall()
        except Exception as exc:
            logger.warning(
                "Failed to inspect paper_sync_links foreign keys; skip FK repair: %s",
                exc,
            )
            return

        has_legacy_fk = any(
            row[2] == PortfolioPosition.__tablename__
            and row[3] == "portfolio_position_id"
            and row[4] == "id"
            for row in foreign_keys
        )
        if not has_legacy_fk:
            return

        self._rebuild_paper_sync_links_table_without_portfolio_position_fk()

    def _rebuild_paper_sync_links_table_without_portfolio_position_fk(self) -> None:
        temporary_table = f"paper_sync_links_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        target_columns = [column.name for column in PaperSyncLink.__table__.columns]

        inspector = inspect(self._engine)
        existing_columns = {
            column["name"]
            for column in inspector.get_columns(PaperSyncLink.__tablename__)
        }
        copy_columns = [column for column in target_columns if column in existing_columns]
        if not copy_columns:
            logger.warning("paper_sync_links FK repair skipped because no shared columns were found.")
            return

        column_clause = ", ".join(f'"{column}"' for column in copy_columns)
        select_expressions = []
        for column in copy_columns:
            if column in {"created_at", "updated_at"}:
                select_expressions.append(f'COALESCE("{column}", CURRENT_TIMESTAMP) AS "{column}"')
            elif column == "status":
                select_expressions.append(f'COALESCE("{column}", \'synced\') AS "{column}"')
            elif column == "fee":
                select_expressions.append(f'COALESCE("{column}", 0) AS "{column}"')
            elif column == "details_json":
                select_expressions.append(f'COALESCE("{column}", \'{{}}\') AS "{column}"')
            else:
                select_expressions.append(f'"{column}"')
        select_clause = ", ".join(select_expressions)
        create_table_sql = str(
            CreateTable(PaperSyncLink.__table__).compile(dialect=self._engine.dialect)
        ).replace(
            f"CREATE TABLE {PaperSyncLink.__tablename__}",
            f'CREATE TABLE "{temporary_table}"',
            1,
        )
        logger.info(
            "Rebuilding paper_sync_links table to remove legacy portfolio_positions FK."
        )
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            connection.execute(text(create_table_sql))
            connection.execute(
                text(
                    f'INSERT INTO "{temporary_table}" ({column_clause}) '
                    f'SELECT {select_clause} FROM "{PaperSyncLink.__tablename__}"'
                )
            )
            connection.execute(text(f'DROP TABLE "{PaperSyncLink.__tablename__}"'))
            connection.execute(
                text(
                    f'ALTER TABLE "{temporary_table}" '
                    f'RENAME TO "{PaperSyncLink.__tablename__}"'
                )
            )
            for index in PaperSyncLink.__table__.indexes:
                index.create(connection, checkfirst=True)

    def _ensure_paper_trading_observation_schema(self) -> None:
        """Idempotently add hybrid paper-monitor tables/columns to local SQLite."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(PaperExecutionPlan.__tablename__):
            return
        Base.metadata.create_all(self._engine, tables=[PaperPlanObservation.__table__])

    def _ensure_paper_trading_capital_schema(self) -> None:
        """Idempotently add paper capital/dispatch tables and nullable legacy columns."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        Base.metadata.create_all(
            self._engine,
            tables=[
                PaperCapitalAccount.__table__,
                PaperCapitalAdjustment.__table__,
                PaperSyncLink.__table__,
                PaperNotificationDispatch.__table__,
                RuntimeSchedulerHeartbeat.__table__,
                RuntimeSchedulerLease.__table__,
                ReportPlanReconciliation.__table__,
                DailyMarketRegime.__table__,
                PaperReanalysisRequest.__table__,
                PaperResolution.__table__,
                PaperRiskRevision.__table__,
                PaperStrategyThesis.__table__,
                PaperStrategyTransition.__table__,
                PaperSignalConsumption.__table__,
                PaperAccountRiskSnapshot.__table__,
                PaperAccountRiskEvent.__table__,
            ],
        )
        table_columns = {
            PaperPlanObservation.__tablename__: {
                "resolution_key": "VARCHAR(220)",
                "resolution_bucket_json": "TEXT",
                "logical_order_key": "VARCHAR(220)",
            },
            PaperCapitalAccount.__tablename__: {
                "external_cash_flow": "FLOAT NOT NULL DEFAULT 0",
            },
            PaperExecutionPlan.__tablename__: {
                "valid_for_date": "DATE",
                "plan_allocation_budget": "FLOAT",
                "plan_risk_budget": "FLOAT",
                "budget_snapshot_equity": "FLOAT",
                "budget_snapshot_at": "DATETIME",
                "trading_style": "VARCHAR(32)",
                "trading_style_policy_version": "VARCHAR(32)",
                "trading_style_settings_json": "TEXT",
                "market_regime_context_id": "INTEGER",
                "market_regime": "VARCHAR(32)",
                "market_regime_policy_version": "VARCHAR(32)",
            },
            PaperPosition.__tablename__: {
                "entry_leg_id": "VARCHAR(64) NOT NULL DEFAULT 'legacy_entry'",
                "execution_index": "INTEGER NOT NULL DEFAULT 1",
                "opened_at": "DATETIME",
                "capital_account_id": "INTEGER",
                "currency": "VARCHAR(8)",
                "allocated_capital": "FLOAT",
                "initial_quantity": "FLOAT",
                "initial_allocated_capital": "FLOAT",
                "remaining_cost": "FLOAT",
                "realized_quantity": "FLOAT NOT NULL DEFAULT 0",
                "realized_proceeds": "FLOAT NOT NULL DEFAULT 0",
                "realized_fees": "FLOAT NOT NULL DEFAULT 0",
                "realized_pnl": "FLOAT NOT NULL DEFAULT 0",
                "lot_size": "INTEGER",
                "risk_amount": "FLOAT",
                "risk_per_share": "FLOAT",
                "sizing_reason": "VARCHAR(64)",
                "legacy_fixed_notional": "BOOLEAN NOT NULL DEFAULT 1",
                "trading_style": "VARCHAR(32)",
                "trading_style_policy_version": "VARCHAR(32)",
                "trading_style_settings_json": "TEXT",
                "market_regime_context_id": "INTEGER",
                "market_regime": "VARCHAR(32)",
                "market_regime_policy_version": "VARCHAR(32)",
                "sync_origin": "VARCHAR(32) NOT NULL DEFAULT 'paper_generated'",
                "sync_status": "VARCHAR(32) NOT NULL DEFAULT 'paper_only'",
                "source_portfolio_position_id": "INTEGER",
                "last_synced_at": "DATETIME",
                "available_quantity": "FLOAT",
                "frozen_quantity": "FLOAT",
                "pending_exit_reason": "VARCHAR(64)",
                "pending_exit_on": "DATE",
                "pending_exit_price": "FLOAT",
                "pending_exit_event_id": "INTEGER",
                "sellability_rule": "VARCHAR(32) NOT NULL DEFAULT 'unknown_t1_fail_closed'",
            },
            PaperTradeEvent.__tablename__: {
                "capital_account_id": "INTEGER",
                "currency": "VARCHAR(8)",
                "allocated_capital": "FLOAT",
                "quantity": "FLOAT",
                "requested_quantity": "FLOAT",
                "executed_quantity": "FLOAT",
                "unfilled_quantity": "FLOAT",
                "gross_amount": "FLOAT",
                "commission": "FLOAT",
                "tax": "FLOAT",
                "slippage_amount": "FLOAT",
                "fees": "FLOAT",
                "net_amount": "FLOAT",
                "gross_realized_pnl": "FLOAT",
                "net_realized_pnl": "FLOAT",
                "fill_price": "FLOAT",
                "fill_evidence_grade": "VARCHAR(32)",
                "resolution_key": "VARCHAR(220)",
                "logical_order_key": "VARCHAR(220)",
                "instrument_assumption": "VARCHAR(64)",
                "quote_provider_timestamp": "VARCHAR(64)",
                "quote_fetched_at": "VARCHAR(64)",
                "cash_after": "FLOAT",
                "exposure_after": "FLOAT",
                "realized_pnl_after": "FLOAT",
            },
        }
        for table_name, columns in table_columns.items():
            if not inspector.has_table(table_name):
                continue
            existing = {column["name"] for column in inspector.get_columns(table_name)}
            for column_name, column_sql in columns.items():
                if column_name in existing:
                    continue
                try:
                    with self._engine.begin() as connection:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"
                        )
                except OperationalError as exc:
                    if not self._is_sqlite_duplicate_column_error(exc, column_name):
                        raise
        if inspector.has_table(PaperPosition.__tablename__):
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        "UPDATE paper_positions "
                        "SET initial_quantity = COALESCE(initial_quantity, quantity), "
                        "initial_allocated_capital = COALESCE(initial_allocated_capital, allocated_capital, normalized_notional, quantity * opened_price), "
                        "remaining_cost = COALESCE(remaining_cost, allocated_capital, normalized_notional, quantity * opened_price), "
                        "realized_quantity = COALESCE(realized_quantity, 0), "
                        "realized_proceeds = COALESCE(realized_proceeds, 0), "
                        "realized_fees = COALESCE(realized_fees, 0), "
                        "realized_pnl = CASE "
                        "WHEN COALESCE(realized_pnl, 0) = 0 AND simulated_pnl IS NOT NULL THEN simulated_pnl "
                        "ELSE COALESCE(realized_pnl, 0) END"
                    )
            except OperationalError:
                logger.debug("paper_positions partial-ledger backfill skipped; columns not ready", exc_info=True)
        if inspector.has_table(PaperTradeEvent.__tablename__):
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        "UPDATE paper_trade_events "
                        "SET fill_price = COALESCE(fill_price, price), "
                        "fill_evidence_grade = COALESCE(fill_evidence_grade, 'legacy_quote_based'), "
                        "gross_amount = COALESCE(gross_amount, allocated_capital), "
                        "fees = COALESCE(fees, 0), "
                        "net_amount = COALESCE(net_amount, allocated_capital)"
                    )
            except OperationalError:
                logger.debug("paper_trade_events fill-evidence backfill skipped; columns not ready", exc_info=True)

    def _ensure_paper_positions_multi_lot_schema(self) -> None:
        """Allow one plan to own multiple explicit entry-leg lots in SQLite."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(PaperPosition.__tablename__):
            return
        columns = {column["name"] for column in inspector.get_columns(PaperPosition.__tablename__)}
        if "entry_leg_id" not in columns:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        "ALTER TABLE paper_positions "
                        "ADD COLUMN entry_leg_id VARCHAR(64) NOT NULL DEFAULT 'legacy_entry'"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "entry_leg_id"):
                    raise
        if "execution_index" not in columns:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        "ALTER TABLE paper_positions "
                        "ADD COLUMN execution_index INTEGER NOT NULL DEFAULT 1"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "execution_index"):
                    raise
        if "opened_at" not in columns:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        "ALTER TABLE paper_positions ADD COLUMN opened_at DATETIME"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "opened_at"):
                    raise
        refreshed_columns = {
            column["name"]
            for column in inspect(self._engine).get_columns(PaperPosition.__tablename__)
        }
        if {"opened_at", "opened_on", "market"}.issubset(refreshed_columns):
            session = self._SessionLocal()
            try:
                legacy_rows = list(session.execute(
                    select(PaperPosition)
                    .where(
                        PaperPosition.opened_at.is_(None),
                        PaperPosition.opened_on.is_not(None),
                    )
                ).scalars().all())
                for row in legacy_rows:
                    row.opened_at = self._legacy_paper_opened_at_utc(row.market, row.opened_on)
                    row.updated_at = utc_naive_now()
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        if not {"plan_id", "entry_leg_id", "execution_index"}.issubset(refreshed_columns):
            return
        try:
            with self._engine.connect() as connection:
                indexes = connection.execute(
                    text("PRAGMA index_list(paper_positions)")
                ).fetchall()
                unique_plan_index_names = []
                for index_row in indexes:
                    index_name = index_row[1]
                    is_unique = bool(index_row[2])
                    if not is_unique:
                        continue
                    index_columns = connection.execute(
                        text(f'PRAGMA index_info("{index_name}")')
                    ).fetchall()
                    indexed_names = [row[2] for row in index_columns]
                    if indexed_names == ["plan_id"]:
                        unique_plan_index_names.append(index_name)
        except Exception as exc:
            logger.warning(
                "Failed to inspect paper_positions unique plan index; skip multi-lot repair: %s",
                exc,
            )
            return
        if not unique_plan_index_names:
            for index in PaperPosition.__table__.indexes:
                index.create(self._engine, checkfirst=True)
            return
        self._rebuild_paper_positions_table_without_unique_plan_id()

    def _rebuild_paper_positions_table_without_unique_plan_id(self) -> None:
        temporary_table = f"paper_positions_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        target_columns = [column.name for column in PaperPosition.__table__.columns]

        inspector = inspect(self._engine)
        existing_columns = {
            column["name"]
            for column in inspector.get_columns(PaperPosition.__tablename__)
        }
        copy_columns = [column for column in target_columns if column in existing_columns]
        if not copy_columns:
            logger.warning("paper_positions multi-lot repair skipped because no shared columns were found.")
            return

        column_clause = ", ".join(f'"{column}"' for column in copy_columns)
        select_expressions = []
        for column in copy_columns:
            if column in {"created_at", "updated_at"}:
                select_expressions.append(f'COALESCE("{column}", CURRENT_TIMESTAMP) AS "{column}"')
            elif column == "entry_leg_id":
                select_expressions.append(f'COALESCE("{column}", \'legacy_entry\') AS "{column}"')
            elif column == "execution_index":
                select_expressions.append(f'COALESCE("{column}", 1) AS "{column}"')
            elif column == "status":
                select_expressions.append(f'COALESCE("{column}", \'open\') AS "{column}"')
            elif column == "normalized_notional":
                select_expressions.append(f'COALESCE("{column}", 100000.0) AS "{column}"')
            elif column == "legacy_fixed_notional":
                select_expressions.append(f'COALESCE("{column}", 1) AS "{column}"')
            elif column == "sync_origin":
                select_expressions.append(f'COALESCE("{column}", \'paper_generated\') AS "{column}"')
            elif column == "sync_status":
                select_expressions.append(f'COALESCE("{column}", \'paper_only\') AS "{column}"')
            elif column == "sellability_rule":
                select_expressions.append(f'COALESCE("{column}", \'unknown_t1_fail_closed\') AS "{column}"')
            elif column == "max_holding_sessions":
                select_expressions.append(f'COALESCE("{column}", 10) AS "{column}"')
            else:
                select_expressions.append(f'"{column}"')
        select_clause = ", ".join(select_expressions)
        create_table_sql = str(
            CreateTable(PaperPosition.__table__).compile(dialect=self._engine.dialect)
        ).replace(
            f"CREATE TABLE {PaperPosition.__tablename__}",
            f'CREATE TABLE "{temporary_table}"',
            1,
        )
        logger.info("Rebuilding paper_positions table to allow multiple lots per plan.")
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            connection.execute(text(create_table_sql))
            connection.execute(
                text(
                    f'INSERT INTO "{temporary_table}" ({column_clause}) '
                    f'SELECT {select_clause} FROM "{PaperPosition.__tablename__}"'
                )
            )
            connection.execute(text(f'DROP TABLE "{PaperPosition.__tablename__}"'))
            connection.execute(
                text(
                    f'ALTER TABLE "{temporary_table}" '
                    f'RENAME TO "{PaperPosition.__tablename__}"'
                )
            )
            for index in PaperPosition.__table__.indexes:
                index.create(connection, checkfirst=True)

    def _ensure_paper_evaluations_position_leg_schema(self) -> None:
        """Migrate EOD evaluations from plan/date uniqueness to lot-aware keys."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(PaperPlanEvaluation.__tablename__):
            return
        existing = {column["name"] for column in inspector.get_columns(PaperPlanEvaluation.__tablename__)}
        for column_name, column_sql in {
            "evaluation_key": "VARCHAR(220) NOT NULL DEFAULT ''",
            "position_id": "INTEGER",
            "entry_leg_id": "VARCHAR(64) NOT NULL DEFAULT 'legacy_entry'",
        }.items():
            if column_name in existing:
                continue
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {PaperPlanEvaluation.__tablename__} "
                        f"ADD COLUMN {column_name} {column_sql}"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, column_name):
                    raise
        with self._engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE paper_plan_evaluations "
                "SET entry_leg_id = COALESCE(NULLIF(entry_leg_id, ''), 'legacy_entry')"
            )
            connection.exec_driver_sql(
                "UPDATE paper_plan_evaluations "
                "SET evaluation_key = 'plan:' || plan_id || ':leg:' || "
                "COALESCE(NULLIF(entry_leg_id, ''), 'legacy_entry') || ':pos:' || "
                "COALESCE(position_id, 0) || ':eod:' || evaluation_date "
                "WHERE evaluation_key IS NULL OR evaluation_key = ''"
            )
        try:
            with self._engine.connect() as connection:
                indexes = connection.execute(
                    text("PRAGMA index_list(paper_plan_evaluations)")
                ).fetchall()
                has_legacy_unique = False
                has_evaluation_key_unique = False
                for index_row in indexes:
                    index_name = index_row[1]
                    is_unique = bool(index_row[2])
                    index_columns = connection.execute(
                        text(f'PRAGMA index_info("{index_name}")')
                    ).fetchall()
                    names = [row[2] for row in index_columns]
                    if is_unique and names == ["plan_id", "evaluation_date"]:
                        has_legacy_unique = True
                    if is_unique and names == ["evaluation_key"]:
                        has_evaluation_key_unique = True
        except Exception as exc:
            logger.warning("Failed to inspect paper_plan_evaluations indexes; skip lot-aware repair: %s", exc)
            return
        if has_legacy_unique or not has_evaluation_key_unique:
            self._rebuild_paper_evaluations_table_with_lot_keys()

    def _rebuild_paper_evaluations_table_with_lot_keys(self) -> None:
        temporary_table = f"paper_plan_evaluations_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        target_columns = [column.name for column in PaperPlanEvaluation.__table__.columns]
        inspector = inspect(self._engine)
        existing_columns = {
            column["name"]
            for column in inspector.get_columns(PaperPlanEvaluation.__tablename__)
        }
        copy_columns = [column for column in target_columns if column in existing_columns]
        if not copy_columns:
            logger.warning("paper_plan_evaluations repair skipped because no shared columns were found.")
            return
        column_clause = ", ".join(f'"{column}"' for column in copy_columns)
        select_clause = ", ".join(
            f'COALESCE("{column}", CURRENT_TIMESTAMP) AS "{column}"'
            if column == "created_at"
            else f'COALESCE(NULLIF("{column}", \'\'), \'legacy_entry\') AS "{column}"'
            if column == "entry_leg_id"
            else f'"{column}"'
            for column in copy_columns
        )
        create_table_sql = str(
            CreateTable(PaperPlanEvaluation.__table__).compile(dialect=self._engine.dialect)
        ).replace(
            f"CREATE TABLE {PaperPlanEvaluation.__tablename__}",
            f'CREATE TABLE "{temporary_table}"',
            1,
        )
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            connection.execute(text(create_table_sql))
            connection.execute(
                text(
                    f'INSERT OR IGNORE INTO "{temporary_table}" ({column_clause}) '
                    f'SELECT {select_clause} FROM "{PaperPlanEvaluation.__tablename__}"'
                )
            )
            connection.execute(text(f'DROP TABLE "{PaperPlanEvaluation.__tablename__}"'))
            connection.execute(
                text(
                    f'ALTER TABLE "{temporary_table}" '
                    f'RENAME TO "{PaperPlanEvaluation.__tablename__}"'
                )
            )
            for index in PaperPlanEvaluation.__table__.indexes:
                index.create(connection, checkfirst=True)

    def _ensure_schema_migration_record(self) -> None:
        session = self._SessionLocal()
        values = {
            "version": CURRENT_SCHEMA_VERSION,
            "description": "Baseline schema created through SQLAlchemy metadata.create_all",
        }
        try:
            if self._is_sqlite_engine:
                statement = sqlite_insert(DatabaseSchemaMigration).values(**values)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                session.execute(statement)
            elif self._dialect_name == "postgresql":
                statement = postgresql_insert(DatabaseSchemaMigration).values(**values)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                session.execute(statement)
            else:
                session.execute(DatabaseSchemaMigration.__table__.insert().values(**values))
            session.commit()
        except IntegrityError:
            session.rollback()
            with self._SessionLocal() as verify_session:
                existing = verify_session.get(DatabaseSchemaMigration, CURRENT_SCHEMA_VERSION)
            if existing is None:
                raise
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def _ensure_report_execution_intent_baseline(self) -> None:
        """Give reports that predate execution intents one explicit legacy row.

        The marker and the backfill commit together, so the baseline runs once
        per database. Reports saved later without an intent stay unauthorized
        instead of being promoted by a restart.
        """

        marker = {
            "version": REPORT_EXECUTION_INTENT_BASELINE_VERSION,
            "description": "Legacy stock reports keep primary paper execution",
        }
        # Partial legacy schemas may lack the market-review markers; paper
        # execution still rejects market reviews before reading intent.
        history_columns = {
            column["name"]
            for column in inspect(self._engine).get_columns(AnalysisHistory.__tablename__)
        }
        legacy_filters = [
            ~exists().where(
                AnalysisReportExecutionIntent.analysis_history_id == AnalysisHistory.id
            ),
        ]
        if "code" in history_columns:
            legacy_filters.append(AnalysisHistory.code != "MARKET")
        if "report_type" in history_columns:
            legacy_filters.append(or_(
                AnalysisHistory.report_type.is_(None),
                AnalysisHistory.report_type != "market_review",
            ))
        with self._engine.begin() as connection:
            if self._is_sqlite_engine:
                statement = sqlite_insert(DatabaseSchemaMigration).values(**marker)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                claimed = bool(connection.execute(statement).rowcount)
            elif self._dialect_name == "postgresql":
                # psycopg 3 reports rowcount=-1 for this INSERT, so only
                # RETURNING proves that this transaction claimed the marker.
                statement = postgresql_insert(DatabaseSchemaMigration).values(**marker)
                statement = statement.on_conflict_do_nothing(index_elements=["version"])
                claimed = connection.execute(
                    statement.returning(DatabaseSchemaMigration.version)
                ).first() is not None
            else:
                claimed = connection.execute(
                    select(DatabaseSchemaMigration.version).where(
                        DatabaseSchemaMigration.version == marker["version"]
                    )
                ).first() is None
                if claimed:
                    connection.execute(DatabaseSchemaMigration.__table__.insert().values(**marker))
            if not claimed:
                return
            legacy_reports = select(
                AnalysisHistory.id,
                literal(REPORT_EXECUTION_INTENT_PAPER_CANDIDATE),
                literal(REPORT_EXECUTION_SCOPE_PRIMARY),
                literal(REPORT_EXECUTION_INTENT_BASELINE_VERSION),
                literal("legacy_baseline"),
                literal(utc_naive_now()),
            ).where(*legacy_filters)
            inserted = connection.execute(
                AnalysisReportExecutionIntent.__table__.insert().from_select(
                    [
                        "analysis_history_id",
                        "execution_intent",
                        "execution_scope",
                        "execution_policy_version",
                        "intent_source",
                        "created_at",
                    ],
                    legacy_reports,
                ),
                execution_options={"preserve_rowcount": True},
            ).rowcount
        logger.info("报告执行用途 legacy 基线已建立: %s 条既有个股报告", inserted)

    def _ensure_decision_signal_profile_schema(self) -> None:
        """Add and backfill nullable decision_profile for existing SQLite DBs."""

        if not self._is_sqlite_engine:
            return
        inspector = inspect(self._engine)
        if not inspector.has_table(DecisionSignalRecord.__tablename__):
            return

        try:
            existing = {
                column["name"]
                for column in inspector.get_columns(DecisionSignalRecord.__tablename__)
            }
        except Exception as exc:
            logger.error(
                "[DecisionSignal] failed to inspect decision_profile column; "
                "profile migration cannot continue safely: %s",
                exc,
            )
            raise

        if "decision_profile" not in existing:
            try:
                with self._engine.begin() as connection:
                    connection.exec_driver_sql(
                        f"ALTER TABLE {DecisionSignalRecord.__tablename__} "
                        "ADD COLUMN decision_profile VARCHAR(16)"
                    )
            except OperationalError as exc:
                if not self._is_sqlite_duplicate_column_error(exc, "decision_profile"):
                    raise

        self._ensure_decision_signal_profile_indexes()
        self._backfill_decision_signal_profile_from_metadata()

    def _ensure_decision_signal_profile_indexes(self) -> None:
        """Create profile-aware indexes without dropping legacy indexes."""

        expected_indexes = {
            "ix_decision_signals_decision_profile": ["decision_profile"],
            "ix_decision_signal_market_stock_profile_created": [
                "market", "stock_code", "decision_profile", "created_at",
            ],
            "ix_decision_signal_report_type_market_stock_profile_action_horizon_phase": [
                "source_report_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
            "ix_decision_signal_trace_type_market_stock_profile_action_horizon_phase": [
                "trace_id", "source_type", "market", "stock_code",
                "decision_profile", "action", "horizon", "market_phase",
            ],
        }
        with self._engine.begin() as connection:
            for index_name, columns in expected_indexes.items():
                connection.exec_driver_sql(
                    f"CREATE INDEX IF NOT EXISTS {index_name} "
                    f"ON decision_signals ({', '.join(columns)})"
                )

        actual_indexes = {
            index["name"]: index["column_names"]
            for index in inspect(self._engine).get_indexes(
                DecisionSignalRecord.__tablename__
            )
        }
        for index_name, expected_columns in expected_indexes.items():
            if actual_indexes.get(index_name) != expected_columns:
                raise RuntimeError(
                    "decision_profile index verification failed: "
                    f"index={index_name} expected={expected_columns} "
                    f"actual={actual_indexes.get(index_name)}"
                )

    def _backfill_decision_signal_profile_from_metadata(self) -> None:
        stats = {
            "candidate_count": 0,
            "backfilled_count": 0,
            "guard_skipped_count": 0,
            "missing_metadata_count": 0,
            "missing_profile_count": 0,
            "invalid_json_count": 0,
            "non_object_count": 0,
            "invalid_profile_count": 0,
            "skipped_existing_profile_count": 0,
        }
        with self._engine.begin() as connection:
            stats["skipped_existing_profile_count"] = connection.execute(
                text(
                    "SELECT COUNT(*) FROM decision_signals "
                    "WHERE decision_profile IS NOT NULL"
                )
            ).scalar_one()
            candidate_rows = [
                (row["id"], row["metadata_json"])
                for row in connection.execute(
                    text(
                        "SELECT id, metadata_json FROM decision_signals "
                        "WHERE decision_profile IS NULL ORDER BY id"
                    )
                ).mappings()
            ]
            stats["candidate_count"] = len(candidate_rows)

            for signal_id, metadata_json in candidate_rows:
                if metadata_json is None:
                    stats["missing_metadata_count"] += 1
                    continue
                try:
                    metadata = json.loads(metadata_json)
                except (TypeError, ValueError, RecursionError):
                    stats["invalid_json_count"] += 1
                    continue
                if not isinstance(metadata, dict):
                    stats["non_object_count"] += 1
                    continue

                raw_profile = metadata.get("decision_profile")
                if raw_profile is None or (
                    isinstance(raw_profile, str) and not raw_profile.strip()
                ):
                    stats["missing_profile_count"] += 1
                    continue
                profile = extract_legacy_decision_profile(metadata)
                if profile is None:
                    stats["invalid_profile_count"] += 1
                    continue

                result = connection.execute(
                    text(
                        "UPDATE decision_signals "
                        "SET decision_profile = :decision_profile "
                        "WHERE id = :signal_id AND decision_profile IS NULL"
                    ),
                    {"decision_profile": profile, "signal_id": signal_id},
                )
                if result.rowcount == 1:
                    stats["backfilled_count"] += 1
                elif result.rowcount == 0:
                    stats["guard_skipped_count"] += 1
                else:
                    raise RuntimeError(
                        "decision_profile backfill updated an unexpected number "
                        f"of rows for signal_id={signal_id}: {result.rowcount}"
                    )

            classified_count = sum(
                stats[key]
                for key in (
                    "backfilled_count",
                    "guard_skipped_count",
                    "missing_metadata_count",
                    "missing_profile_count",
                    "invalid_json_count",
                    "non_object_count",
                    "invalid_profile_count",
                )
            )
            if classified_count != stats["candidate_count"]:
                raise RuntimeError(
                    "decision_profile migration stats did not classify every "
                    f"candidate: candidates={stats['candidate_count']} "
                    f"classified={classified_count}"
                )
        logger.info(
            "[DecisionSignal] decision_profile migration stats: "
            "candidate_count=%s backfilled_count=%s guard_skipped_count=%s "
            "missing_metadata_count=%s missing_profile_count=%s "
            "invalid_json_count=%s non_object_count=%s invalid_profile_count=%s "
            "skipped_existing_profile_count=%s",
            stats["candidate_count"],
            stats["backfilled_count"],
            stats["guard_skipped_count"],
            stats["missing_metadata_count"],
            stats["missing_profile_count"],
            stats["invalid_json_count"],
            stats["non_object_count"],
            stats["invalid_profile_count"],
            stats["skipped_existing_profile_count"],
        )

    def _ensure_intelligence_items_unique_index(self) -> None:
        if not self._is_sqlite_engine:
            return

        if not inspect(self._engine).has_table("intelligence_items"):
            return

        try:
            unique_indexes = self._list_sqlite_unique_indexes("intelligence_items")
        except Exception as exc:
            logger.warning(
                "[Intelligence items] failed to inspect unique indexes; "
                "skip migration/repair: %s",
                exc,
            )
            return

        target_columns = ("source_id", "url", "scope_type", "scope_value", "market")
        has_target_index = any(tuple(cols) == target_columns for cols in unique_indexes)
        has_legacy_url_unique = any(tuple(cols) == ("url",) for cols in unique_indexes)

        if has_target_index:
            return
        if unique_indexes and not has_legacy_url_unique:
            # Table has other unique index shapes; avoid aggressive changes and add
            # the expected scoped uniqueness directly.
            self._ensure_intelligence_items_scoped_unique_index_once()
            return

        self._rebuild_intelligence_items_table()

    def _rebuild_intelligence_items_table(self) -> None:
        temporary_table = f"intelligence_items_recreate_tmp_{int(time.time() * 1_000_000_000)}"
        columns = [column.name for column in IntelligenceItem.__table__.columns]
        select_clause = ", ".join(f'"{column}"' for column in columns)
        scoped_index_columns = ", ".join(["source_id", "url", "scope_type", "scope_value", "market"])
        scoped_index_name = "uix_intel_item_scope"

        tmp_metadata = MetaData()
        tmp_table = Table(
            temporary_table,
            tmp_metadata,
            *(column.copy() for column in IntelligenceItem.__table__.columns),
        )
        logger.info("Rebuilding intelligence_items table to align composite uniqueness constraints.")
        with self._engine.begin() as connection:
            connection.execute(text(f'DROP TABLE IF EXISTS "{temporary_table}"'))
            tmp_table.create(connection)
            connection.execute(
                text(
                    f"INSERT INTO \"{temporary_table}\" ({select_clause}) "
                    f"SELECT {select_clause} FROM intelligence_items"
                )
            )
            connection.execute(text('DROP TABLE "intelligence_items"'))
            connection.execute(
                text(f'ALTER TABLE "{temporary_table}" RENAME TO intelligence_items')
            )
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {scoped_index_name} ON "
                    f"intelligence_items ({scoped_index_columns})"
                )
            )

    def _ensure_intelligence_items_scoped_unique_index_once(self) -> None:
        target_index_name = "uix_intel_item_scope"
        with self._engine.begin() as connection:
            rows = connection.execute(
                text("PRAGMA index_list(intelligence_items)")
            ).fetchall()
            for row in rows:
                if row[1] == target_index_name:
                    return
            index_columns = ", ".join(["source_id", "url", "scope_type", "scope_value", "market"])
            connection.execute(
                text(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {target_index_name} ON "
                    f"intelligence_items ({index_columns})"
                )
            )

    def _list_sqlite_unique_indexes(self, table_name: str):
        with self._engine.connect() as connection:
            rows = connection.execute(
                text(f"PRAGMA index_list({table_name})")
            ).fetchall()
            unique_indexes = []
            for row in rows:
                # row: (seq, name, unique, origin, partial)
                if int(row[2]) != 1:
                    continue
                index_name = row[1]
                index_columns = []
                for index_info in connection.execute(
                    text(f"PRAGMA index_xinfo({index_name})")
                ).fetchall():
                    # index_xinfo: (seqno, cid, name, desc, coll, key, ... )
                    column_name = index_info[2]
                    if column_name is None:
                        continue
                    index_columns.append(column_name)
                unique_indexes.append(index_columns)
            return unique_indexes

    def _ensure_llm_usage_telemetry_columns(self) -> None:
        """Add nullable P0a usage telemetry columns to existing SQLite DBs."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(LLMUsage.__tablename__)
            }
        except Exception as exc:
            logger.warning(
                "[LLM usage] failed to inspect telemetry columns; "
                "skipping best-effort SQLite telemetry column backfill: %s",
                exc,
            )
            return

        max_retries = self._sqlite_write_retry_max
        for column, column_type in _LLM_USAGE_TELEMETRY_COLUMN_SQL.items():
            if column in existing:
                continue
            for attempt in range(max_retries + 1):
                try:
                    with self._engine.begin() as connection:
                        connection.exec_driver_sql(
                            f"ALTER TABLE {LLMUsage.__tablename__} "
                            f"ADD COLUMN {column} {column_type}"
                        )
                    existing.add(column)
                    break
                except OperationalError as exc:
                    if self._is_sqlite_duplicate_column_error(exc, column):
                        existing.add(column)
                        break
                    if self._is_sqlite_locked_error(exc) and attempt < max_retries:
                        delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                        logger.warning(
                            "[LLM usage] SQLite telemetry column backfill locked, "
                            "retrying: %s (%s/%s, %.2fs)",
                            column,
                            attempt + 1,
                            max_retries,
                            delay,
                        )
                        if delay > 0:
                            time.sleep(delay)
                        continue
                    raise

    def _ensure_intelligence_item_scope_values(self) -> None:
        """Backfill nullable intelligence item scopes so SQLite unique keys work."""
        if not self._is_sqlite_engine:
            return
        try:
            existing = {
                column["name"]
                for column in inspect(self._engine).get_columns(IntelligenceItem.__tablename__)
            }
        except Exception as exc:
            logger.warning("资讯池 scope_value 回填检查失败，已跳过: %s", exc)
            return
        if "scope_value" not in existing:
            return
        try:
            with self._engine.begin() as connection:
                connection.exec_driver_sql(
                    f"UPDATE {IntelligenceItem.__tablename__} "
                    "SET scope_value = ? "
                    "WHERE scope_value IS NULL OR scope_value = ''",
                    (INTELLIGENCE_ITEM_NULL_SCOPE_VALUE,),
                )
        except Exception as exc:
            logger.warning("资讯池 scope_value 回填失败，已跳过: %s", exc)

    @classmethod
    def get_instance(cls) -> 'DatabaseManager':
        """获取单例实例"""
        with cls._init_lock:
            if cls._instance is None:
                cls()
            return cls._instance
    
    @classmethod
    def reset_instance(cls) -> None:
        """重置单例（用于测试）"""
        with cls._init_lock:
            if cls._instance is not None:
                if hasattr(cls._instance, '_engine') and cls._instance._engine is not None:
                    cls._instance._engine.dispose()
                cls._instance._initialized = False
                cls._instance = None

    @classmethod
    def create_scoped_readonly(cls, db_url: str) -> 'DatabaseManager':
        """构建一个绑定到其它数据库的隔离只读管理器，用于跨范围读取。

        用于 Web/API 进程读取实验组（``experiment:<id>:<arm>``）隔离数据库中的
        账本，而**不触碰进程主库单例**：它既不写入 ``cls._instance``，失败时也不
        清空主单例；目标库必须已由工作进程建好，本工厂不建表、不迁移 schema。
        只提供读取所需的 engine/session；使用完须调用 :meth:`dispose`。
        """

        if not db_url or not str(db_url).strip():
            raise ValueError("create_scoped_readonly requires a database URL")

        instance = object.__new__(cls)
        instance._initialized = False
        instance._scoped_readonly = True

        config = get_config()
        instance._db_url = db_url
        instance._sqlite_wal_enabled = config.sqlite_wal_enabled
        instance._sqlite_busy_timeout_ms = config.sqlite_busy_timeout_ms
        instance._sqlite_write_retry_max = config.sqlite_write_retry_max
        instance._sqlite_write_retry_base_delay = config.sqlite_write_retry_base_delay

        engine_kwargs = {"echo": False, "pool_pre_ping": True}
        if str(db_url).startswith("sqlite:") and instance._sqlite_busy_timeout_ms > 0:
            engine_kwargs["connect_args"] = {"timeout": instance._sqlite_busy_timeout_ms / 1000}

        created_engine = create_engine(db_url, **engine_kwargs)
        try:
            instance._engine = created_engine
            instance._dialect_name = created_engine.url.get_backend_name()
            instance._is_sqlite_engine = instance._dialect_name == 'sqlite'
            instance._sqlite_file_db = instance._is_sqlite_engine and instance._is_file_sqlite_database()
            instance._install_sqlite_pragma_handler()
            instance._SessionLocal = sessionmaker(
                bind=created_engine, autocommit=False, autoflush=False,
            )
            instance._initialized = True
            atexit.register(DatabaseManager._cleanup_engine, created_engine)
        except Exception:
            try:
                created_engine.dispose()
            except Exception as cleanup_exc:
                logger.warning("跨范围只读引擎初始化失败后的清理也失败: %s", cleanup_exc)
            instance._engine = None
            instance._SessionLocal = None
            instance._initialized = False
            raise
        return instance

    def dispose(self) -> None:
        """释放隔离只读管理器的引擎；不用于释放进程主单例。"""
        if not getattr(self, "_scoped_readonly", False):
            return
        engine = getattr(self, "_engine", None)
        if engine is not None:
            engine.dispose()
        self._engine = None
        self._SessionLocal = None
        self._initialized = False

    @classmethod
    def _cleanup_engine(cls, engine) -> None:
        """
        清理数据库引擎（atexit 钩子）

        确保程序退出时关闭所有数据库连接，避免 ResourceWarning

        Args:
            engine: SQLAlchemy 引擎对象
        """
        try:
            if engine is not None:
                engine.dispose()
                logger.debug("数据库引擎已清理")
        except Exception as e:
            logger.warning(f"清理数据库引擎时出错: {e}")

    def _install_sqlite_pragma_handler(self) -> None:
        """为 SQLite 连接安装竞争保护参数。"""
        if not self._is_sqlite_engine:
            return

        @event.listens_for(self._engine, "connect")
        def _configure_sqlite_connection(dbapi_connection, _connection_record) -> None:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(f"PRAGMA busy_timeout={int(self._sqlite_busy_timeout_ms)}")
                if self._sqlite_file_db and self._sqlite_wal_enabled:
                    cursor.execute("PRAGMA journal_mode=WAL")
            except Exception as exc:
                logger.warning("初始化 SQLite PRAGMA 失败: %s", exc)
            finally:
                cursor.close()

    def _is_file_sqlite_database(self) -> bool:
        database = (self._engine.url.database or "").strip()
        return bool(database) and database.lower() != ":memory:"

    def _run_write_transaction(
        self,
        operation_name: str,
        write_operation: Callable[[Session], T],
    ) -> T:
        max_retries = self._sqlite_write_retry_max if self._is_sqlite_engine else 0

        for attempt in range(max_retries + 1):
            session = self.get_session()
            try:
                if self._is_sqlite_engine:
                    # Acquire the SQLite writer lock before any reads inside
                    # `write_operation()` so pre-write existence checks and the
                    # later upsert share one consistent write window.
                    session.connection().exec_driver_sql("BEGIN IMMEDIATE")
                result = write_operation(session)
                session.commit()
                return result
            except OperationalError as exc:
                session.rollback()
                if (
                    self._is_sqlite_engine
                    and self._is_sqlite_locked_error(exc)
                    and attempt < max_retries
                ):
                    delay = self._sqlite_write_retry_base_delay * (2 ** attempt)
                    logger.warning(
                        "SQLite 写入锁冲突，准备重试: %s (%s/%s, %.2fs)",
                        operation_name,
                        attempt + 1,
                        max_retries,
                        delay,
                    )
                    if delay > 0:
                        time.sleep(delay)
                    continue
                raise
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

    @staticmethod
    def _is_sqlite_locked_error(exc: OperationalError) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return any(
            token in err_text
            for token in (
                "database is locked",
                "database schema is locked",
                "database table is locked",
            )
        )

    @staticmethod
    def _is_sqlite_duplicate_column_error(exc: OperationalError, column: str) -> bool:
        err_text = str(getattr(exc, "orig", exc)).lower()
        return "duplicate column name" in err_text and column.lower() in err_text

    @staticmethod
    def _normalize_daily_date(value: Any) -> Any:
        if isinstance(value, str):
            return datetime.strptime(value, '%Y-%m-%d').date()
        if isinstance(value, pd.Timestamp):
            return value.date()
        if isinstance(value, datetime):
            return value.date()
        return value

    @staticmethod
    def _normalize_sql_value(value: Any) -> Any:
        return None if pd.isna(value) else value
    
    def get_session(self) -> Session:
        """
        获取数据库 Session
        
        使用示例:
            with db.get_session() as session:
                # 执行查询
                session.commit()  # 如果需要
        """
        if not getattr(self, '_initialized', False) or not hasattr(self, '_SessionLocal'):
            raise RuntimeError(
                "DatabaseManager 未正确初始化。"
                "请确保通过 DatabaseManager.get_instance() 获取实例。"
            )
        session = self._SessionLocal()
        try:
            return session
        except Exception:
            session.close()
            raise

    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def has_today_data(self, code: str, target_date: Optional[date] = None) -> bool:
        """
        检查是否已有指定日期的数据
        
        用于断点续传逻辑：如果已有数据则跳过网络请求
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            是否存在数据
        """
        if target_date is None:
            target_date = date.today()
        # 注意：这里的 target_date 语义是“自然日”，而不是“最新交易日”。
        # 在周末/节假日/非交易日运行时，即使数据库已有最新交易日数据，这里也会返回 False。
        # 该行为目前保留（按需求不改逻辑）。
        
        with self.get_session() as session:
            result = session.execute(
                select(StockDaily).where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date == target_date
                    )
                )
            ).scalar_one_or_none()
            
            return result is not None
    
    def get_latest_data(
        self, 
        code: str, 
        days: int = 2
    ) -> List[StockDaily]:
        """
        获取最近 N 天的数据
        
        用于计算"相比昨日"的变化
        
        Args:
            code: 股票代码
            days: 获取天数
            
        Returns:
            StockDaily 对象列表（按日期降序）
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(StockDaily.code == code)
                .order_by(desc(StockDaily.date))
                .limit(days)
            ).scalars().all()
            
            return list(results)

    def save_news_intel(
        self,
        code: str,
        name: str,
        dimension: str,
        query: str,
        response: 'SearchResponse',
        query_context: Optional[Dict[str, str]] = None
    ) -> int:
        """
        保存新闻情报到数据库

        去重策略：
        - 优先按 URL 去重（唯一约束）
        - URL 缺失时按 title + source + published_date 进行软去重

        关联策略：
        - query_context 记录用户查询信息（平台、用户、会话、原始指令等）
        """
        if not response or not response.results:
            return 0

        saved_count = 0
        query_ctx = query_context or {}
        current_query_id = (query_ctx.get("query_id") or "").strip()

        def _write(session: Session) -> int:
            local_saved_count = 0

            for item in response.results:
                title = (item.title or '').strip()
                url = (item.url or '').strip()
                source = (item.source or '').strip()
                snippet = (item.snippet or '').strip()
                published_date = self._parse_published_date(item.published_date)

                if not title and not url:
                    continue

                url_key = url or self._build_fallback_url_key(
                    code=code,
                    title=title,
                    source=source,
                    published_date=published_date
                )

                existing = session.execute(
                    select(NewsIntel).where(NewsIntel.url == url_key)
                ).scalar_one_or_none()

                if existing:
                    existing.name = name or existing.name
                    existing.dimension = dimension or existing.dimension
                    existing.query = query or existing.query
                    existing.provider = response.provider or existing.provider
                    existing.snippet = snippet or existing.snippet
                    existing.source = source or existing.source
                    existing.published_date = published_date or existing.published_date
                    existing.fetched_at = datetime.now()

                    if query_context:
                        if not existing.query_id and current_query_id:
                            existing.query_id = current_query_id
                        existing.query_source = (
                            query_context.get("query_source") or existing.query_source
                        )
                        existing.requester_platform = (
                            query_context.get("requester_platform") or existing.requester_platform
                        )
                        existing.requester_user_id = (
                            query_context.get("requester_user_id") or existing.requester_user_id
                        )
                        existing.requester_user_name = (
                            query_context.get("requester_user_name") or existing.requester_user_name
                        )
                        existing.requester_chat_id = (
                            query_context.get("requester_chat_id") or existing.requester_chat_id
                        )
                        existing.requester_message_id = (
                            query_context.get("requester_message_id") or existing.requester_message_id
                        )
                        existing.requester_query = (
                            query_context.get("requester_query") or existing.requester_query
                        )
                    continue

                try:
                    with session.begin_nested():
                        record = NewsIntel(
                            code=code,
                            name=name,
                            dimension=dimension,
                            query=query,
                            provider=response.provider,
                            title=title,
                            snippet=snippet,
                            url=url_key,
                            source=source,
                            published_date=published_date,
                            fetched_at=datetime.now(),
                            query_id=current_query_id or None,
                            query_source=query_ctx.get("query_source"),
                            requester_platform=query_ctx.get("requester_platform"),
                            requester_user_id=query_ctx.get("requester_user_id"),
                            requester_user_name=query_ctx.get("requester_user_name"),
                            requester_chat_id=query_ctx.get("requester_chat_id"),
                            requester_message_id=query_ctx.get("requester_message_id"),
                            requester_query=query_ctx.get("requester_query"),
                        )
                        session.add(record)
                        session.flush()
                    local_saved_count += 1
                except IntegrityError:
                    logger.debug("新闻情报重复（已跳过）: %s %s", code, url_key)

            return local_saved_count

        try:
            saved_count = self._run_write_transaction(
                f"save_news_intel[{code}]",
                _write,
            )
            logger.info(f"保存新闻情报成功: {code}, 新增 {saved_count} 条")
        except Exception as e:
            logger.error(f"保存新闻情报失败: {e}")
            raise

        return saved_count

    def save_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
        payload: Optional[Dict[str, Any]],
        source_chain: Optional[Any] = None,
        coverage: Optional[Any] = None,
    ) -> int:
        """
        保存基本面快照（P0 write-only）。失败不抛异常，返回写入条数 0/1。
        """
        if not query_id or not code or payload is None:
            return 0

        try:
            def _write(session: Session) -> int:
                session.add(
                    FundamentalSnapshot(
                        query_id=query_id,
                        code=code,
                        payload=self._safe_json_dumps(payload),
                        source_chain=self._safe_json_dumps(source_chain or []),
                        coverage=self._safe_json_dumps(coverage or {}),
                    )
                )
                return 1
            return self._run_write_transaction(
                f"save_fundamental_snapshot[{query_id}:{code}]",
                _write,
            )
        except Exception as e:
            logger.debug(
                "基本面快照写入失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_latest_fundamental_snapshot(
        self,
        query_id: str,
        code: str,
    ) -> Optional[Dict[str, Any]]:
        """
        获取指定 query_id + code 的最新基本面快照 payload。

        读取失败或不存在时返回 None（fail-open）。
        """
        if not query_id or not code:
            return None

        with self.get_session() as session:
            try:
                row = session.execute(
                    select(FundamentalSnapshot)
                    .where(
                        and_(
                            FundamentalSnapshot.query_id == query_id,
                            FundamentalSnapshot.code == code,
                        )
                    )
                    .order_by(desc(FundamentalSnapshot.created_at))
                    .limit(1)
                ).scalar_one_or_none()
            except Exception as e:
                logger.debug(
                    "基本面快照读取失败（fail-open）: query_id=%s code=%s err=%s",
                    query_id,
                    code,
                    e,
                )
                return None

            if row is None:
                return None
            try:
                payload = json.loads(row.payload or "{}")
                return payload if isinstance(payload, dict) else None
            except Exception:
                return None

    def get_recent_news(self, code: str, days: int = 7, limit: int = 20) -> List[NewsIntel]:
        """
        获取指定股票最近 N 天的新闻情报
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(
                    and_(
                        NewsIntel.code == code,
                        NewsIntel.fetched_at >= cutoff_date
                    )
                )
                .order_by(desc(NewsIntel.fetched_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_news_intel_by_query_id(self, query_id: str, limit: int = 20) -> List[NewsIntel]:
        """
        根据 query_id 获取新闻情报列表

        Args:
            query_id: 分析记录唯一标识
            limit: 返回数量限制

        Returns:
            NewsIntel 列表（按发布时间或抓取时间倒序）
        """
        from sqlalchemy import func

        with self.get_session() as session:
            results = session.execute(
                select(NewsIntel)
                .where(NewsIntel.query_id == query_id)
                .order_by(
                    desc(func.coalesce(NewsIntel.published_date, NewsIntel.fetched_at)),
                    desc(NewsIntel.fetched_at)
                )
                .limit(limit)
            ).scalars().all()

            return list(results)

    def save_analysis_history(
        self,
        result: Any,
        query_id: str,
        report_type: str,
        news_content: Optional[str],
        context_snapshot: Optional[Dict[str, Any]] = None,
        save_snapshot: bool = True,
        execution_intent: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        保存分析结果历史记录。

        ``execution_intent`` 只能来自后端可信任务上下文，与报告同一事务首次写入且不可修改；
        不提供时报告不获得模拟交易执行权限（大盘复盘等非交易报告无需提供）。

        Returns:
            新保存的 AnalysisHistory.id；保存失败返回 0。
        """
        if result is None:
            return 0
        if getattr(result, "success", True) is False:
            logger.info(
                "跳过失败分析结果历史保存: %s",
                getattr(result, "code", "unknown"),
            )
            return 0

        apply_position_aware_result_contract(result, report_type=report_type)
        sniper_points = self._extract_sniper_points(result)
        raw_result = self._build_raw_result(result)
        context_text = None
        if save_snapshot and context_snapshot is not None:
            context_text = self._safe_json_dumps(context_snapshot)

        try:
            intent_values = (
                normalize_report_execution_intent(execution_intent)
                if execution_intent is not None
                else None
            )

            def _write(session: Session) -> int:
                history = AnalysisHistory(
                    query_id=query_id,
                    code=result.code,
                    name=result.name,
                    report_type=report_type,
                    model_used=(str(getattr(result, "model_used", "") or "").strip() or None),
                    sentiment_score=result.sentiment_score,
                    operation_advice=result.operation_advice,
                    trend_prediction=result.trend_prediction,
                    analysis_summary=result.analysis_summary,
                    raw_result=self._safe_json_dumps(raw_result),
                    news_content=news_content,
                    context_snapshot=context_text,
                    ideal_buy=sniper_points.get("ideal_buy"),
                    secondary_buy=sniper_points.get("secondary_buy"),
                    stop_loss=sniper_points.get("stop_loss"),
                    take_profit=sniper_points.get("take_profit"),
                    created_at=datetime.now(),
                )
                session.add(history)
                session.flush()
                if intent_values is not None:
                    session.add(
                        AnalysisReportExecutionIntent(
                            analysis_history_id=history.id,
                            **intent_values,
                        )
                    )
                    session.flush()
                return int(history.id or 0)
            return self._run_write_transaction(
                f"save_analysis_history[{result.code}]",
                _write,
            )
        except Exception as e:
            logger.error(f"保存分析历史失败: {e}")
            return 0

    def save_analysis_attempt(
        self,
        *,
        query_id: str,
        code: str,
        name: Optional[str] = None,
        report_type: Optional[str] = None,
        model_used: Optional[str] = None,
        status: str,
        error_message: Optional[str] = None,
        analysis_history_id: Optional[int] = None,
    ) -> int:
        """Persist the outcome of one analysis attempt, including no-report failures."""

        safe_status = str(status or "").strip().lower()
        if safe_status not in {"completed", "failed", "degraded"}:
            safe_status = "failed"
        safe_error_message = sanitize_diagnostic_text(error_message, max_length=1000)

        try:
            def _write(session: Session) -> int:
                row = AnalysisAttempt(
                    query_id=query_id,
                    code=code,
                    name=name,
                    report_type=report_type,
                    model_used=(str(model_used or "").strip() or None),
                    status=safe_status,
                    error_message=(safe_error_message or None),
                    analysis_history_id=(
                        int(analysis_history_id)
                        if isinstance(analysis_history_id, int)
                        and not isinstance(analysis_history_id, bool)
                        and analysis_history_id > 0
                        else None
                    ),
                    created_at=datetime.now(),
                )
                session.add(row)
                session.flush()
                return int(row.id or 0)

            return self._run_write_transaction(
                f"save_analysis_attempt[{code}:{model_used or '*'}:{safe_status}]",
                _write,
            )
        except Exception as e:
            logger.warning("保存分析尝试记录失败: %s", e)
            return 0

    def get_latest_analysis_attempts(
        self,
        *,
        model_used: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 500,
    ) -> List[AnalysisAttempt]:
        """Return latest attempts for stock-bar reconciliation."""

        with self.get_session() as session:
            conditions = []
            if model_used:
                conditions.append(AnalysisAttempt.model_used == model_used)
            if start_date:
                conditions.append(AnalysisAttempt.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                conditions.append(AnalysisAttempt.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
            where_clause = and_(*conditions) if conditions else True
            rows = session.execute(
                select(AnalysisAttempt)
                .where(where_clause)
                .order_by(desc(AnalysisAttempt.created_at), desc(AnalysisAttempt.id))
                .limit(max(1, min(int(limit), 2000)))
            ).scalars().all()
            return list(rows)

    def update_analysis_history_diagnostics(
        self,
        *,
        query_id: str,
        code: Optional[str] = None,
        diagnostics: Optional[Dict[str, Any]] = None,
        notification_runs: Optional[List[Dict[str, Any]]] = None,
    ) -> int:
        """
        更新已保存分析历史的运行诊断快照。

        通知结果通常在分析历史落库后才产生，因此这里仅补写
        context_snapshot.diagnostics，不改变报告正文或其它历史字段。
        """
        if not query_id or (diagnostics is None and not notification_runs):
            return 0

        try:
            def _write(session: Session) -> int:
                conditions = [AnalysisHistory.query_id == query_id]
                if code:
                    conditions.append(AnalysisHistory.code == code)

                row = session.execute(
                    select(AnalysisHistory)
                    .where(and_(*conditions))
                    .order_by(desc(AnalysisHistory.created_at))
                    .limit(1)
                ).scalars().first()
                if row is None:
                    return 0

                context_snapshot: Dict[str, Any] = {}
                if row.context_snapshot:
                    try:
                        parsed = json.loads(row.context_snapshot)
                        if isinstance(parsed, dict):
                            context_snapshot = parsed
                    except Exception:
                        context_snapshot = {}

                if diagnostics is not None:
                    context_snapshot["diagnostics"] = diagnostics
                else:
                    existing_diagnostics = context_snapshot.get("diagnostics")
                    if not isinstance(existing_diagnostics, dict):
                        existing_diagnostics = {
                            "query_id": query_id,
                            "stock_code": code,
                            "notification_runs": [],
                        }
                    runs = existing_diagnostics.get("notification_runs")
                    if not isinstance(runs, list):
                        runs = []
                    trace_id = existing_diagnostics.get("trace_id")
                    for run in notification_runs or []:
                        if isinstance(run, dict):
                            run_payload = dict(run)
                            if trace_id and not run_payload.get("trace_id"):
                                run_payload["trace_id"] = trace_id
                            runs.append(run_payload)
                    existing_diagnostics["notification_runs"] = runs
                    context_snapshot["diagnostics"] = existing_diagnostics
                row.context_snapshot = self._safe_json_dumps(context_snapshot)
                return 1

            return self._run_write_transaction(
                f"update_analysis_history_diagnostics[{query_id}:{code or '*'}]",
                _write,
            )
        except Exception as e:
            logger.warning(
                "更新分析历史诊断快照失败（fail-open）: query_id=%s code=%s err=%s",
                query_id,
                code,
                e,
            )
            return 0

    def get_analysis_history(
        self,
        code: Optional[str] = None,
        query_id: Optional[str] = None,
        days: int = 30,
        limit: int = 50,
        exclude_query_id: Optional[str] = None,
    ) -> List[AnalysisHistory]:
        """
        Query analysis history records.

        Notes:
        - If query_id is provided, perform exact lookup and ignore days window.
        - If query_id is not provided, apply days-based time filtering.
        - exclude_query_id: exclude records with this query_id (for history comparison).
        """
        cutoff_date = datetime.now() - timedelta(days=days)

        with self.get_session() as session:
            conditions = []

            if query_id:
                conditions.append(AnalysisHistory.query_id == query_id)
            else:
                conditions.append(AnalysisHistory.created_at >= cutoff_date)

            if code:
                conditions.append(AnalysisHistory.code == code)

            # exclude_query_id only applies when not doing exact lookup (query_id is None)
            if exclude_query_id and not query_id:
                conditions.append(AnalysisHistory.query_id != exclude_query_id)

            results = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(limit)
            ).scalars().all()

            return list(results)

    def get_latest_analysis_history_id(
        self,
        *,
        query_id: str,
        code: str,
        report_type: str,
    ) -> Optional[int]:
        """Return the latest matching history id for read-only lookups.

        P2 automatic DecisionSignal extraction receives the freshly saved id
        directly from ``save_analysis_history()`` and does not use this helper.
        """

        if not query_id or not code or not report_type:
            return None

        with self.get_session() as session:
            return session.execute(
                select(AnalysisHistory.id)
                .where(
                    AnalysisHistory.query_id == query_id,
                    AnalysisHistory.code == code,
                    AnalysisHistory.report_type == report_type,
                )
                .order_by(desc(AnalysisHistory.created_at), desc(AnalysisHistory.id))
                .limit(1)
            ).scalar_one_or_none()
    
    def get_analysis_history_paginated(
        self,
        code: Optional[Union[str, List[str]]] = None,
        report_type: Optional[str] = None,
        model_used: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        offset: int = 0,
        limit: int = 20
    ) -> Tuple[List[AnalysisHistory], int]:
        """
        分页查询分析历史记录（带总数）
        
        Args:
            code: 股票代码筛选
            report_type: 报告类型筛选
            start_date: 开始日期（含）
            end_date: 结束日期（含）
            offset: 偏移量（跳过前 N 条）
            limit: 每页数量
            
        Returns:
            Tuple[List[AnalysisHistory], int]: (记录列表, 总数)
        """
        from sqlalchemy import func
        
        with self.get_session() as session:
            conditions = []
            
            if code:
                if isinstance(code, list):
                    codes = [c for c in code if c]
                    if codes:
                        conditions.append(AnalysisHistory.code.in_(codes))
                else:
                    conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)
            if model_used:
                conditions.append(AnalysisHistory.model_used == model_used)
            if start_date:
                # created_at >= start_date 00:00:00
                conditions.append(AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time()))
            if end_date:
                # created_at < end_date+1 00:00:00 (即 <= end_date 23:59:59)
                conditions.append(AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time()))
            
            # 构建 where 子句
            where_clause = and_(*conditions) if conditions else True
            
            # 查询总数
            total_query = select(func.count(AnalysisHistory.id)).where(where_clause)
            total = session.execute(total_query).scalar() or 0
            
            # 查询分页数据
            data_query = (
                select(AnalysisHistory)
                .where(where_clause)
                .order_by(desc(AnalysisHistory.created_at))
                .offset(offset)
                .limit(limit)
            )
            results = session.execute(data_query).scalars().all()
            
            return list(results), total
    
    def get_analysis_history_by_id(self, record_id: int) -> Optional[AnalysisHistory]:
        """
        根据数据库主键 ID 查询单条分析历史记录
        
        由于 query_id 可能重复（批量分析时多条记录共享同一 query_id），
        使用主键 ID 确保精确查询唯一记录。
        
        Args:
            record_id: 分析历史记录的主键 ID
            
        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            result = session.execute(
                select(AnalysisHistory).where(AnalysisHistory.id == record_id)
            ).scalars().first()
            return result

    def get_report_execution_intent(self, record_id: int) -> Optional[AnalysisReportExecutionIntent]:
        """Return the execution purpose frozen with one report, if any."""

        with self.get_session() as session:
            return session.execute(
                select(AnalysisReportExecutionIntent).where(
                    AnalysisReportExecutionIntent.analysis_history_id == int(record_id)
                )
            ).scalar_one_or_none()

    def delete_analysis_history_records(self, record_ids: List[int]) -> int:
        """
        删除指定的分析历史记录。

        同时清理依赖这些历史记录的回测结果和分析来源决策信号，避免
        依赖历史记录的派生数据残留。DecisionSignal 的 source_report_id
        允许弱引用，因此这里只清理 source_type=analysis 的真实历史绑定信号。

        Args:
            record_ids: 要删除的历史记录主键 ID 列表

        Returns:
            实际删除的历史记录数量
        """
        ids = sorted({int(record_id) for record_id in record_ids if record_id is not None})
        if not ids:
            return 0

        with self.session_scope() as session:
            existing_ids = sorted(
                session.execute(
                    select(AnalysisHistory.id).where(AnalysisHistory.id.in_(ids))
                ).scalars().all()
            )
            if not existing_ids:
                return 0

            linked_plan_ids = sorted(
                session.execute(
                    select(PaperExecutionPlan.id).where(
                        PaperExecutionPlan.source_report_id.in_(existing_ids)
                    )
                ).scalars().all()
            )
            linked_position_ids = sorted(
                session.execute(
                    select(PaperPosition.id).where(
                        PaperPosition.source_report_id.in_(existing_ids)
                    )
                ).scalars().all()
            )
            if linked_plan_ids or linked_position_ids:
                event_conditions = [PaperTradeEvent.source_report_id.in_(existing_ids)]
                if linked_plan_ids:
                    event_conditions.append(PaperTradeEvent.plan_id.in_(linked_plan_ids))
                if linked_position_ids:
                    event_conditions.append(PaperTradeEvent.position_id.in_(linked_position_ids))
                session.execute(delete(PaperTradeEvent).where(or_(*event_conditions)))
                observation_conditions = [PaperPlanObservation.source_report_id.in_(existing_ids)]
                if linked_plan_ids:
                    observation_conditions.append(PaperPlanObservation.plan_id.in_(linked_plan_ids))
                if linked_position_ids:
                    observation_conditions.append(PaperPlanObservation.position_id.in_(linked_position_ids))
                session.execute(delete(PaperPlanObservation).where(or_(*observation_conditions)))
                if linked_position_ids:
                    session.execute(
                        delete(PaperPosition).where(PaperPosition.id.in_(linked_position_ids))
                    )
                if linked_plan_ids:
                    session.execute(
                        delete(PaperPlanEvaluation).where(
                            PaperPlanEvaluation.plan_id.in_(linked_plan_ids)
                        )
                    )
                    session.execute(
                        delete(PaperExecutionPlan).where(
                            PaperExecutionPlan.id.in_(linked_plan_ids)
                        )
                    )

            linked_signal_ids = sorted(
                session.execute(
                    select(DecisionSignalRecord.id).where(
                        and_(
                            DecisionSignalRecord.source_type == "analysis",
                            DecisionSignalRecord.source_report_id.in_(existing_ids),
                        )
                    )
                ).scalars().all()
            )
            if linked_signal_ids:
                session.execute(
                    update(PaperPosition)
                    .where(PaperPosition.close_signal_id.in_(linked_signal_ids))
                    .values(close_signal_id=None)
                )
                session.execute(
                    update(PaperTradeEvent)
                    .where(PaperTradeEvent.triggering_signal_id.in_(linked_signal_ids))
                    .values(triggering_signal_id=None)
                )
                session.execute(
                    delete(DecisionSignalOutcomeRecord).where(
                        DecisionSignalOutcomeRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalFeedbackRecord).where(
                        DecisionSignalFeedbackRecord.signal_id.in_(linked_signal_ids)
                    )
                )
                session.execute(
                    delete(DecisionSignalRecord).where(DecisionSignalRecord.id.in_(linked_signal_ids))
                )
            session.execute(
                delete(BacktestResult).where(BacktestResult.analysis_history_id.in_(existing_ids))
            )
            session.execute(
                delete(AnalysisReportExecutionIntent).where(
                    AnalysisReportExecutionIntent.analysis_history_id.in_(existing_ids)
                )
            )
            result = session.execute(
                delete(AnalysisHistory).where(AnalysisHistory.id.in_(existing_ids))
            )
            return result.rowcount or 0

    def get_distinct_stocks_from_history(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 200,
        include_market_review: bool = False,
        model_used: Optional[str] = None,
    ) -> List[AnalysisHistory]:
        """
        获取历史记录中的不重复股票列表，每只股票取最新一条记录。

        使用子查询按 code 分组取 MAX(id)，再 JOIN 回查完整记录。
        默认排除大盘复盘，避免混入普通个股栏。

        Args:
            start_date: 开始日期
            end_date: 结束日期
            limit: 最大返回数量
            include_market_review: 是否包含大盘复盘记录

        Returns:
            每条股票最新一条 AnalysisHistory 记录列表
        """
        with self.get_session() as session:
            subq = (
                select(
                    AnalysisHistory.code,
                    func.max(AnalysisHistory.id).label("max_id"),
                )
            )
            if start_date:
                subq = subq.where(
                    AnalysisHistory.created_at >= datetime.combine(start_date, datetime.min.time())
                )
            if end_date:
                subq = subq.where(
                    AnalysisHistory.created_at < datetime.combine(end_date + timedelta(days=1), datetime.min.time())
                )
            if model_used:
                subq = subq.where(AnalysisHistory.model_used == model_used)
            if not include_market_review:
                subq = subq.where(
                    and_(
                        AnalysisHistory.code != "MARKET",
                        or_(
                            AnalysisHistory.report_type.is_(None),
                            AnalysisHistory.report_type != "market_review",
                        ),
                    )
                )
            subq = subq.group_by(AnalysisHistory.code).subquery()

            results = (
                session.execute(
                    select(AnalysisHistory)
                    .join(subq, AnalysisHistory.id == subq.c.max_id)
                    .order_by(
                        desc(AnalysisHistory.created_at),
                    )
                    .limit(limit)
                )
                .scalars()
                .all()
            )
            return list(results)

    def get_latest_analysis_by_query_id(
        self,
        query_id: str,
        *,
        code: Optional[str] = None,
        report_type: Optional[str] = None,
    ) -> Optional[AnalysisHistory]:
        """
        根据 query_id 查询最新一条分析历史记录

        query_id 在批量分析时可能重复，故返回最近创建的一条。

        Args:
            query_id: 分析记录关联的 query_id
            code: 可选股票代码过滤，用于区分同一 query_id 下的 MARKET 与个股记录
            report_type: 可选报告类型过滤

        Returns:
            AnalysisHistory 对象，不存在返回 None
        """
        with self.get_session() as session:
            conditions = [AnalysisHistory.query_id == query_id]
            if code:
                conditions.append(AnalysisHistory.code == code)
            if report_type:
                conditions.append(AnalysisHistory.report_type == report_type)

            result = session.execute(
                select(AnalysisHistory)
                .where(and_(*conditions))
                .order_by(desc(AnalysisHistory.created_at))
                .limit(1)
            ).scalars().first()
            return result
    
    def get_data_range(
        self, 
        code: str, 
        start_date: date, 
        end_date: date
    ) -> List[StockDaily]:
        """
        获取指定日期范围的数据
        
        Args:
            code: 股票代码
            start_date: 开始日期
            end_date: 结束日期
            
        Returns:
            StockDaily 对象列表
        """
        with self.get_session() as session:
            results = session.execute(
                select(StockDaily)
                .where(
                    and_(
                        StockDaily.code == code,
                        StockDaily.date >= start_date,
                        StockDaily.date <= end_date
                    )
                )
                .order_by(StockDaily.date)
            ).scalars().all()
            
            return list(results)
    
    def save_daily_data(
        self, 
        df: pd.DataFrame, 
        code: str,
        data_source: str = "Unknown"
    ) -> int:
        """
        保存日线数据到数据库
        
        策略：
        - 按 `(code, date)` 做批量 UPSERT，已存在记录会覆盖更新
        - 同一批次内若存在重复日期，以最后一条记录为准
        - SQLite 分支按 chunk 写入以避免绑定参数上限
        
        Args:
            df: 包含日线数据的 DataFrame
            code: 股票代码
            data_source: 数据来源名称
            
        Returns:
            本次实际新增的记录数（不含更新）
        """
        if df is None or df.empty:
            logger.warning(f"保存数据为空，跳过 {code}")
            return 0

        now = datetime.now()
        records_by_date: Dict[date, Dict[str, Any]] = {}
        from data_provider.volume_units import SHARES_SOURCE_SUFFIX
        if df.attrs.get('volume_unit') == 'shares' and not data_source.endswith(SHARES_SOURCE_SUFFIX):
            data_source += SHARES_SOURCE_SUFFIX
        for row in df.to_dict(orient='records'):
            row_date = self._normalize_daily_date(row.get('date'))
            records_by_date[row_date] = {
                'code': code,
                'date': row_date,
                'open': self._normalize_sql_value(row.get('open')),
                'high': self._normalize_sql_value(row.get('high')),
                'low': self._normalize_sql_value(row.get('low')),
                'close': self._normalize_sql_value(row.get('close')),
                'volume': self._normalize_sql_value(row.get('volume')),
                'amount': self._normalize_sql_value(row.get('amount')),
                'pct_chg': self._normalize_sql_value(row.get('pct_chg')),
                'ma5': self._normalize_sql_value(row.get('ma5')),
                'ma10': self._normalize_sql_value(row.get('ma10')),
                'ma20': self._normalize_sql_value(row.get('ma20')),
                'volume_ratio': self._normalize_sql_value(row.get('volume_ratio')),
                'data_source': data_source,
                'created_at': now,
                'updated_at': now,
            }

        if not records_by_date:
            return 0

        records = list(records_by_date.values())
        batch_dates = list(records_by_date.keys())

        def _write(session: Session) -> int:
            if self._is_sqlite_engine:
                # SQLite has a per-statement bind-parameter limit (commonly 999).
                # Each record has ~15 columns, so chunk upserts to stay within bounds.
                _SQLITE_CHUNK = 50
                # `_run_write_transaction()` opens SQLite writes with
                # `BEGIN IMMEDIATE`, so existence checks and upsert execute
                # within one stable write window.
                existing_dates = set()
                _COUNT_CHUNK = 500
                for j in range(0, len(batch_dates), _COUNT_CHUNK):
                    chunk_dates = batch_dates[j : j + _COUNT_CHUNK]
                    if not chunk_dates:
                        continue
                    existing_dates.update(
                        session.execute(
                            select(StockDaily.date).where(
                                and_(
                                    StockDaily.code == code,
                                    StockDaily.date.in_(chunk_dates),
                                )
                            )
                        ).scalars().all()
                    )
                new_records = [
                    record for record in records if record['date'] not in existing_dates
                ]
                for i in range(0, len(records), _SQLITE_CHUNK):
                    chunk = records[i : i + _SQLITE_CHUNK]
                    stmt = sqlite_insert(StockDaily).values(chunk)
                    excluded = stmt.excluded
                    session.execute(
                        stmt.on_conflict_do_update(
                            index_elements=['code', 'date'],
                            set_={
                                'open': excluded.open,
                                'high': excluded.high,
                                'low': excluded.low,
                                'close': excluded.close,
                                'volume': excluded.volume,
                                'amount': excluded.amount,
                                'pct_chg': excluded.pct_chg,
                                'ma5': excluded.ma5,
                                'ma10': excluded.ma10,
                                'ma20': excluded.ma20,
                                'volume_ratio': excluded.volume_ratio,
                                'data_source': excluded.data_source,
                                'updated_at': excluded.updated_at,
                            },
                        )
                    )
                return len(new_records)
            elif self._dialect_name == "postgresql":
                existing_dates = set(
                    session.execute(
                        select(StockDaily.date).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date.in_(batch_dates),
                            )
                        )
                    ).scalars().all()
                )
                stmt = postgresql_insert(StockDaily).values(records)
                excluded = stmt.excluded
                session.execute(
                    stmt.on_conflict_do_update(
                        index_elements=["code", "date"],
                        set_={
                            "open": excluded.open,
                            "high": excluded.high,
                            "low": excluded.low,
                            "close": excluded.close,
                            "volume": excluded.volume,
                            "amount": excluded.amount,
                            "pct_chg": excluded.pct_chg,
                            "ma5": excluded.ma5,
                            "ma10": excluded.ma10,
                            "ma20": excluded.ma20,
                            "volume_ratio": excluded.volume_ratio,
                            "data_source": excluded.data_source,
                            "updated_at": excluded.updated_at,
                        },
                    )
                )
                return sum(
                    1 for record in records if record["date"] not in existing_dates
                )
            else:
                existing_rows = {
                    row.date: row
                    for row in session.execute(
                        select(StockDaily).where(
                            and_(
                                StockDaily.code == code,
                                StockDaily.date.in_(batch_dates),
                            )
                        )
                    ).scalars().all()
                }
                new_count = 0
                for record in records:
                    existing = existing_rows.get(record['date'])
                    if existing is None:
                        session.add(StockDaily(**record))
                        new_count += 1
                        continue
                    existing.open = record['open']
                    existing.high = record['high']
                    existing.low = record['low']
                    existing.close = record['close']
                    existing.volume = record['volume']
                    existing.amount = record['amount']
                    existing.pct_chg = record['pct_chg']
                    existing.ma5 = record['ma5']
                    existing.ma10 = record['ma10']
                    existing.ma20 = record['ma20']
                    existing.volume_ratio = record['volume_ratio']
                    existing.data_source = record['data_source']
                    existing.updated_at = record['updated_at']
                return new_count

        try:
            saved_count = self._run_write_transaction(
                f"save_daily_data[{code}]",
                _write,
            )
            logger.info(f"保存 {code} 数据成功，新增 {saved_count} 条")
            return saved_count
        except Exception as e:
            logger.error(f"保存 {code} 数据失败: {e}")
            raise
    
    def get_analysis_context(
        self, 
        code: str,
        target_date: Optional[date] = None
    ) -> Optional[Dict[str, Any]]:
        """
        获取分析所需的上下文数据
        
        返回今日数据 + 昨日数据的对比信息
        
        Args:
            code: 股票代码
            target_date: 目标日期（默认今天）
            
        Returns:
            包含今日数据、昨日对比等信息的字典
        """
        if target_date is None:
            target_date = date.today()
        # 注意：尽管入参提供了 target_date，但当前实现实际使用的是“最新两天数据”（get_latest_data），
        # 并不会按 target_date 精确取当日/前一交易日的上下文。
        # 因此若未来需要支持“按历史某天复盘/重算”的可解释性，这里需要调整。
        # 该行为目前保留（按需求不改逻辑）。
        
        # 获取最近2天数据
        recent_data = self.get_latest_data(code, days=2)
        
        if not recent_data:
            logger.warning(f"未找到 {code} 的数据")
            return None
        
        today_data = recent_data[0]
        yesterday_data = recent_data[1] if len(recent_data) > 1 else None
        
        context = {
            'code': code,
            'date': today_data.date.isoformat(),
            'today': today_data.to_dict(),
        }
        
        if yesterday_data:
            context['yesterday'] = yesterday_data.to_dict()
            
            # 计算相比昨日的变化
            today_volume = context['today'].get('volume')
            yesterday_volume = context['yesterday'].get('volume')
            if today_volume is not None and yesterday_volume and yesterday_volume > 0:
                context['volume_change_ratio'] = round(
                    today_volume / yesterday_volume, 2
                )
            
            if yesterday_data.close and yesterday_data.close > 0:
                context['price_change_ratio'] = round(
                    (today_data.close - yesterday_data.close) / yesterday_data.close * 100, 2
                )
            
            # 均线形态判断
            context['ma_status'] = self._analyze_ma_status(today_data)
        
        return context
    
    def _analyze_ma_status(self, data: StockDaily) -> str:
        """
        分析均线形态
        
        判断条件：
        - 多头排列：close > ma5 > ma10 > ma20
        - 空头排列：close < ma5 < ma10 < ma20
        - 震荡整理：其他情况
        """
        # 注意：这里的均线形态判断基于“close/ma5/ma10/ma20”静态比较，
        # 未考虑均线拐点、斜率、或不同数据源复权口径差异。
        # 该行为目前保留（按需求不改逻辑）。
        close = data.close or 0
        ma5 = data.ma5 or 0
        ma10 = data.ma10 or 0
        ma20 = data.ma20 or 0
        
        if close > ma5 > ma10 > ma20 > 0:
            return "多头排列 📈"
        elif close < ma5 < ma10 < ma20 and ma20 > 0:
            return "空头排列 📉"
        elif close > ma5 and ma5 > ma10:
            return "短期向好 🔼"
        elif close < ma5 and ma5 < ma10:
            return "短期走弱 🔽"
        else:
            return "震荡整理 ↔️"

    @staticmethod
    def _parse_published_date(value: Optional[str]) -> Optional[datetime]:
        """
        解析发布时间字符串（失败返回 None）
        """
        if not value:
            return None

        if isinstance(value, datetime):
            return value

        text = str(value).strip()
        if not text:
            return None

        # 优先尝试 ISO 格式
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(text, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _safe_json_dumps(data: Any) -> str:
        """
        安全序列化为 JSON 字符串
        """
        try:
            return json.dumps(data, ensure_ascii=False, default=str)
        except Exception:
            return json.dumps(str(data), ensure_ascii=False)

    @staticmethod
    def _build_raw_result(result: Any) -> Dict[str, Any]:
        """
        生成完整分析结果字典
        """
        data = result.to_dict() if hasattr(result, "to_dict") else {}
        data.update({
            'data_sources': getattr(result, 'data_sources', ''),
            'raw_response': getattr(result, 'raw_response', None),
        })
        return data

    @staticmethod
    def _parse_sniper_value(value: Any) -> Optional[float]:
        return parse_sniper_value(value)

    def _extract_sniper_points(self, result: Any) -> Dict[str, Optional[float]]:
        """Extract normalized sniper point values from an AnalysisResult."""

        return extract_sniper_points(result)

    @staticmethod
    def _build_fallback_url_key(
        code: str,
        title: str,
        source: str,
        published_date: Optional[datetime]
    ) -> str:
        """
        生成无 URL 时的去重键（确保稳定且较短）
        """
        date_str = published_date.isoformat() if published_date else ""
        raw_key = f"{code}|{title}|{source}|{date_str}"
        digest = hashlib.md5(raw_key.encode("utf-8")).hexdigest()
        return f"no-url:{code}:{digest}"

    def save_conversation_message(self, session_id: str, role: str, content: str) -> int:
        """
        保存 Agent 对话消息
        """
        with self.session_scope() as session:
            msg = ConversationMessage(
                session_id=session_id,
                role=role,
                content=content
            )
            session.add(msg)
            session.flush()
            return int(msg.id)

    def get_conversation_history(self, session_id: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        获取 Agent 对话历史
        """
        with self.session_scope() as session:
            stmt = select(ConversationMessage).filter(
                ConversationMessage.session_id == session_id
            ).order_by(ConversationMessage.created_at.desc()).limit(limit)
            messages = session.execute(stmt).scalars().all()

            # 倒序返回，保证时间顺序
            return [{"role": msg.role, "content": msg.content} for msg in reversed(messages)]

    def get_visible_conversation_messages(self, session_id: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Return visible user/assistant conversation messages in chronological order."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(
                    and_(
                        ConversationMessage.session_id == session_id,
                        ConversationMessage.role.in_(["user", "assistant"]),
                    )
                )
                .order_by(ConversationMessage.created_at, ConversationMessage.id)
            )
            if limit is not None:
                stmt = (
                    stmt.order_by(None)
                    .order_by(ConversationMessage.created_at.desc(), ConversationMessage.id.desc())
                    .limit(limit)
                )
            messages = session.execute(stmt).scalars().all()
            if limit is not None:
                messages = list(reversed(messages))
            return [
                {
                    "id": msg.id,
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at,
                }
                for msg in messages
                if msg.content
            ]

    def get_conversation_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """Return the rolling summary for a conversation session, if present."""
        with self.session_scope() as session:
            stmt = select(ConversationSummary).where(
                ConversationSummary.session_id == session_id
            )
            row = session.execute(stmt).scalar_one_or_none()
            if row is None:
                return None
            return {
                "id": row.id,
                "session_id": row.session_id,
                "summary": row.summary,
                "covered_message_id": row.covered_message_id,
                "source_message_count": row.source_message_count,
                "estimated_tokens": row.estimated_tokens,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }

    def save_agent_provider_turn(
        self,
        *,
        session_id: str,
        run_id: str,
        provider: str,
        model: str,
        anchor_user_message_id: int,
        anchor_assistant_message_id: int,
        messages: List[Dict[str, Any]],
        contains_reasoning: bool,
        contains_tool_calls: bool,
        contains_thinking_blocks: bool,
        must_roundtrip: bool,
        estimated_tokens: int,
    ) -> int:
        """Persist one provider protocol trace and enforce per-model retention."""
        with self.session_scope() as session:
            row = AgentProviderTurn(
                session_id=session_id,
                run_id=run_id,
                provider=provider,
                model=model,
                anchor_user_message_id=int(anchor_user_message_id or 0),
                anchor_assistant_message_id=int(anchor_assistant_message_id or 0),
                messages_json=json.dumps(messages or [], ensure_ascii=False, default=str),
                contains_reasoning=bool(contains_reasoning),
                contains_tool_calls=bool(contains_tool_calls),
                contains_thinking_blocks=bool(contains_thinking_blocks),
                must_roundtrip=bool(must_roundtrip),
                estimated_tokens=int(estimated_tokens or 0),
            )
            session.add(row)
            session.flush()
            row_id = int(row.id)
            if row.must_roundtrip:
                self._trim_agent_provider_turns(
                    session=session,
                    session_id=session_id,
                    provider=provider,
                    model=model,
                    keep=PROVIDER_TRACE_RETENTION_LIMIT,
                )
            return row_id

    def get_agent_provider_turns(
        self,
        session_id: str,
        *,
        provider: Optional[str] = None,
        model: Optional[str] = None,
        must_roundtrip_only: bool = True,
    ) -> List[Dict[str, Any]]:
        """Return provider trace turns in chronological order."""
        with self.session_scope() as session:
            conditions = [AgentProviderTurn.session_id == session_id]
            if provider:
                conditions.append(AgentProviderTurn.provider == provider)
            if model:
                conditions.append(AgentProviderTurn.model == model)
            if must_roundtrip_only:
                conditions.append(AgentProviderTurn.must_roundtrip.is_(True))
            stmt = (
                select(AgentProviderTurn)
                .where(and_(*conditions))
                .order_by(AgentProviderTurn.created_at, AgentProviderTurn.id)
            )
            rows = session.execute(stmt).scalars().all()
            result = []
            for row in rows:
                try:
                    messages = json.loads(row.messages_json or "[]")
                except json.JSONDecodeError as exc:
                    logger.warning(
                        "Invalid provider trace messages_json skipped for session %s turn %s: %s",
                        row.session_id,
                        row.id,
                        exc,
                    )
                    messages = []
                result.append({
                    "id": row.id,
                    "session_id": row.session_id,
                    "run_id": row.run_id,
                    "provider": row.provider,
                    "model": row.model,
                    "anchor_user_message_id": row.anchor_user_message_id,
                    "anchor_assistant_message_id": row.anchor_assistant_message_id,
                    "messages": messages if isinstance(messages, list) else [],
                    "messages_json": row.messages_json,
                    "contains_reasoning": row.contains_reasoning,
                    "contains_tool_calls": row.contains_tool_calls,
                    "contains_thinking_blocks": row.contains_thinking_blocks,
                    "must_roundtrip": row.must_roundtrip,
                    "estimated_tokens": row.estimated_tokens,
                    "created_at": row.created_at,
                })
            return result

    def _trim_agent_provider_turns(
        self,
        *,
        session: Session,
        session_id: str,
        provider: str,
        model: str,
        keep: int,
    ) -> int:
        old_ids_stmt = (
            select(AgentProviderTurn.id)
            .where(
                and_(
                    AgentProviderTurn.session_id == session_id,
                    AgentProviderTurn.provider == provider,
                    AgentProviderTurn.model == model,
                    AgentProviderTurn.must_roundtrip.is_(True),
                )
            )
            .order_by(AgentProviderTurn.created_at.desc(), AgentProviderTurn.id.desc())
            .offset(max(0, int(keep)))
        )
        old_ids = list(session.execute(old_ids_stmt).scalars().all())
        if not old_ids:
            return 0
        result = session.execute(
            delete(AgentProviderTurn).where(AgentProviderTurn.id.in_(old_ids))
        )
        return int(result.rowcount or 0)

    def upsert_conversation_summary(
        self,
        session_id: str,
        summary: str,
        covered_message_id: int,
        source_message_count: int,
        estimated_tokens: int,
    ) -> None:
        """Create or update the rolling summary for a conversation session."""
        with self.session_scope() as session:
            now = datetime.now()
            values = {
                "session_id": session_id,
                "summary": summary,
                "covered_message_id": int(covered_message_id or 0),
                "source_message_count": int(source_message_count or 0),
                "estimated_tokens": int(estimated_tokens or 0),
                "updated_at": now,
            }
            if self._dialect_name == "postgresql":
                stmt = postgresql_insert(ConversationSummary).values(**values)
            elif self._is_sqlite_engine:
                stmt = sqlite_insert(ConversationSummary).values(**values)
            else:
                existing = session.get(ConversationSummary, session_id)
                if existing is None:
                    session.add(ConversationSummary(**values))
                else:
                    for key, value in values.items():
                        setattr(existing, key, value)
                return
            session.execute(
                stmt.on_conflict_do_update(
                    index_elements=["session_id"],
                    set_=values,
                )
            )

    def conversation_session_exists(self, session_id: str) -> bool:
        """Return True when at least one message exists for the given session."""
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage.id)
                .where(ConversationMessage.session_id == session_id)
                .limit(1)
            )
            return session.execute(stmt).scalar() is not None

    def get_chat_sessions(
        self,
        limit: int = 50,
        session_prefix: Optional[str] = None,
        extra_session_ids: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """
        获取聊天会话列表（从 conversation_messages 聚合）

        Args:
            limit: Maximum number of sessions to return.
            session_prefix: If provided, only return sessions whose session_id
                starts with this prefix.  Used for per-user isolation (e.g.
                ``"telegram_12345"``).
            extra_session_ids: Optional exact session ids to include in
                addition to the scoped prefix.

        Returns:
            按最近活跃时间倒序的会话列表，每条包含 session_id, title, message_count, last_active
        """
        from sqlalchemy import func

        with self.session_scope() as session:
            normalized_prefix = None
            if session_prefix:
                normalized_prefix = session_prefix if session_prefix.endswith(":") else f"{session_prefix}:"
            exact_ids = [sid for sid in (extra_session_ids or []) if sid]

            # 聚合每个 session 的消息数和最后活跃时间
            base = (
                select(
                    ConversationMessage.session_id,
                    func.count(ConversationMessage.id).label("message_count"),
                    func.min(ConversationMessage.created_at).label("created_at"),
                    func.max(ConversationMessage.created_at).label("last_active"),
                )
            )
            conditions = []
            if normalized_prefix:
                conditions.append(ConversationMessage.session_id.startswith(normalized_prefix))
            if exact_ids:
                conditions.append(ConversationMessage.session_id.in_(exact_ids))
            if conditions:
                base = base.where(or_(*conditions))
            stmt = (
                base
                .group_by(ConversationMessage.session_id)
                .order_by(desc(func.max(ConversationMessage.created_at)))
                .limit(limit)
            )
            rows = session.execute(stmt).all()

            results = []
            for row in rows:
                sid = row.session_id
                # 取该会话第一条 user 消息作为标题
                first_user_msg = session.execute(
                    select(ConversationMessage.content)
                    .where(
                        and_(
                            ConversationMessage.session_id == sid,
                            ConversationMessage.role == "user",
                        )
                    )
                    .order_by(ConversationMessage.created_at)
                    .limit(1)
                ).scalar()
                title = (first_user_msg or "新对话")[:60]

                results.append({
                    "session_id": sid,
                    "title": title,
                    "message_count": row.message_count,
                    "created_at": row.created_at.isoformat() if row.created_at else None,
                    "last_active": row.last_active.isoformat() if row.last_active else None,
                })
            return results

    def get_conversation_messages(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        获取单个会话的完整消息列表（用于前端恢复历史）
        """
        with self.session_scope() as session:
            stmt = (
                select(ConversationMessage)
                .where(ConversationMessage.session_id == session_id)
                .order_by(ConversationMessage.created_at)
                .limit(limit)
            )
            messages = session.execute(stmt).scalars().all()
            return [
                {
                    "id": str(msg.id),
                    "role": msg.role,
                    "content": msg.content,
                    "created_at": msg.created_at.isoformat() if msg.created_at else None,
                }
                for msg in messages
            ]

    def delete_conversation_session(self, session_id: str) -> int:
        """
        删除指定会话的所有消息

        Returns:
            删除的消息数
        """
        with self.session_scope() as session:
            session.execute(
                delete(AgentProviderTurn).where(
                    AgentProviderTurn.session_id == session_id
                )
            )
            session.execute(
                delete(ConversationSummary).where(
                    ConversationSummary.session_id == session_id
                )
            )
            result = session.execute(
                delete(ConversationMessage).where(
                    ConversationMessage.session_id == session_id
                )
            )
            return result.rowcount

    # ------------------------------------------------------------------
    # LLM usage tracking
    # ------------------------------------------------------------------

    def record_llm_usage(
        self,
        call_type: str,
        model: str,
        prompt_tokens: int,
        completion_tokens: int,
        total_tokens: int,
        stock_code: Optional[str] = None,
        **telemetry: Any,
    ) -> None:
        """Append one LLM call record to llm_usage."""
        row_values: Dict[str, Any] = {
            "call_type": call_type,
            "model": model or "unknown",
            "stock_code": stock_code,
            "prompt_tokens": prompt_tokens,
            "completion_tokens": completion_tokens,
            "total_tokens": total_tokens,
        }
        for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL:
            row_values[column] = None if column in _LLM_USAGE_DROPPED_FREE_TEXT_COLUMNS else telemetry.get(column)
        row = LLMUsage(**row_values)
        with self.session_scope() as session:
            session.add(row)

    def get_llm_usage_summary(
        self,
        from_dt: datetime,
        to_dt: datetime,
    ) -> Dict[str, Any]:
        """Return aggregated token usage between from_dt and to_dt.

        Returns a dict with keys:
          total_calls, total_prompt_tokens, total_completion_tokens, total_tokens,
          by_call_type: list of {call_type, calls, prompt_tokens,
            completion_tokens, total_tokens},
          by_model: list of {model, calls, prompt_tokens, completion_tokens,
            total_tokens, max_total_tokens}
        """
        with self.session_scope() as session:
            base_filter = and_(
                LLMUsage.called_at >= from_dt,
                LLMUsage.called_at <= to_dt,
            )

            # Overall totals
            totals = session.execute(
                select(
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                ).where(base_filter)
            ).one()

            # Breakdown by call_type
            by_type_rows = session.execute(
                select(
                    LLMUsage.call_type,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.call_type)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

            # Breakdown by model
            by_model_rows = session.execute(
                select(
                    LLMUsage.model,
                    func.count(LLMUsage.id).label("calls"),
                    func.coalesce(func.sum(LLMUsage.prompt_tokens), 0).label("prompt_tokens"),
                    func.coalesce(func.sum(LLMUsage.completion_tokens), 0).label("completion_tokens"),
                    func.coalesce(func.sum(LLMUsage.total_tokens), 0).label("tokens"),
                    func.coalesce(func.max(LLMUsage.total_tokens), 0).label("max_total_tokens"),
                )
                .where(base_filter)
                .group_by(LLMUsage.model)
                .order_by(desc(func.sum(LLMUsage.total_tokens)))
            ).all()

        return {
            "total_calls": totals.calls,
            "total_prompt_tokens": totals.prompt_tokens,
            "total_completion_tokens": totals.completion_tokens,
            "total_tokens": totals.tokens,
            "by_call_type": [
                {
                    "call_type": r.call_type,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                }
                for r in by_type_rows
            ],
            "by_model": [
                {
                    "model": r.model,
                    "calls": r.calls,
                    "prompt_tokens": r.prompt_tokens,
                    "completion_tokens": r.completion_tokens,
                    "total_tokens": r.tokens,
                    "max_total_tokens": r.max_total_tokens,
                }
                for r in by_model_rows
            ],
        }

    def get_llm_usage_records(
        self,
        from_dt: datetime,
        to_dt: datetime,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Return recent LLM usage audit rows between from_dt and to_dt.

        Each row contains id, call_type, model, stock_code, prompt_tokens,
        completion_tokens, total_tokens, and called_at. Results are ordered by
        newest call first, and limit is clamped to the public API range.
        """
        normalized_limit = max(1, min(int(limit or 50), 200))
        with self.session_scope() as session:
            rows = session.execute(
                select(
                    LLMUsage.id,
                    LLMUsage.call_type,
                    LLMUsage.model,
                    LLMUsage.stock_code,
                    LLMUsage.prompt_tokens,
                    LLMUsage.completion_tokens,
                    LLMUsage.total_tokens,
                    LLMUsage.called_at,
                )
                .where(
                    and_(
                        LLMUsage.called_at >= from_dt,
                        LLMUsage.called_at <= to_dt,
                    )
                )
                .order_by(desc(LLMUsage.called_at), desc(LLMUsage.id))
                .limit(normalized_limit)
            ).all()

        return [
            {
                "id": r.id,
                "call_type": r.call_type,
                "model": r.model,
                "stock_code": r.stock_code,
                "prompt_tokens": r.prompt_tokens,
                "completion_tokens": r.completion_tokens,
                "total_tokens": r.total_tokens,
                "called_at": r.called_at,
            }
            for r in rows
        ]


# 便捷函数
def get_db() -> DatabaseManager:
    """获取数据库管理器实例的快捷方式"""
    return DatabaseManager.get_instance()


def persist_llm_usage(
    usage: Dict[str, Any],
    model: str,
    call_type: str,
    stock_code: Optional[str] = None,
) -> None:
    """Fire-and-forget: write one LLM call record to llm_usage. Never raises."""
    try:
        if usage is None:
            usage = {}
        prompt_cache_telemetry_disabled = bool(
            getattr(usage, _LLM_PROMPT_CACHE_TELEMETRY_DISABLED_ATTR, False)
        )
        prompt_tokens = _coerce_llm_usage_non_negative_int(usage.get("prompt_tokens")) or 0
        completion_tokens = _coerce_llm_usage_non_negative_int(usage.get("completion_tokens")) or 0
        total_tokens = _coerce_llm_usage_non_negative_int(usage.get("total_tokens")) or 0
        telemetry = {
            column: usage.get(column)
            for column in _LLM_USAGE_TELEMETRY_COLUMN_SQL
        }
        if prompt_cache_telemetry_disabled:
            for column in _LLM_PROMPT_CACHE_TELEMETRY_COLUMNS:
                telemetry[column] = None
        for column in _LLM_USAGE_INTEGER_TELEMETRY_COLUMNS:
            telemetry[column] = _coerce_llm_usage_non_negative_int(telemetry.get(column))
        telemetry["normalized_prompt_tokens"] = (
            telemetry.get("normalized_prompt_tokens")
            if telemetry.get("normalized_prompt_tokens") is not None
            else prompt_tokens
        )
        telemetry["normalized_completion_tokens"] = (
            telemetry.get("normalized_completion_tokens")
            if telemetry.get("normalized_completion_tokens") is not None
            else completion_tokens
        )
        telemetry["normalized_total_tokens"] = (
            telemetry.get("normalized_total_tokens")
            if telemetry.get("normalized_total_tokens") is not None
            else total_tokens
        )
        has_usage_payload = bool(usage.get("provider_usage_json")) or any(
            key in usage
            for key in (
                "prompt_tokens",
                "completion_tokens",
                "total_tokens",
                "normalized_prompt_tokens",
                "normalized_completion_tokens",
                "normalized_total_tokens",
            )
        )
        if not prompt_cache_telemetry_disabled:
            telemetry["cache_capability"] = usage.get("cache_capability") or "unknown"
            telemetry["cache_eligibility"] = usage.get("cache_eligibility") or "unknown"
            telemetry["cache_observation"] = usage.get("cache_observation") or (
                "no_usage" if not has_usage_payload else "unknown"
            )
        db = DatabaseManager.get_instance()
        db.record_llm_usage(
            call_type=call_type,
            model=model,
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens,
            stock_code=stock_code,
            **telemetry,
        )
    except Exception as exc:
        logging.getLogger(__name__).warning("[LLM usage] failed to persist usage record: %s", exc)


def _coerce_llm_usage_non_negative_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        if value < 0 or not value.is_integer():
            return None
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text or not text.isdigit():
            return None
        return int(text)
    return None


if __name__ == "__main__":
    # 测试代码
    logging.basicConfig(level=logging.DEBUG)
    
    db = get_db()
    
    print("=== 数据库测试 ===")
    print(f"数据库初始化成功")
    
    # 测试检查今日数据
    has_data = db.has_today_data('600519')
    print(f"茅台今日是否有数据: {has_data}")
    
    # 测试保存数据
    test_df = pd.DataFrame({
        'date': [date.today()],
        'open': [1800.0],
        'high': [1850.0],
        'low': [1780.0],
        'close': [1820.0],
        'volume': [10000000],
        'amount': [18200000000],
        'pct_chg': [1.5],
        'ma5': [1810.0],
        'ma10': [1800.0],
        'ma20': [1790.0],
        'volume_ratio': [1.2],
    })
    
    saved = db.save_daily_data(test_df, '600519', 'TestSource')
    print(f"保存测试数据: {saved} 条")
    
    # 测试获取上下文
    context = db.get_analysis_context('600519')
    print(f"分析上下文: {context}")

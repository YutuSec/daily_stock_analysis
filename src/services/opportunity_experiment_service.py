# -*- coding: utf-8 -*-
"""R4: trusted orchestration and evidence import for the opportunity A/B/C experiment.

Each arm runs in its own process against its own database and reuses the one
paper-trading engine.  A worker sets its ``DATABASE_URL`` *before* importing
``src`` so the ``DatabaseManager`` singleton binds to the arm DB; the registry
therefore stores the *names* of the env vars that hold each arm's URL, never the
URLs (let alone credentials) themselves.

``import_research_bundle`` is the only door through which a shared research
result becomes executable inside an arm.  A correct ``bundle_id`` is an
integrity check, not an authorization: authorization comes from the trusted
orchestrator writing an experiment-scoped ``paper_candidate`` intent for the
freshly created arm-local report.  The original research report and intent are
never mutated; the arm gets its own copy bound to ``experiment:<id>:<arm>``.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Mapping, Optional, Tuple

from sqlalchemy.engine import make_url

from src.storage import (
    REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
    DatabaseManager,
    normalize_report_execution_scope,
)

logger = logging.getLogger(__name__)

REGISTERED_ARMS = ("a", "b", "c")
EXPERIMENT_IMPORT_INTENT_SOURCE = "opportunity_experiment_import"
# 与机会池研究证据包版本对齐；跨版本不导入，避免结构漂移被当作合法证据。
from src.services.market_opportunity_service import RESEARCH_BUNDLE_VERSION


class OpportunityExperimentError(ValueError):
    """A bundle or registry violates the experiment's trust contract."""


def _stable_hash(value: Any) -> str:
    """Same canonical hash the research bundle uses, so recomputation matches."""

    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def experiment_scope(experiment_id: str, arm: str) -> str:
    """Return the normalized ``experiment:<id>:<arm>`` execution scope."""

    return normalize_report_execution_scope(f"experiment:{experiment_id}:{_normalize_arm(arm)}")


def _normalize_arm(arm: Any) -> str:
    normalized = str(arm or "").strip().lower()
    if normalized not in REGISTERED_ARMS:
        raise OpportunityExperimentError(f"unregistered opportunity experiment arm: {arm!r}")
    return normalized


def market_input_batch_id(experiment_id: str, session_date: Any) -> str:
    """Deterministic id for one session's frozen market inputs, shared by A/B/C.

    The three arms consume the same real close data for a session; recovery
    after a restart reuses the same batch instead of refetching, and the id is
    identical across arms so no arm's positions leak into another's inputs.
    """

    day = _coerce_date(session_date)
    key = day.isoformat() if day is not None else str(session_date)
    return _stable_hash({"experiment_id": str(experiment_id), "session": key})[:32]


@dataclass(frozen=True)
class OpportunityExperimentRegistry:
    """Thin, trusted description of one experiment's three isolated arms.

    ``arm_db_env`` maps ``a``/``b``/``c`` to the *name* of the environment
    variable that holds that arm's database URL.  URLs and credentials are
    resolved only at validation/worker start and are never stored or logged.
    """

    experiment_id: str
    arm_db_env: Mapping[str, str]
    initial_capital: float
    code_version: str
    created_at: Optional[datetime] = None
    status: str = "registered"

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "OpportunityExperimentRegistry":
        if not isinstance(value, Mapping):
            raise OpportunityExperimentError("experiment registry must be a mapping")
        experiment_id = str(value.get("experiment_id") or "").strip()
        if not experiment_id:
            raise OpportunityExperimentError("experiment registry requires experiment_id")
        # 借用 scope 规则校验 id 合法（长度/字符集），避免非法 id 生成非法 scope。
        experiment_scope(experiment_id, "a")

        raw_env = value.get("arm_db_env")
        if not isinstance(raw_env, Mapping):
            raise OpportunityExperimentError("experiment registry requires arm_db_env mapping")
        arm_db_env: Dict[str, str] = {}
        for arm in REGISTERED_ARMS:
            name = str(raw_env.get(arm) or "").strip()
            if not name:
                raise OpportunityExperimentError(f"experiment registry missing db env var name for arm {arm!r}")
            arm_db_env[arm] = name
        if len({name for name in arm_db_env.values()}) != len(REGISTERED_ARMS):
            raise OpportunityExperimentError("each experiment arm needs a distinct db env var name")

        try:
            initial_capital = float(value.get("initial_capital"))
        except (TypeError, ValueError):
            raise OpportunityExperimentError("experiment registry requires numeric initial_capital")
        if initial_capital <= 0:
            raise OpportunityExperimentError("experiment initial_capital must be positive")

        code_version = str(value.get("code_version") or "").strip()
        if not code_version:
            raise OpportunityExperimentError("experiment registry requires code_version")

        created_at = value.get("created_at")
        created_at = created_at if isinstance(created_at, datetime) else None
        return cls(
            experiment_id=experiment_id,
            arm_db_env=dict(arm_db_env),
            initial_capital=initial_capital,
            code_version=code_version,
            created_at=created_at,
            status=str(value.get("status") or "registered"),
        )

    def scope_for(self, arm: str) -> str:
        return experiment_scope(self.experiment_id, arm)

    def scopes(self) -> Dict[str, str]:
        """arm -> ``experiment:<id>:<arm>`` for all three registered arms."""

        return {arm: self.scope_for(arm) for arm in REGISTERED_ARMS}

    def raw_arm_db_url(
        self,
        arm: str,
        *,
        main_db_url: str,
        env: Optional[Mapping[str, str]] = None,
    ) -> str:
        """Return the arm's real DB URL from its env var, proving isolation first.

        Server-side only: the raw URL (with credentials) is used to open a
        read-only connection and must never be returned to a client. Re-runs the
        full isolation proof so a mis-set env can't leak the main DB into an arm.
        """

        arm_key = _normalize_arm(arm)
        # 复用隔离校验：任一组等于主库或组间同库都会抛错，读取路径同样 fail-closed。
        self.resolve_isolated_databases(main_db_url=main_db_url, env=env)
        source = os.environ if env is None else env
        raw_url = str(source.get(self.arm_db_env[arm_key]) or "").strip()
        if not raw_url:
            raise OpportunityExperimentError(
                f"arm {arm_key!r} db env var {self.arm_db_env[arm_key]!r} is not set"
            )
        return raw_url

    def resolve_isolated_databases(
        self,
        *,
        main_db_url: str,
        env: Optional[Mapping[str, str]] = None,
    ) -> Dict[str, str]:
        """Resolve each arm's DB URL from its env var and prove full isolation.

        Returns arm -> password-hidden URL for audit.  Raises when an arm URL is
        missing, when two arms share a URL, or when any arm points at the main
        database.  Credentials never appear in the return value or the raised
        message.
        """

        source = os.environ if env is None else env
        main_key = _url_identity(main_db_url)
        resolved: Dict[str, str] = {}
        identities: Dict[str, str] = {}
        for arm in REGISTERED_ARMS:
            var_name = self.arm_db_env[arm]
            raw_url = str(source.get(var_name) or "").strip()
            if not raw_url:
                raise OpportunityExperimentError(
                    f"arm {arm!r} db env var {var_name!r} is not set"
                )
            identity = _url_identity(raw_url)
            if identity == main_key:
                raise OpportunityExperimentError(
                    f"arm {arm!r} db env var {var_name!r} points at the main database"
                )
            for other_arm, other_identity in identities.items():
                if other_identity == identity:
                    raise OpportunityExperimentError(
                        f"arms {other_arm!r} and {arm!r} share the same database"
                    )
            identities[arm] = identity
            resolved[arm] = _render_safe_url(raw_url)
        return resolved


def _url_identity(url: str) -> str:
    """Comparable identity of a DB URL: backend + host + port + database.

    Ignores credentials so two arms with the same target but different logins
    are still treated as the same database (and thus rejected).
    """

    parsed = make_url(str(url))
    backend = parsed.get_backend_name()
    database = parsed.database or ""
    if backend == "sqlite":
        database = os.path.abspath(database) if database and database != ":memory:" else database
        return f"sqlite::{database}"
    return f"{backend}:{parsed.host or ''}:{parsed.port or ''}:{database}"


def _render_safe_url(url: str) -> str:
    return make_url(str(url)).render_as_string(hide_password=True)


class OpportunityExperimentService:
    """Import shared research evidence into one isolated arm database."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def import_research_bundle(
        self,
        bundle: Mapping[str, Any],
        *,
        experiment_id: str,
        arm: str,
    ) -> Dict[str, Any]:
        """Create (idempotently) the arm-local executable copy of one research result.

        The original research report/intent are left untouched.  Returns the
        arm-local ``report_id``, ``decision_signal_id`` (when a signal is
        derivable), the experiment ``scope``, ``bundle_id`` and whether this call
        created the copy (``created``) or found an existing one.
        """

        arm_key = _normalize_arm(arm)
        scope = experiment_scope(experiment_id, arm_key)
        payload, as_of = self._validated_bundle(bundle, experiment_id=experiment_id)

        query_id = self._arm_query_id(experiment_id=experiment_id, arm=arm_key, bundle_id=bundle["bundle_id"])
        existing = self.db.get_latest_analysis_by_query_id(query_id)
        if existing is not None:
            return self._result(
                report_id=int(existing.id),
                scope=scope,
                bundle_id=str(bundle["bundle_id"]),
                created=False,
            )

        result = self._result_from_payload(payload)
        report_id = self.db.save_analysis_history(
            result,
            query_id=query_id,
            report_type=str(bundle.get("report_type") or "simple"),
            news_content=None,
            context_snapshot={
                "opportunity_experiment": {
                    "experiment_id": str(experiment_id),
                    "arm": arm_key,
                    "bundle_id": str(bundle["bundle_id"]),
                    "source_report_id": bundle.get("report_id"),
                    "research_kind": bundle.get("research_kind"),
                    "as_of": as_of.isoformat(),
                }
            },
            execution_intent={
                "execution_intent": REPORT_EXECUTION_INTENT_PAPER_CANDIDATE,
                "execution_scope": scope,
                "intent_source": EXPERIMENT_IMPORT_INTENT_SOURCE,
                "decision_as_of": as_of.isoformat(),
            },
        )
        if not report_id:
            raise OpportunityExperimentError(
                f"failed to persist arm-local report for bundle {bundle['bundle_id']!r}"
            )

        signal_id = self._create_arm_signal(report_id)
        return self._result(
            report_id=int(report_id),
            scope=scope,
            bundle_id=str(bundle["bundle_id"]),
            created=True,
            decision_signal_id=signal_id,
        )

    # ---- validation -----------------------------------------------------

    def _validated_bundle(
        self,
        bundle: Mapping[str, Any],
        *,
        experiment_id: str,
    ) -> Tuple[Dict[str, Any], date]:
        if not isinstance(bundle, Mapping):
            raise OpportunityExperimentError("research bundle must be a mapping")
        if bundle.get("bundle_version") != RESEARCH_BUNDLE_VERSION:
            raise OpportunityExperimentError(
                f"unsupported research bundle version: {bundle.get('bundle_version')!r}"
            )
        bundle_id = bundle.get("bundle_id")
        if not isinstance(bundle_id, str) or not bundle_id:
            raise OpportunityExperimentError("research bundle missing bundle_id")

        # 完整性：用包内字段重算 bundle_id，篡改任一关键字段都会失配。
        recomputed = _stable_hash({
            "report_id": int(bundle.get("report_id") or 0),
            "candidate_key": (bundle.get("candidate") or {}).get("candidate_key"),
            "model": str(bundle.get("model") or ""),
            "as_of": bundle.get("as_of"),
            "research_kind": bundle.get("research_kind"),
            "input_hash": bundle.get("input_hash"),
            "output_hash": bundle.get("output_hash"),
            "bundle_version": RESEARCH_BUNDLE_VERSION,
        })
        if recomputed != bundle_id:
            raise OpportunityExperimentError("research bundle_id does not match its own contents")

        as_of = _coerce_date(bundle.get("as_of"))
        if as_of is None:
            raise OpportunityExperimentError("research bundle missing a valid as_of session")

        payload = bundle.get("report_payload")
        if not isinstance(payload, Mapping) or not payload:
            raise OpportunityExperimentError("research bundle carries no executable report payload")
        if payload.get("success") is False:
            raise OpportunityExperimentError("research bundle payload is a failed analysis")
        # 正文完整性：报告正文哈希必须等于包内 output_hash，防止导入被替换的正文。
        if _stable_hash(dict(payload)) != bundle.get("output_hash"):
            raise OpportunityExperimentError("research bundle payload does not match output_hash")
        return dict(payload), as_of

    # ---- reconstruction -------------------------------------------------

    def _result_from_payload(self, payload: Mapping[str, Any]) -> Any:
        """Rebuild an ``AnalysisResult`` from the account-independent payload.

        The payload is a prior ``AnalysisResult.to_dict()``; it carries no
        account, position, quantity or cost state, so the arm-local copy cannot
        inherit another book's mutable state.
        """

        from src.analyzer import AnalysisResult

        def _pick(key: str, default: Any) -> Any:
            value = payload.get(key)
            return value if value is not None else default

        return AnalysisResult(
            code=str(_pick("code", "")),
            name=str(_pick("name", "")),
            sentiment_score=int(_pick("sentiment_score", 50)),
            trend_prediction=str(_pick("trend_prediction", "")),
            operation_advice=str(_pick("operation_advice", "")),
            decision_type=str(_pick("decision_type", "hold")),
            confidence_level=str(_pick("confidence_level", "中")),
            report_language=str(_pick("report_language", "zh")),
            report_schema_version=payload.get("report_schema_version"),
            action=payload.get("action"),
            action_label=payload.get("action_label"),
            raw_score=payload.get("raw_score"),
            adjusted_score=payload.get("adjusted_score"),
            guardrail=payload.get("guardrail") if isinstance(payload.get("guardrail"), dict) else None,
            limitations=list(payload.get("limitations") or []),
            position_actions=(
                payload.get("position_actions")
                if isinstance(payload.get("position_actions"), dict)
                else None
            ),
            holding_state=str(_pick("holding_state", "unknown")),
            dashboard=payload.get("dashboard") if isinstance(payload.get("dashboard"), dict) else None,
            analysis_summary=str(_pick("analysis_summary", "")),
            key_points=str(_pick("key_points", "")),
            risk_warning=str(_pick("risk_warning", "")),
            buy_reason=str(_pick("buy_reason", "")),
            market_snapshot=payload.get("market_snapshot") if isinstance(payload.get("market_snapshot"), dict) else None,
            raw_response=payload.get("raw_response"),
            search_performed=bool(payload.get("search_performed", False)),
            data_sources=str(_pick("data_sources", "")),
            success=bool(payload.get("success", True)),
            error_message=payload.get("error_message"),
            current_price=_as_float(payload.get("current_price")),
            change_pct=_as_float(payload.get("change_pct")),
            model_used=payload.get("model_used"),
            market_structure_context=(
                payload.get("market_structure_context")
                if isinstance(payload.get("market_structure_context"), dict)
                else None
            ),
        )

    def _create_arm_signal(self, report_id: int) -> Optional[int]:
        """Derive the arm-local analysis decision signal from the saved report.

        Reuses the existing history backfill so the signal is bound to this arm's
        report (``source_type='analysis'``, ``source_report_id=report_id``).  A
        report that yields no executable signal (e.g. watch) simply has none.
        """

        from src.repositories.paper_trading_repo import PaperTradingRepository
        from src.services.decision_signal_service import DecisionSignalService

        service = DecisionSignalService(db_manager=self.db)
        service._backfill_analysis_signal_from_history(int(report_id))
        signal = PaperTradingRepository(db_manager=self.db).latest_report_signal(int(report_id))
        return int(signal.id) if signal is not None else None

    # ---- helpers --------------------------------------------------------

    @staticmethod
    def _arm_query_id(*, experiment_id: str, arm: str, bundle_id: str) -> str:
        # scope + bundle 唯一确定一条导入记录，重复导入命中同一 query_id 而幂等。
        # query_id / trace_id 均为 String(64)，故摘要压缩到 64 字符内。
        digest = _stable_hash({"experiment_id": str(experiment_id), "arm": arm, "bundle_id": bundle_id})
        return f"opp-exp:{digest[:48]}"

    @staticmethod
    def _result(
        *,
        report_id: int,
        scope: str,
        bundle_id: str,
        created: bool,
        decision_signal_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        return {
            "report_id": report_id,
            "execution_scope": scope,
            "bundle_id": bundle_id,
            "created": created,
            "decision_signal_id": decision_signal_id,
        }


def _coerce_date(value: Any) -> Optional[date]:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10]) if value else None
    except ValueError:
        return None


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def load_experiment_registries(registry_path: str) -> List[OpportunityExperimentRegistry]:
    """Load the trusted experiment registry file for the Web/API process.

    The file is a server-side JSON describing zero or more experiments (a single
    experiment object, or ``{"experiments": [...]}``).  Each entry stores arm→DB
    **env var names** only; addresses/credentials are never in the file.  An
    empty/missing path yields no experiments so the UI shows only ``primary``.
    """

    path = str(registry_path or "").strip()
    if not path:
        return []
    try:
        with open(path, "r", encoding="utf-8") as handle:
            raw = json.load(handle)
    except FileNotFoundError:
        logger.warning("opportunity experiment registry path not found: %s", path)
        return []
    except (OSError, ValueError) as exc:
        raise OpportunityExperimentError(f"invalid experiment registry file: {exc}") from exc

    if isinstance(raw, Mapping) and isinstance(raw.get("experiments"), list):
        entries = raw["experiments"]
    elif isinstance(raw, Mapping):
        entries = [raw]
    elif isinstance(raw, list):
        entries = raw
    else:
        raise OpportunityExperimentError("experiment registry file must be an object or list")

    registries: List[OpportunityExperimentRegistry] = []
    seen: set = set()
    for entry in entries:
        registry = OpportunityExperimentRegistry.from_mapping(entry)
        if registry.experiment_id in seen:
            raise OpportunityExperimentError(
                f"duplicate experiment_id in registry file: {registry.experiment_id!r}"
            )
        seen.add(registry.experiment_id)
        registries.append(registry)
    return registries


def resolve_scope_registry(
    scope: str,
    registries: List[OpportunityExperimentRegistry],
) -> Tuple[OpportunityExperimentRegistry, str]:
    """Map an ``experiment:<id>:<arm>`` scope to its (registry, arm).

    Raises ``OpportunityExperimentError`` for a malformed scope, ``primary``
    (which has no registry), or an experiment id not present in the trusted
    file.  The caller never passes a DB address — only a scope id.
    """

    try:
        normalized = normalize_report_execution_scope(str(scope or ""))
    except ValueError as exc:
        # 归一化失败（非法 scope 字符串）统一转成实验错误，让调用方 fail-closed。
        raise OpportunityExperimentError(str(exc)) from exc
    if not normalized.startswith("experiment:"):
        raise OpportunityExperimentError(f"scope {scope!r} is not an experiment scope")
    _, experiment_id, arm = normalized.split(":", 2)
    for registry in registries:
        if registry.experiment_id == experiment_id:
            return registry, _normalize_arm(arm)
    raise OpportunityExperimentError(f"unknown experiment id in scope: {experiment_id!r}")

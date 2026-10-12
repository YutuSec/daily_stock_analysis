"""The shared summary producer and legacy API consumer must not expose credentials."""
from datetime import datetime, timezone
import json

from src.scheduler import Scheduler
from api.v1.endpoints.paper_trading import _scheduler_last_run_summaries


def test_failure_summary_sanitizes_secrets_before_persistence_and_at_legacy_read():
    secret_error = (
        "provider failed https://user:fixture-password@provider.test/path?api_key=fixture-key "
        "Authorization: Bearer fixture-bearer api_key=fixture-assignment "
        "https://open.feishu.cn/open-apis/bot/v2/hook/fixture-hook"
    )
    summary = Scheduler._background_run_summary(
        {}, status="failed", started_at=None,
        finished_at=datetime(2026, 9, 8, tzinfo=timezone.utc), duration_seconds=1,
        error_message=secret_error,
    )
    legacy = _scheduler_last_run_summaries({"monitor": {"heartbeat": {"details": {
        "last_run_summary": {**summary, "error_message": secret_error},
    }}}})
    for output in (summary, legacy["monitor"]):
        assert output["status"] == "failed"
        assert "provider failed" in output["error_message"]
        assert "REDACTED" in output["error_message"]
        assert "fixture-" not in json.dumps(output)

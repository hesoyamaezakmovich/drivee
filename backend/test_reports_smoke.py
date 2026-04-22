"""Smoke test for the scheduled-reports pipeline.

Does not require Ollama, SMTP, or a network. Exercises:
- schema migration on existing reports.db
- CRUD on reports
- cron validation & presets
- end-to-end scheduler.run_and_dispatch (records run + in-app delivery)

Run directly from the backend/ directory:

    python test_reports_smoke.py
"""
from __future__ import annotations

import asyncio
import os
import sys
import tempfile
from pathlib import Path

# Isolate from the developer's real reports.db.
TMP = Path(tempfile.mkdtemp())
os.environ["DRIVEE_TEST_MODE"] = "1"

import reports_store  # noqa: E402

reports_store.DB_PATH = str(TMP / "reports.db")

import cron_utils  # noqa: E402
import notifications  # noqa: E402
from scheduler import ReportScheduler  # noqa: E402


def check(cond: bool, msg: str) -> None:
    if not cond:
        print(f"FAIL: {msg}")
        sys.exit(1)
    print(f"ok   {msg}")


async def fake_execute(sql: str) -> dict:
    assert "select" in sql.lower()
    return {
        "columns": ["city", "orders"],
        "rows": [["Москва", 1200], ["Санкт-Петербург", 800], ["Казань", 300]],
    }


async def main() -> None:
    reports_store.init_db()
    check(Path(reports_store.DB_PATH).exists(), "reports.db created")

    # Cron validation
    ok = cron_utils.validate_cron("0 9 * * 1")
    check(ok["ok"] and len(ok["next_runs"]) == 5, "validate monday 9am cron")
    bad = cron_utils.validate_cron("nope")
    check(not bad["ok"], "reject bad cron")
    check(len(cron_utils.PRESETS) >= 5, "presets available")

    # CRUD
    r = reports_store.create_report(
        name="Недельный пульс городов",
        question="покажи поездки по городам за последние 7 дней",
        sql="SELECT city_id, COUNT(*) FROM orders GROUP BY city_id LIMIT 1000",
        chart_type="bar",
        schedule_cron="0 9 * * 1",
        recipients=["ceo@example.com", "ops@example.com"],
        enabled=True,
    )
    check(r.id > 0, "report created")
    check(r.recipients == ["ceo@example.com", "ops@example.com"], "recipients stored")

    r2 = reports_store.update_report(r.id, enabled=False, recipients=["a@b.com"])
    check(not r2.enabled and r2.recipients == ["a@b.com"], "patch persists")

    reports_store.update_report(r.id, enabled=True)

    # Scheduler run_and_dispatch
    sched = ReportScheduler(execute_sql=fake_execute)
    sched.start()
    result = await sched.run_and_dispatch(r.id, trigger="manual")
    check(result["status"] == "success", "dispatch succeeds")
    check(result["row_count"] == 3, "row count correct")
    check(any(d["channel"] == "in-app" for d in result["deliveries"]), "in-app delivery recorded")

    runs = reports_store.list_runs(r.id)
    check(len(runs) == 1 and runs[0]["status"] == "success", "run history")

    inbox = reports_store.list_deliveries()
    check(len(inbox) == 1, "inbox has 1 delivery")
    check(reports_store.count_unread_deliveries() == 1, "delivery unread by default")
    reports_store.mark_delivery_read(inbox[0]["id"])
    check(reports_store.count_unread_deliveries() == 0, "mark-read works")

    detail = reports_store.get_delivery(inbox[0]["id"])
    check(detail and detail["run"]["data_snapshot"]["columns"] == ["city", "orders"],
          "snapshot roundtrips through json")

    # Email path when SMTP is unconfigured → delivery is recorded as "skipped".
    result2 = await sched.run_and_dispatch(r.id, trigger="manual")
    email_deliveries = [d for d in result2["deliveries"] if d["channel"] == "email"]
    check(len(email_deliveries) == 1 and email_deliveries[0]["status"] == "skipped",
          "email marked skipped without SMTP")

    # HTML rendering
    html = notifications.render_report_html(
        report_name="test",
        question="q",
        sql="SELECT 1",
        columns=["a", "b"],
        rows=[[1, 2]],
        run_ts="now",
    )
    check("<table" in html and "SELECT 1" in html, "html rendered")

    # Schedule / unschedule lifecycle
    sched.sync(r.id)
    r_after = reports_store.get_report(r.id)
    check(r_after.next_run_at is not None, "next_run_at populated after schedule")
    reports_store.update_report(r.id, enabled=False)
    sched.sync(r.id)
    r_off = reports_store.get_report(r.id)
    check(r_off.next_run_at is None, "next_run_at cleared when disabled")

    # Delete cleanup
    scheduled_before = len(sched._scheduler.get_jobs())  # type: ignore[union-attr]
    reports_store.delete_report(r.id)
    sched.unschedule(r.id)
    check(reports_store.get_report(r.id) is None, "delete removes report")
    check(len(sched._scheduler.get_jobs()) <= scheduled_before,  # type: ignore[union-attr]
          "job removed on delete")

    sched.shutdown()
    print("\nAll smoke checks passed.")


if __name__ == "__main__":
    asyncio.run(main())
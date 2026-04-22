"""Report scheduler — runs saved SQL on a cron schedule and dispatches results.

Built on APScheduler's AsyncIOScheduler so it co-operates with uvicorn's
event loop. Each saved report with a non-empty `schedule_cron` and
`enabled=1` gets a CronTrigger job; job IDs are `report-<id>` so we can
reschedule/remove deterministically.
"""
from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Awaitable, Callable

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

import reports_store
import notifications
from cron_utils import next_fire, resolve_timezone

log = logging.getLogger(__name__)

# Injected from main.py — runs SQL and returns {columns, rows, ...}.
ExecuteSqlFn = Callable[[str], Awaitable[dict]]


class ReportScheduler:
    def __init__(self, execute_sql: ExecuteSqlFn) -> None:
        self._execute_sql = execute_sql
        self._scheduler: AsyncIOScheduler | None = None

    # ---------- lifecycle ----------

    def start(self) -> None:
        if self._scheduler is not None:
            return
        self._scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        self._scheduler.start()
        # Load existing scheduled reports.
        for r in reports_store.list_reports():
            if r.enabled and r.schedule_cron:
                try:
                    self.schedule(r.id, r.schedule_cron, r.timezone or "Europe/Moscow")
                except Exception as e:
                    log.warning("skipping report %s: %s", r.id, e)

    def shutdown(self) -> None:
        if self._scheduler is not None:
            self._scheduler.shutdown(wait=False)
            self._scheduler = None

    # ---------- job management ----------

    def _job_id(self, report_id: int) -> str:
        return f"report-{report_id}"

    def schedule(self, report_id: int, cron_expr: str, tz: str) -> datetime | None:
        assert self._scheduler is not None
        trigger = CronTrigger.from_crontab(cron_expr, timezone=resolve_timezone(tz))
        self._scheduler.add_job(
            self._run_report_job,
            trigger=trigger,
            id=self._job_id(report_id),
            replace_existing=True,
            args=[report_id],
            misfire_grace_time=300,
            coalesce=True,
        )
        nxt = next_fire(cron_expr, tz)
        if nxt:
            reports_store.update_report(report_id, next_run_at=nxt.isoformat())
        return nxt

    def unschedule(self, report_id: int) -> None:
        if self._scheduler is None:
            return
        try:
            self._scheduler.remove_job(self._job_id(report_id))
        except Exception:
            pass
        reports_store.update_report(report_id, next_run_at=None)

    def sync(self, report_id: int) -> datetime | None:
        """Re-read a report and (re)schedule or unschedule it as needed."""
        r = reports_store.get_report(report_id)
        if not r:
            self.unschedule(report_id)
            return None
        if r.enabled and r.schedule_cron:
            return self.schedule(r.id, r.schedule_cron, r.timezone or "Europe/Moscow")
        self.unschedule(report_id)
        return None

    # ---------- execution ----------

    async def _run_report_job(self, report_id: int) -> None:
        """Top-level wrapper used by APScheduler (exceptions must be swallowed)."""
        try:
            await self.run_and_dispatch(report_id, trigger="schedule")
        except Exception as e:  # pragma: no cover - defensive
            log.exception("scheduled run of report %s failed: %s", report_id, e)

    async def run_and_dispatch(self, report_id: int, *, trigger: str) -> dict:
        """Execute the saved SQL, record a run, and dispatch notifications.

        `trigger` is one of: schedule, manual, api.
        Returns a summary dict used by manual-trigger endpoints.
        """
        report = reports_store.get_report(report_id)
        if report is None:
            raise LookupError(f"report {report_id} not found")

        started = time.time()
        columns: list[str] = []
        rows: list[list] = []
        error: str | None = None
        status = "success"
        try:
            result = await self._execute_sql(report.sql)
            columns = list(result.get("columns", []))
            rows = list(result.get("rows", []))
        except Exception as e:
            status = "error"
            error = str(e)

        snapshot = (
            {"columns": columns, "rows": rows[:notifications.MAX_PREVIEW_ROWS]}
            if status == "success"
            else None
        )
        run_id = reports_store.record_run(
            report_id=report_id,
            status=status,
            trigger=trigger,
            started_at=started,
            row_count=len(rows) if status == "success" else None,
            error=error,
            snapshot=snapshot,
        )

        now_iso = datetime.now(resolve_timezone(report.timezone or "Europe/Moscow")).isoformat()
        nxt = next_fire(report.schedule_cron, report.timezone or "Europe/Moscow") if report.schedule_cron else None
        reports_store.mark_run_stats(
            report_id,
            status=status,
            when=now_iso,
            next_run_at=nxt.isoformat() if nxt else None,
        )

        dispatch_summary = await self._dispatch(report, run_id, status, columns, rows, error, now_iso)
        return {
            "run_id": run_id,
            "status": status,
            "row_count": len(rows) if status == "success" else 0,
            "error": error,
            "next_run_at": nxt.isoformat() if nxt else None,
            "deliveries": dispatch_summary,
            "data": {"columns": columns, "rows": rows} if status == "success" else None,
        }

    async def _dispatch(
            self,
            report,
            run_id: int,
            status: str,
            columns: list[str],
            rows: list[list],
            error: str | None,
            when_iso: str,
    ) -> list[dict]:
        subject = f"[Drivee] {report.name}" + (" — ошибка" if status == "error" else "")
        preview = (
            error
            if status == "error"
            else notifications.render_preview(columns, rows)
        )

        html = None
        if status == "success":
            html = notifications.render_report_html(
                report_name=report.name,
                question=report.question,
                sql=report.sql,
                columns=columns,
                rows=rows,
                run_ts=when_iso,
            )

        out: list[dict] = []

        # 1) In-app delivery — always recorded, UI reads `deliveries` table.
        in_app_id = reports_store.record_delivery(
            report_id=report.id,
            run_id=run_id,
            channel="in-app",
            target="in-app",
            status="sent" if status == "success" else "failed",
            subject=subject,
            preview=preview,
            error=error,
        )
        out.append({"id": in_app_id, "channel": "in-app", "status": "sent" if status == "success" else "failed"})

        # 2) Email delivery — only if there are recipients AND SMTP is configured
        #    AND the run succeeded (we don't spam failure emails by default).
        if status == "success" and report.recipients and notifications.smtp_configured():
            for addr in report.recipients:
                d_status = "sent"
                d_err: str | None = None
                try:
                    await notifications.send_email(addr, subject, html or "")
                except Exception as e:
                    d_status = "failed"
                    d_err = str(e)
                d_id = reports_store.record_delivery(
                    report_id=report.id,
                    run_id=run_id,
                    channel="email",
                    target=addr,
                    status=d_status,
                    subject=subject,
                    preview=preview,
                    error=d_err,
                )
                out.append({"id": d_id, "channel": "email", "target": addr, "status": d_status, "error": d_err})
        elif status == "success" and report.recipients and not notifications.smtp_configured():
            for addr in report.recipients:
                d_id = reports_store.record_delivery(
                    report_id=report.id,
                    run_id=run_id,
                    channel="email",
                    target=addr,
                    status="skipped",
                    subject=subject,
                    preview=preview,
                    error="SMTP не сконфигурирован",
                )
                out.append({"id": d_id, "channel": "email", "target": addr, "status": "skipped"})

        return out
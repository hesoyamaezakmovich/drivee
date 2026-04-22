"""Persistence for saved reports, run history, and in-app deliveries."""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Iterable

BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "reports.db")

SCHEMA = """
CREATE TABLE IF NOT EXISTS reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    question TEXT NOT NULL,
    sql TEXT NOT NULL,
    chart_type TEXT,
    schedule_cron TEXT,
    timezone TEXT DEFAULT 'Europe/Moscow',
    recipients TEXT,
    enabled INTEGER DEFAULT 1,
    last_run_at TIMESTAMP,
    last_status TEXT,
    next_run_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS report_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id INTEGER NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    error TEXT,
    row_count INTEGER,
    duration_ms INTEGER,
    trigger TEXT,
    data_snapshot TEXT,
    FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS deliveries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    report_id INTEGER NOT NULL,
    run_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    target TEXT,
    status TEXT NOT NULL,
    error TEXT,
    subject TEXT,
    preview TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    read_at TIMESTAMP,
    FOREIGN KEY (report_id) REFERENCES reports(id) ON DELETE CASCADE,
    FOREIGN KEY (run_id) REFERENCES report_runs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_runs_report ON report_runs(report_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deliveries_report ON deliveries(report_id, created_at DESC);
"""

# Columns that may be missing in older reports.db instances created by earlier
# versions — we ALTER TABLE to add them on startup.
_EXPECTED_REPORT_COLUMNS = {
    "timezone": "TEXT DEFAULT 'Europe/Moscow'",
    "recipients": "TEXT",
    "enabled": "INTEGER DEFAULT 1",
    "last_run_at": "TIMESTAMP",
    "last_status": "TEXT",
    "next_run_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}


def _connect() -> sqlite3.Connection:
    con = sqlite3.connect(DB_PATH, timeout=10, isolation_level=None)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    con.execute("PRAGMA journal_mode = WAL")
    return con


def init_db() -> None:
    Path(DB_PATH).parent.mkdir(parents=True, exist_ok=True)
    con = _connect()
    try:
        con.executescript(SCHEMA)
        existing = {r["name"] for r in con.execute("PRAGMA table_info(reports)")}
        for col, ddl in _EXPECTED_REPORT_COLUMNS.items():
            if col not in existing:
                con.execute(f"ALTER TABLE reports ADD COLUMN {col} {ddl}")
    finally:
        con.close()


# ---------- Reports ----------

@dataclass
class Report:
    id: int
    name: str
    question: str
    sql: str
    chart_type: str | None
    schedule_cron: str | None
    timezone: str | None
    recipients: list[str]
    enabled: bool
    last_run_at: str | None
    last_status: str | None
    next_run_at: str | None
    created_at: str | None
    updated_at: str | None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _row_to_report(row: sqlite3.Row) -> Report:
    recipients = []
    raw = row["recipients"] if "recipients" in row.keys() else None
    if raw:
        try:
            recipients = json.loads(raw)
            if not isinstance(recipients, list):
                recipients = []
        except Exception:
            recipients = [x.strip() for x in raw.split(",") if x.strip()]
    return Report(
        id=row["id"],
        name=row["name"],
        question=row["question"],
        sql=row["sql"],
        chart_type=row["chart_type"],
        schedule_cron=row["schedule_cron"],
        timezone=row["timezone"] if "timezone" in row.keys() else None,
        recipients=recipients,
        enabled=bool(row["enabled"]) if "enabled" in row.keys() and row["enabled"] is not None else True,
        last_run_at=row["last_run_at"] if "last_run_at" in row.keys() else None,
        last_status=row["last_status"] if "last_status" in row.keys() else None,
        next_run_at=row["next_run_at"] if "next_run_at" in row.keys() else None,
        created_at=row["created_at"],
        updated_at=row["updated_at"] if "updated_at" in row.keys() else None,
    )


def create_report(
        *,
        name: str,
        question: str,
        sql: str,
        chart_type: str | None = None,
        schedule_cron: str | None = None,
        timezone: str = "Europe/Moscow",
        recipients: Iterable[str] | None = None,
        enabled: bool = True,
) -> Report:
    con = _connect()
    try:
        cur = con.execute(
            """INSERT INTO reports
               (name, question, sql, chart_type, schedule_cron, timezone,
                recipients, enabled, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)""",
            (
                name.strip(),
                question,
                sql,
                chart_type,
                schedule_cron,
                timezone,
                json.dumps(list(recipients or []), ensure_ascii=False),
                1 if enabled else 0,
            ),
        )
        rid = cur.lastrowid
        return get_report(rid)  # type: ignore[return-value]
    finally:
        con.close()


def list_reports() -> list[Report]:
    con = _connect()
    try:
        rows = con.execute("SELECT * FROM reports ORDER BY id DESC").fetchall()
        return [_row_to_report(r) for r in rows]
    finally:
        con.close()


def get_report(report_id: int) -> Report | None:
    con = _connect()
    try:
        row = con.execute("SELECT * FROM reports WHERE id = ?", (report_id,)).fetchone()
        return _row_to_report(row) if row else None
    finally:
        con.close()


def update_report(report_id: int, **fields: Any) -> Report | None:
    if not fields:
        return get_report(report_id)
    allowed = {
        "name", "schedule_cron", "timezone", "recipients", "enabled",
        "chart_type", "next_run_at",
    }
    sets: list[str] = []
    values: list[Any] = []
    for k, v in fields.items():
        if k not in allowed:
            continue
        if k == "recipients":
            v = json.dumps(list(v or []), ensure_ascii=False)
        if k == "enabled":
            v = 1 if v else 0
        sets.append(f"{k} = ?")
        values.append(v)
    if not sets:
        return get_report(report_id)
    sets.append("updated_at = CURRENT_TIMESTAMP")
    values.append(report_id)
    con = _connect()
    try:
        con.execute(f"UPDATE reports SET {', '.join(sets)} WHERE id = ?", values)
    finally:
        con.close()
    return get_report(report_id)


def delete_report(report_id: int) -> bool:
    con = _connect()
    try:
        cur = con.execute("DELETE FROM reports WHERE id = ?", (report_id,))
        return cur.rowcount > 0
    finally:
        con.close()


def mark_run_stats(report_id: int, *, status: str, when: str, next_run_at: str | None) -> None:
    con = _connect()
    try:
        con.execute(
            """UPDATE reports
               SET last_run_at = ?, last_status = ?, next_run_at = ?,
                   updated_at = CURRENT_TIMESTAMP
               WHERE id = ?""",
            (when, status, next_run_at, report_id),
        )
    finally:
        con.close()


# ---------- Runs ----------

def record_run(
        *,
        report_id: int,
        status: str,
        trigger: str,
        started_at: float,
        row_count: int | None,
        error: str | None,
        snapshot: dict | None,
) -> int:
    duration_ms = int((time.time() - started_at) * 1000)
    con = _connect()
    try:
        cur = con.execute(
            """INSERT INTO report_runs
               (report_id, finished_at, status, error, row_count,
                duration_ms, trigger, data_snapshot)
               VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, ?)""",
            (
                report_id,
                status,
                error,
                row_count,
                duration_ms,
                trigger,
                json.dumps(snapshot, ensure_ascii=False, default=str) if snapshot else None,
            ),
        )
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        con.close()


def list_runs(report_id: int, limit: int = 20) -> list[dict]:
    con = _connect()
    try:
        rows = con.execute(
            """SELECT id, started_at, finished_at, status, error,
                      row_count, duration_ms, trigger
               FROM report_runs WHERE report_id = ?
               ORDER BY id DESC LIMIT ?""",
            (report_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_run(run_id: int) -> dict | None:
    con = _connect()
    try:
        row = con.execute(
            "SELECT * FROM report_runs WHERE id = ?", (run_id,)
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        if d.get("data_snapshot"):
            try:
                d["data_snapshot"] = json.loads(d["data_snapshot"])
            except Exception:
                pass
        return d
    finally:
        con.close()


# ---------- Deliveries ----------

def record_delivery(
        *,
        report_id: int,
        run_id: int,
        channel: str,
        target: str | None,
        status: str,
        subject: str | None = None,
        preview: str | None = None,
        error: str | None = None,
) -> int:
    con = _connect()
    try:
        cur = con.execute(
            """INSERT INTO deliveries
               (report_id, run_id, channel, target, status, subject, preview, error)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (report_id, run_id, channel, target, status, subject, preview, error),
        )
        return cur.lastrowid  # type: ignore[return-value]
    finally:
        con.close()


def list_deliveries(limit: int = 50, unread_only: bool = False) -> list[dict]:
    con = _connect()
    try:
        q = """SELECT d.*, r.name AS report_name, r.chart_type
               FROM deliveries d LEFT JOIN reports r ON r.id = d.report_id
               WHERE d.channel = 'in-app'"""
        if unread_only:
            q += " AND d.read_at IS NULL"
        q += " ORDER BY d.id DESC LIMIT ?"
        rows = con.execute(q, (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        con.close()


def get_delivery(delivery_id: int) -> dict | None:
    con = _connect()
    try:
        row = con.execute(
            """SELECT d.*, r.name AS report_name, r.chart_type, r.question
               FROM deliveries d LEFT JOIN reports r ON r.id = d.report_id
               WHERE d.id = ?""",
            (delivery_id,),
        ).fetchone()
        if not row:
            return None
        d = dict(row)
        run = get_run(d["run_id"])
        if run:
            d["run"] = run
        return d
    finally:
        con.close()


def mark_delivery_read(delivery_id: int) -> bool:
    con = _connect()
    try:
        cur = con.execute(
            "UPDATE deliveries SET read_at = CURRENT_TIMESTAMP WHERE id = ? AND read_at IS NULL",
            (delivery_id,),
        )
        return cur.rowcount > 0
    finally:
        con.close()


def count_unread_deliveries() -> int:
    con = _connect()
    try:
        return con.execute(
            "SELECT COUNT(*) FROM deliveries WHERE channel='in-app' AND read_at IS NULL"
        ).fetchone()[0]
    finally:
        con.close()
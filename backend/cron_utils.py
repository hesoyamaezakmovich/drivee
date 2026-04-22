"""Cron validation, preset library, and next-fire computation."""
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    from croniter import croniter
except ImportError:  # pragma: no cover
    croniter = None  # type: ignore[assignment]

# Human-readable presets (label → 5-field cron expression).
# The "weekly-monday-9" preset matches the PRD screenshot:
# «каждый понедельник в 9:00».
PRESETS: dict[str, dict[str, str]] = {
    "every-monday-9": {
        "label": "Каждый понедельник в 9:00",
        "cron": "0 9 * * 1",
    },
    "weekday-9": {
        "label": "По будням в 9:00",
        "cron": "0 9 * * 1-5",
    },
    "daily-9": {
        "label": "Ежедневно в 9:00",
        "cron": "0 9 * * *",
    },
    "daily-18": {
        "label": "Ежедневно в 18:00",
        "cron": "0 18 * * *",
    },
    "friday-18": {
        "label": "Каждую пятницу в 18:00",
        "cron": "0 18 * * 5",
    },
    "month-start-9": {
        "label": "Первое число месяца в 9:00",
        "cron": "0 9 1 * *",
    },
    "hourly": {
        "label": "Каждый час",
        "cron": "0 * * * *",
    },
}


def resolve_timezone(tz: str | None) -> ZoneInfo:
    try:
        return ZoneInfo(tz) if tz else ZoneInfo("Europe/Moscow")
    except ZoneInfoNotFoundError:
        return ZoneInfo("UTC")


def validate_cron(expr: str, tz: str = "Europe/Moscow") -> dict:
    """Validate a 5-field cron expression and return human-friendly info.

    Returns: {ok, error, next_runs: [iso strings], label}
    """
    expr = (expr or "").strip()
    if not expr:
        return {"ok": False, "error": "Пустое выражение"}
    if croniter is None:
        return {"ok": False, "error": "croniter не установлен"}
    try:
        zone = resolve_timezone(tz)
        base = datetime.now(zone)
        it = croniter(expr, base)
        upcoming = []
        for _ in range(5):
            nxt = it.get_next(datetime)
            if nxt.tzinfo is None:
                nxt = nxt.replace(tzinfo=zone)
            upcoming.append(nxt.isoformat())
        return {
            "ok": True,
            "error": None,
            "next_runs": upcoming,
            "label": _describe(expr),
        }
    except Exception as e:
        return {"ok": False, "error": f"Некорректный cron: {e}"}


def next_fire(expr: str, tz: str = "Europe/Moscow", after: datetime | None = None) -> datetime | None:
    if croniter is None or not expr:
        return None
    try:
        zone = resolve_timezone(tz)
        base = after or datetime.now(zone)
        if base.tzinfo is None:
            base = base.replace(tzinfo=zone)
        nxt = croniter(expr, base).get_next(datetime)
        if nxt.tzinfo is None:
            nxt = nxt.replace(tzinfo=zone)
        return nxt
    except Exception:
        return None


_DOW = {
    "0": "воскресенье", "1": "понедельник", "2": "вторник",
    "3": "среда", "4": "четверг", "5": "пятница",
    "6": "суббота", "7": "воскресенье",
}


def _describe(expr: str) -> str:
    """Tiny humanizer — good enough for presets, falls back to the raw cron."""
    parts = expr.split()
    if len(parts) != 5:
        return expr
    m, h, dom, mon, dow = parts
    if dom == "*" and mon == "*" and dow == "*" and m.isdigit() and h.isdigit():
        return f"Ежедневно в {int(h):02d}:{int(m):02d}"
    if dom == "*" and mon == "*" and dow in _DOW and m.isdigit() and h.isdigit():
        return f"Каждый(-ую) {_DOW[dow]} в {int(h):02d}:{int(m):02d}"
    if dom == "*" and mon == "*" and dow == "1-5" and m.isdigit() and h.isdigit():
        return f"По будням в {int(h):02d}:{int(m):02d}"
    return expr
"""Rendering and delivery of scheduled reports.

- In-app: always available, stores a `deliveries` row the UI reads.
- Email: via SMTP if credentials are configured in env.
"""
from __future__ import annotations

import asyncio
import logging
import os
import smtplib
import ssl
from email.message import EmailMessage
from html import escape
from typing import Any

log = logging.getLogger(__name__)

SMTP_HOST = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TLS = os.getenv("SMTP_TLS", "starttls").lower()  # starttls | ssl | none


def smtp_configured() -> bool:
    return bool(SMTP_HOST and SMTP_FROM)


MAX_PREVIEW_ROWS = 50


def _fmt_cell(v: Any) -> str:
    if v is None or v == "":
        return "—"
    if isinstance(v, float):
        if v.is_integer():
            return f"{int(v):,}".replace(",", " ")
        return f"{v:,.2f}".replace(",", " ")
    if isinstance(v, int):
        return f"{v:,}".replace(",", " ")
    return escape(str(v))


def render_report_html(
        *,
        report_name: str,
        question: str,
        sql: str,
        columns: list[str],
        rows: list[list[Any]],
        run_ts: str,
) -> str:
    headers = "".join(f"<th>{escape(c)}</th>" for c in columns)
    body_rows = "".join(
        "<tr>" + "".join(f"<td>{_fmt_cell(v)}</td>" for v in r) + "</tr>"
        for r in rows[:MAX_PREVIEW_ROWS]
    )
    truncated_note = (
        f"<p style='color:#6b7280'>Показаны первые {MAX_PREVIEW_ROWS} из "
        f"{len(rows)} строк.</p>"
        if len(rows) > MAX_PREVIEW_ROWS
        else ""
    )
    return f"""<!doctype html>
<html><body style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;color:#111827;background:#f6f7f9;padding:24px">
  <div style="max-width:720px;margin:auto;background:white;border-radius:12px;padding:24px;box-shadow:0 2px 12px rgba(0,0,0,0.05)">
    <h2 style="margin:0 0 4px 0">{escape(report_name)}</h2>
    <div style="color:#6b7280;font-size:13px">{escape(run_ts)} · Drivee Analytics</div>
    <div style="margin-top:16px;padding:12px;background:#f9fafb;border-left:4px solid #A3E635;border-radius:6px">
      <b>Вопрос:</b> {escape(question)}
    </div>
    <table style="width:100%;border-collapse:collapse;margin-top:16px;font-size:14px">
      <thead><tr style="background:#f3f4f6">{headers}</tr></thead>
      <tbody>{body_rows}</tbody>
    </table>
    {truncated_note}
    <details style="margin-top:20px">
      <summary style="cursor:pointer;color:#6b7280">Показать SQL</summary>
      <pre style="background:#1f2937;color:#e5e7eb;padding:12px;border-radius:6px;overflow-x:auto;font-size:12px">{escape(sql)}</pre>
    </details>
    <p style="color:#9ca3af;font-size:12px;margin-top:24px">
      Отчёт сгенерирован автоматически расписанием в Drivee Analytics.
    </p>
  </div>
</body></html>"""


def render_preview(columns: list[str], rows: list[list[Any]], limit: int = 5) -> str:
    if not rows:
        return "Нет данных"
    first = rows[0]
    if len(columns) == 1 and len(rows) == 1:
        return f"{columns[0]}: {_fmt_cell(first[0])}"
    head = ", ".join(columns[:3])
    samples = []
    for r in rows[:limit]:
        samples.append(" | ".join(_fmt_cell(v) for v in r[:3]))
    return f"{head}\n" + "\n".join(samples)


def _send_smtp_sync(to: str, subject: str, html: str) -> None:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to
    msg.set_content("HTML-отчёт. Откройте письмо в почтовом клиенте, поддерживающем HTML.")
    msg.add_alternative(html, subtype="html")

    if SMTP_TLS == "ssl":
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=ctx, timeout=20) as s:
            if SMTP_USER:
                s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)
    else:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as s:
            if SMTP_TLS == "starttls":
                s.starttls(context=ssl.create_default_context())
            if SMTP_USER:
                s.login(SMTP_USER, SMTP_PASSWORD)
            s.send_message(msg)


async def send_email(to: str, subject: str, html: str) -> None:
    if not smtp_configured():
        raise RuntimeError("SMTP не сконфигурирован (SMTP_HOST/SMTP_FROM не заданы)")
    await asyncio.to_thread(_send_smtp_sync, to, subject, html)
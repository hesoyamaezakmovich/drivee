import os
import re
import json
import time
import sqlite3
from pathlib import Path
from contextlib import asynccontextmanager

import yaml
import pandas as pd
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI

import reports_store
import notifications
from scheduler import ReportScheduler
from cron_utils import validate_cron, PRESETS

BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "drivee.db")

OLLAMA_URL = os.getenv("OLLAMA_URL", "http://localhost:11434/v1")
FAST_MODEL = os.getenv("FAST_MODEL", "qwen2.5-coder:1.5b")
STRONG_MODEL = os.getenv("STRONG_MODEL", "qwen2.5-coder:7b")
client = AsyncOpenAI(base_url=OLLAMA_URL, api_key="local")

with open(BASE_DIR / "semantic_layer.yaml", "r", encoding="utf-8") as f:
    SEMANTIC = yaml.safe_load(f)

def get_semantic_prompt():
    metrics = "\n".join([f"- {m['canonical']}: {m['sql']}" for m in SEMANTIC['metrics'].values()])
    return f"МЕТРИКИ:\n{metrics}"

SQL_SYSTEM = f"""Ты — Senior Data Analyst сервиса такси Drivee. 
Генерируешь SQLite-запросы. ОТВЕЧАЙ СТРОГО JSON ФОРМАТОМ. Никакого текста до или после.

СХЕМА:
TABLE orders (city_id int, order_id text, tender_id text, user_id text, driver_id text, status_order text, status_tender text, order_timestamp timestamp, distance_in_meters real, duration_in_seconds real, price_order_local real);

{get_semantic_prompt()}

ПРАВИЛА:
1. Только SELECT. Обязательно LIMIT 1000.
2. Используй точные формулы метрик из блока МЕТРИКИ (COUNT DISTINCT, CASE WHEN).
3. ПРАВИЛО ДЛЯ ДАТ: В базе данные за 2025/2026 годы. Если просят "за последние 7 дней" или "вчера", используй дату относительно максимума:
   WHERE DATE(order_timestamp) >= DATE((SELECT MAX(order_timestamp) FROM orders), '-7 days')
4. Если просят группировать "по городам", выводи city_id.

ФОРМАТ ОТВЕТА - СТРОГО JSON:
{{
  "sql": "SELECT ...",
  "explanation": "Что я считаю и как",
  "chart_type": "bar | line | pie | table",
  "confidence": 0.95
}}
"""

# ── Scheduler singleton ───────────────────────────────────────
_scheduler: ReportScheduler | None = None


async def _run_sql(sql: str) -> dict:
    """Execute SQL read-only and return columns + rows."""
    con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
    df = pd.read_sql_query(sql, con)
    con.close()
    return {"columns": list(df.columns), "rows": df.fillna("").values.tolist()}


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _scheduler
    reports_store.init_db()
    _scheduler = ReportScheduler(execute_sql=_run_sql)
    _scheduler.start()
    yield
    if _scheduler:
        _scheduler.shutdown()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Pydantic models ───────────────────────────────────────────

class QueryReq(BaseModel):
    question: str

class GhostReq(BaseModel):
    prefix: str

class ChipsReq(BaseModel):
    input: str = ""
    history: list[str] = []

class ReportCreate(BaseModel):
    name: str
    question: str
    sql: str
    chart_type: str | None = None
    schedule_cron: str | None = None
    timezone: str = "Europe/Moscow"
    recipients: list[str] = []
    enabled: bool = True

class ReportUpdate(BaseModel):
    name: str | None = None
    schedule_cron: str | None = None
    timezone: str | None = None
    recipients: list[str] | None = None
    enabled: bool | None = None
    chart_type: str | None = None

class InsightReq(BaseModel):
    question: str
    columns: list[str]
    rows: list[list]


# ── Guardrails ────────────────────────────────────────────────

def validate_sql(sql: str) -> str:
    try:
        parsed = sqlglot.parse_one(sql, dialect="sqlite")
    except Exception as e:
        raise ValueError(f"Синтаксическая ошибка: {e}")
    if not isinstance(parsed, exp.Select):
        raise ValueError("Разрешены только SELECT-запросы")
    allowed = set(SEMANTIC["rules"]["allowed_tables"])
    tables = {t.name.lower() for t in parsed.find_all(exp.Table)}
    if tables - allowed:
        raise ValueError(f"Доступ к запрещённым таблицам: {tables - allowed}")
    if not parsed.args.get("limit"):
        parsed = parsed.limit(SEMANTIC["rules"]["default_limit"])
    return parsed.sql(dialect="sqlite")


# ── /query ────────────────────────────────────────────────────

@app.post("/query")
async def query(req: QueryReq):
    t_start = time.time()
    try:
        response = await client.chat.completions.create(
            model=STRONG_MODEL,
            messages=[
                {"role": "system", "content": SQL_SYSTEM},
                {"role": "user", "content": f"Вопрос: {req.question}"}
            ],
            temperature=0.1,
            response_format={"type": "json_object"}
        )
        content = response.choices[0].message.content
        result = json.loads(content)
    except Exception as e:
        raise HTTPException(500, f"Ошибка LLM: {str(e)}")

    sql_raw = result.get("sql", "")
    try:
        sql_safe = validate_sql(sql_raw)
    except Exception as e:
        return {"error": "guardrails", "message": str(e), "sql": sql_raw,
                "explanation": result.get("explanation")}

    try:
        df = (await _run_sql(sql_safe))
    except Exception as e:
        return {"error": "db", "message": str(e), "sql": sql_safe}

    return {
        "question": req.question,
        "sql": sql_safe,
        "explanation": result.get("explanation"),
        "chart_type": result.get("chart_type", "table"),
        "confidence": result.get("confidence", 0.9),
        "data": df,
        "timings": {"total_ms": int((time.time() - t_start) * 1000)}
    }


# ── /suggest/ghost ────────────────────────────────────────────
#
# Prompt uses English instructions + "Input/Output" format which forces
# completion behaviour in any model — small models treat Russian text as
# a conversation starter, but this structure makes it a fill-in-the-blank task.

GHOST_SYSTEM = """You complete Russian analytics queries for a taxi service.
Given partial user input, output ONLY the missing continuation — nothing else.
No explanations, no quotes, no punctuation at the end.

Rules:
- If the input cuts off mid-word: complete that word first, then add context.
- If the input ends on a complete word or space: start your output with a space, then the next words.
- Maximum 8 words total. Russian language only.

Input: отм
Output: ены по городам за неделю

Input: покажи выр
Output: учку по городам за месяц

Input: топ водит
Output: елей по поездкам за неделю

Input: динамик
Output: а выручки за последние 30 дней

Input: доля отм
Output: ен по городам за месяц

Input: сравни отм
Output: ены в этом месяце с прошлым

Input: средний чек по
Output:  классам машин за месяц

Input: отмены по городам
Output:  за последнюю неделю

Input: покажи выручку
Output:  по городам за месяц

Input: сравни отмены
Output:  в этом месяце с прошлым

Input: динамика
Output:  выручки за последние 30 дней

Input: топ водителей
Output:  по поездкам за неделю"""


@app.post("/suggest/ghost")
async def suggest_ghost(req: GhostReq):
    if len(req.prefix.strip()) < 2:
        async def empty():
            yield "data: [DONE]\n\n"
        return StreamingResponse(empty(), media_type="text/event-stream")

    async def iter_tokens():
        try:
            response = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[
                    {"role": "system", "content": GHOST_SYSTEM},
                    # "Output:" at the end forces the model to continue from there
                    {"role": "user", "content": f"Input: {req.prefix}\nOutput:"},
                ],
                temperature=0.05,
                max_tokens=20,
                stream=True,
                stop=["\n", "Input:", "Output:"],
            )
            async for chunk in response:
                txt = chunk.choices[0].delta.content or ""
                if txt:
                    yield f"data: {json.dumps({'text': txt}, ensure_ascii=False)}\n\n"
        except Exception:
            pass
        yield "data: [DONE]\n\n"

    return StreamingResponse(iter_tokens(), media_type="text/event-stream")


INSIGHT_SYSTEM = """Ты — аналитик сервиса такси Drivee. По данным SQL-отчёта напиши 2-3 предложения на русском: главный вывод, важный тренд или аномалию. Используй конкретные числа из данных. Только текст — без заголовков, без списков, без лишних слов."""

CHIPS_SYSTEM = """Ты — помощник аналитика сервиса такси Drivee. Предлагай 4 коротких вопроса на русском для анализа данных.
Доступные метрики: заказы, отмены, выручка, завершённые поездки, средняя дистанция.
Измерения: города, дата, час.

Отвечай СТРОГО JSON: {"suggestions": ["вопрос1", "вопрос2", "вопрос3", "вопрос4"]}
Вопросы должны быть короткими (5-8 слов), конкретными и разными."""


@app.post("/suggest/chips")
async def suggest_chips(req: ChipsReq):
    last_q = req.history[-1] if req.history else ""
    user_msg = f'Ввод: "{req.input}"\nПоследний вопрос: "{last_q}"'
    try:
        response = await client.chat.completions.create(
            model=FAST_MODEL,
            messages=[
                {"role": "system", "content": CHIPS_SYSTEM},
                {"role": "user", "content": user_msg}
            ],
            temperature=0.5,
            max_tokens=200,
        )
        raw = response.choices[0].message.content or ""
        m = re.search(r"\{.*\}", raw, re.DOTALL)
        if m:
            suggestions = json.loads(m.group(0)).get("suggestions", [])
            suggestions = [s.strip().strip('"') for s in suggestions if s.strip()][:4]
            return {"suggestions": suggestions}
    except Exception:
        pass
    # fallback to static
    return {"suggestions": [
        "Покажи отмены по городам за неделю",
        "Динамика выручки за последний месяц",
        "Топ водителей по поездкам",
        "Средний чек по классам машин",
    ]}


# ── /insight ─────────────────────────────────────────────────

@app.post("/insight")
async def generate_insight(req: InsightReq):
    preview = req.rows[:20]
    header = "\t".join(req.columns)
    body = "\n".join("\t".join(str(v) for v in row) for row in preview)
    suffix = f"\n(и ещё {len(req.rows) - 20} строк)" if len(req.rows) > 20 else ""
    try:
        resp = await client.chat.completions.create(
            model=FAST_MODEL,
            messages=[
                {"role": "system", "content": INSIGHT_SYSTEM},
                {"role": "user", "content": f"Вопрос: {req.question}\n\nДанные:\n{header}\n{body}{suffix}"}
            ],
            temperature=0.3,
            max_tokens=150,
        )
        return {"insight": (resp.choices[0].message.content or "").strip()}
    except Exception as e:
        return {"insight": None, "error": str(e)}


# ── Reports CRUD ──────────────────────────────────────────────

@app.get("/reports")
async def list_reports():
    return [r.to_dict() for r in reports_store.list_reports()]


@app.post("/reports")
async def create_report(body: ReportCreate):
    validated_sql = validate_sql(body.sql)
    r = reports_store.create_report(
        name=body.name,
        question=body.question,
        sql=validated_sql,
        chart_type=body.chart_type,
        schedule_cron=body.schedule_cron,
        timezone=body.timezone,
        recipients=body.recipients,
        enabled=body.enabled,
    )
    if r.enabled and r.schedule_cron and _scheduler:
        _scheduler.schedule(r.id, r.schedule_cron, r.timezone or "Europe/Moscow")
    return r.to_dict()


@app.get("/reports/{report_id}")
async def get_report(report_id: int):
    r = reports_store.get_report(report_id)
    if not r:
        raise HTTPException(404, "Report not found")
    return r.to_dict()


@app.patch("/reports/{report_id}")
async def update_report(report_id: int, body: ReportUpdate):
    fields = {k: v for k, v in body.model_dump().items() if v is not None}
    r = reports_store.update_report(report_id, **fields)
    if not r:
        raise HTTPException(404, "Report not found")
    if _scheduler:
        _scheduler.sync(report_id)
    return r.to_dict()


@app.delete("/reports/{report_id}")
async def delete_report(report_id: int):
    if _scheduler:
        _scheduler.unschedule(report_id)
    ok = reports_store.delete_report(report_id)
    if not ok:
        raise HTTPException(404, "Report not found")
    return {"ok": True}


@app.post("/reports/{report_id}/run")
async def run_report_now(report_id: int):
    if not _scheduler:
        raise HTTPException(503, "Scheduler not ready")
    result = await _scheduler.run_and_dispatch(report_id, trigger="manual")
    return result


@app.get("/reports/{report_id}/runs")
async def get_runs(report_id: int):
    return reports_store.list_runs(report_id)


@app.get("/reports/{report_id}/runs/{run_id}")
async def get_run_detail(report_id: int, run_id: int):
    run = reports_store.get_run(run_id)
    if not run:
        raise HTTPException(404, "Run not found")
    return run


# ── Cron helpers ──────────────────────────────────────────────

@app.get("/cron/presets")
async def cron_presets():
    return PRESETS


@app.post("/cron/validate")
async def cron_validate(body: dict):
    return validate_cron(body.get("expr", ""), body.get("tz", "Europe/Moscow"))


# ── Inbox ─────────────────────────────────────────────────────

@app.get("/inbox")
async def inbox(unread_only: bool = False):
    return reports_store.list_deliveries(limit=50, unread_only=unread_only)


@app.get("/inbox/unread_count")
async def inbox_unread():
    return {"count": reports_store.count_unread_deliveries()}


@app.post("/inbox/{delivery_id}/read")
async def mark_read(delivery_id: int):
    reports_store.mark_delivery_read(delivery_id)
    return {"ok": True}


# ── Health ────────────────────────────────────────────────────

@app.get("/health")
async def health():
    try:
        models_resp = await client.models.list()
        model_ids = [m.id for m in models_resp.data]
        ollama_ok = True
    except:
        model_ids = []
        ollama_ok = False
    return {
        "ollama": ollama_ok,
        "db": Path(DB_PATH).exists(),
        "reports_db": Path(reports_store.DB_PATH).exists(),
        "ollama_models": model_ids,
        "fast_loaded": True,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
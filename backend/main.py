import os
import re
import json
import time
import sqlite3
from pathlib import Path

import yaml
import pandas as pd
import sqlglot
from sqlglot import exp
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from openai import AsyncOpenAI

BASE_DIR = Path(__file__).parent
DB_PATH = str(BASE_DIR / "data" / "drivee.db")

OLLAMA_URL = "http://localhost:11434/v1"
FAST_MODEL = "qwen2.5-coder:1.5b"
STRONG_MODEL = "qwen2.5-coder:7b"
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

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

class QueryReq(BaseModel):
    question: str

class GhostReq(BaseModel):
    prefix: str

# === GUARDRAILS ===
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
        raise ValueError(f"Доступ к запрещенным таблицам: {tables - allowed}")
    
    if not parsed.args.get("limit"):
        parsed = parsed.limit(SEMANTIC["rules"]["default_limit"])
        
    return parsed.sql(dialect="sqlite")

# === ЭНДПОИНТЫ ===
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
        
        print("\n" + "="*40)
        print(f"ВОПРОС: {req.question}")
        print(f"ОТВЕТ LLM:\n{content}")
        print("="*40 + "\n")
        
        result = json.loads(content)
    except Exception as e:
        raise HTTPException(500, f"Ошибка LLM: {str(e)}")

    sql_raw = result.get("sql", "")
    
    try:
        sql_safe = validate_sql(sql_raw)
    except Exception as e:
        return {"error": "guardrails", "message": str(e), "sql": sql_raw, "explanation": result.get("explanation")}

    try:
        con = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        df = pd.read_sql_query(sql_safe, con)
        con.close()
    except Exception as e:
        return {"error": "db", "message": str(e), "sql": sql_safe}

    return {
        "question": req.question,
        "sql": sql_safe,
        "explanation": result.get("explanation"),
        "chart_type": result.get("chart_type", "table"),
        "confidence": result.get("confidence", 0.9),
        "data": {
            "columns": list(df.columns),
            "rows": df.fillna("").values.tolist(),
        },
        "timings": {"total_ms": int((time.time() - t_start) * 1000)}
    }

@app.post("/suggest/ghost")
async def suggest_ghost(req: GhostReq):
    async def iter_tokens():
        try:
            response = await client.chat.completions.create(
                model=FAST_MODEL,
                messages=[
                    {"role": "system", "content": "Продолжи запрос аналитики 2-5 словами. Не повторяй начало. Выведи только продолжение."},
                    {"role": "user", "content": f'"{req.prefix}"'}
                ],
                temperature=0.2,
                max_tokens=20,
                stream=True
            )
            async for chunk in response:
                txt = chunk.choices[0].delta.content or ""
                if txt:
                    yield f"data: {json.dumps({'text': txt}, ensure_ascii=False)}\n\n"
        except:
            pass
        yield "data: [DONE]\n\n"

    return StreamingResponse(iter_tokens(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

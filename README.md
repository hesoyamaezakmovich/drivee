# Drivee NL2SQL — MVP для конкурса «Моя профессия – ИТ»

Естественно-языковой интерфейс к аналитической БД. Пользователь пишет
«покажи отмены по городам за прошлую неделю» — система генерирует SQL,
выполняет его и рисует график. Три уровня подсказок на базе LLM
(ghost-text, chips, финальная генерация SQL), жёсткие guardrails через
sqlglot, семантический слой для точности.

## Структура

```
drivee-nl2sql/
├── backend/
│   ├── main.py               # FastAPI: все эндпоинты + пайплайн
│   ├── semantic_layer.yaml   # Бизнес-словарь: метрики, измерения, периоды
│   ├── seed_db.py            # Генератор тестовой БД (42 000 заказов)
│   └── requirements.txt
├── frontend/
│   └── index.html            # React + Chart.js одним файлом (без npm)
├── data/
│   ├── drivee.db             # SQLite с тестовыми данными
│   └── reports.db            # Сохранённые отчёты
├── setup.bat                 # Первый запуск (зависимости + БД)
└── run_server.bat            # Запуск сервера
```

## Быстрый старт на Windows

### 1. Установить Ollama

Скачайте `OllamaSetup.exe` с https://ollama.com/download/windows и
запустите. После установки Ollama работает как фоновый сервис.

### 2. Скачать модели

Откройте PowerShell и выполните:

```powershell
ollama pull qwen2.5-coder:1.5b
ollama pull qwen2.5-coder:7b
```

Первая (1.5B, ~900 МБ) — для ghost-text и chips.  
Вторая (7B, ~4.5 ГБ) — для финальной генерации SQL.

Если 7B слишком тяжёлая для вашего железа, используйте `qwen2.5-coder:3b`
и поправьте `STRONG_MODEL` в `backend/main.py`.

### 3. Настройка в PyCharm

1. Откройте папку проекта в PyCharm (File → Open → папка `drivee-nl2sql`).
2. PyCharm предложит создать venv — согласитесь.
3. Откройте Terminal внутри PyCharm: `pip install -r backend/requirements.txt`
4. Запустите `python backend/seed_db.py` один раз — создастся тестовая БД.
5. Создайте Run Configuration:
   - Type: **FastAPI**
   - Application file: `backend/main.py`
   - Host: `0.0.0.0`, Port: `8000`
6. Запускайте зелёной кнопкой. Сервер поднимется на http://localhost:8000

Альтернативно — используйте `setup.bat` и `run_server.bat` из корня проекта
(двойной клик).

### 4. Открыть фронт

Откройте `frontend/index.html` двойным кликом — он запустится в браузере
и подключится к localhost:8000. Всё, можно начинать задавать вопросы.

## Проверка, что всё работает

Откройте http://localhost:8000/health — должно быть:

```json
{
  "ollama": true,
  "db": true,
  "reports_db": true,
  "ollama_models": ["qwen2.5-coder:1.5b", "qwen2.5-coder:7b"],
  "fast_loaded": true
}
```

Откройте http://localhost:8000/docs — встроенный Swagger для всех API.

## Демо-сценарий (для защиты)

Порядок запросов, которые хорошо раскрывают все фичи:

1. **«покажи отмены по городам за прошлую неделю»** — базовый сценарий, bar chart.
2. **«динамика выручки за последний месяц»** — line chart, видна сезонность.
3. **«топ 10 водителей по поездкам за неделю»** — таблица, автовыбор работает.
4. **«доля отмен по классам машин»** — pie chart, выражение метрики из семантики.
5. **«а теперь только по Москве»** — follow-up с учётом истории.
6. (опционально, для wow) **попробовать удалить заказ** — `DELETE FROM orders`
   через поле запроса → система покажет guardrail в действии.

## Настройка под другой LLM

Все переменные окружения можно задать в `.env` или через PyCharm Run
Configuration. Ключевые:

```
OLLAMA_URL=http://localhost:11434        # где крутится Ollama
FAST_MODEL=qwen2.5-coder:1.5b            # для ghost/chips
STRONG_MODEL=qwen2.5-coder:7b            # для финальной генерации
STRONG_PROVIDER=ollama                   # ollama | openai | groq
```

### Переключение на Groq для уровня 3 (быстрее)

1. Получите ключ на https://console.groq.com
2. В переменных окружения:
   ```
   STRONG_PROVIDER=openai
   OPENAI_KEY=gsk_...
   OPENAI_URL=https://api.groq.com/openai/v1/chat/completions
   OPENAI_MODEL=llama-3.3-70b-versatile
   ```

## Что реализовано по критериям оценки

| Критерий | Что сделано |
|---|---|
| №1 Ценность (15) | Чат с инсайтами, follow-ups, UX «как с коллегой» |
| №2 MVP (20) | Работающий прототип от ввода до графика |
| №3 NL→SQL (20) | Семантический слой + RAG из few-shot + confidence |
| №4 Корректность (15) | sqlglot-валидация AST, адаптация PG→SQLite |
| №5 Безопасность (15) | SELECT-only, whitelist таблиц, read-only БД, PII-защита, timeout |
| №6 UX (10) | Ghost text, chips, объяснения, confidence-индикатор |
| №7 Демо (5) | Описано в этом README |
| №8 Расписание (+5) | CRUD отчётов с полем schedule_cron |
| №9 Семантика (+5) | YAML с метриками/измерениями/периодами/сущностями |
| №10 Confidence (+5) | Модель сама оценивает + clarification_needed |
| №11 Шаблоны (+5) | `templates:` в YAML, chips на главной |

## Что можно добавить дальше

- **APScheduler** для реальных рассылок по расписанию (эндпоинт CRUD уже есть).
- **Chroma** для RAG-поиска по сохранённым парам «вопрос → SQL» (обучение на правках).
- **Лемматизация** через pymorphy3 для fuzzy-match сущностей (Москве → Москва).
- **Agentic SQL-критик** (вторая LLM проверяет первую перед выполнением).
- **Row-level security**: автоинъекция `WHERE city = user.region` в SQL.

## Частые проблемы

**«Connection refused» к Ollama** — проверьте что Ollama запущена:
в трее должна быть иконка или `ollama ps` в PowerShell что-то показывает.

**Модель работает на CPU** — `ollama ps` покажет. Если есть NVIDIA GPU,
обновите драйверы до последних. Для AMD GPU на Windows Ollama пока не работает.

**CORS ошибка в браузере** — не должно быть, т.к. в main.py разрешены все
origins. Если всё же возникла, проверьте что открываете `file://.../index.html`
а не через какой-то dev-сервер на другом порту.

**Кириллица в путях** — если логин Windows на русском
(`C:\Users\Иван\...`), переложите проект в `C:\Dev\drivee-nl2sql`.

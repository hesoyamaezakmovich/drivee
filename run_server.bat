@echo off
chcp 65001 > nul
cd backend
echo Запускаю сервер на http://localhost:8000
echo API docs: http://localhost:8000/docs
echo.
echo Открой frontend/index.html в браузере
echo (двойной клик по файлу или перетащи в Chrome)
echo.
python -m uvicorn main:app --reload --host 0.0.0.0 --port 8000

@echo off
chcp 65001 > nul
echo === Drivee NL2SQL: первый запуск ===
echo.

cd backend

echo [1/3] Устанавливаю зависимости Python...
pip install -r requirements.txt
if errorlevel 1 goto error

echo.
echo [2/3] Генерирую тестовую БД...
python seed_db.py
if errorlevel 1 goto error

echo.
echo [3/3] Проверяю Ollama...
curl -s http://localhost:11434/api/tags > nul
if errorlevel 1 (
    echo !!! Ollama не запущена. Запустите её и скачайте модель:
    echo     ollama pull qwen2.5-coder:1.5b
    echo     ollama pull qwen2.5-coder:7b
    pause
    exit /b
)

echo.
echo === Готово! Запускайте сервер через run_server.bat ===
pause
exit /b

:error
echo Произошла ошибка, проверьте вывод выше.
pause

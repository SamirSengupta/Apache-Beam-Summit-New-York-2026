@echo off
cd /d "%~dp0"
title Fraud Dashboard server

REM ===================== CHOOSE YOUR MODEL =====================
REM List models first (in any terminal):  curl http://127.0.0.1:8317/v1/models
REM Then paste the model id you want below (avoid the slow "thinking" one).
set "LLM_MODEL=gemini-3-flash"
REM Optional overrides (usually leave as-is):
set "LLM_BASE_URL=http://127.0.0.1:8317/v1"
set "LLM_API_KEY=dummy"
REM ============================================================

echo ============================================================
echo    Live Fraud Dashboard  (real RAG + real LLM)
echo    model: %LLM_MODEL%
echo ============================================================
echo The browser opens automatically in about 5 seconds.
echo If it says "refused to connect", wait a moment and refresh.
echo.

if exist ".venv\Scripts\activate.bat" call ".venv\Scripts\activate.bat"

start "" /min cmd /c "timeout /t 5 >nul & start http://127.0.0.1:8800"

python dashboard_server.py

echo.
echo Server stopped. Press any key to close.
pause >nul

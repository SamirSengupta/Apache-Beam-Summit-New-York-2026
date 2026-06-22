@echo off
setlocal enabledelayedexpansion
cd /d "%~dp0"

echo ============================================================
echo    Real-Time Fraud Detection  -  Beam Summit demo
echo ============================================================

REM --- OPTIONAL: auto-start your antigravity proxy --------------------
REM If the proxy is NOT already running, put its start command below
REM (delete the word REM in front of the "start" line and edit it):
REM start "antigravity" cmd /k antigravity serve
REM -------------------------------------------------------------------

echo Waiting for the LLM proxy on 127.0.0.1:8317 ...
set /a tries=0
:waitproxy
curl -s -o nul -m 1 http://127.0.0.1:8317/v1/models && goto proxyok
set /a tries+=1
if !tries! GEQ 12 (
  echo Proxy not detected - the demo will use the built-in rule-based fallback.
  goto rundemo
)
timeout /t 1 >nul
goto waitproxy
:proxyok
echo Proxy is up.

:rundemo
call .venv\Scripts\activate.bat
python advanced_pipeline.py
if errorlevel 1 (
  echo.
  echo Advanced pipeline hit an error - running the simple version instead...
  python realtime_fraud_rag_beam.py
)
echo.
echo Demo finished. Press any key to close.
pause >nul

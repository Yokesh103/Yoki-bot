@echo off
TITLE YokiBot Commander
COLOR 0A
CLS

echo ===================================================
echo       ROCKET LAUNCHER FOR YOKIBOT 🚀
echo ===================================================
echo.

:: 1. UPDATE DATA
echo [1/6] Checking for Updates (CSV)...
python tools/update_data.py
IF %ERRORLEVEL% NEQ 0 (
    echo ⚠️ CSV Update Failed or Script missing. Continuing...
) ELSE (
    echo ✅ Data Updated.
)
echo.

:: 2. REDIS
echo [2/6] Starting Redis Database...
start "1. Redis Server" redis-server
timeout /t 3 /nobreak >nul

:: 3. LIVE FEED
echo [3/6] Starting Broker Gateway (Live Feed)...
cd live_feed_microservice
start "2. Broker Feed" cmd /k "uvicorn app.main:app --port 8000 --reload"
cd ..
timeout /t 2 /nobreak >nul

:: 4. OPTION CHAIN
echo [4/6] Starting Option Chain Service...
cd optionchain-service
start "3. Option Chain" cmd /k "uvicorn app.main:app --port 8100 --reload"
cd ..

:: 5. GREEKS
echo [5/6] Starting Greeks Engine...
cd greeks-service
start "4. Greeks Engine" cmd /k "uvicorn main:app --port 8200 --reload"
cd ..

:: 6. SIGNAL ENGINE & PAPER
echo [6/6] Starting Signal Brain & Paper Trading...
cd paper-exec
start "5. Paper Exec" cmd /k "uvicorn main:app --port 8400 --reload"
cd ..

cd signal-engine
start "6. Signal Brain" cmd /k "uvicorn app.main:app --port 9000 --reload"
cd ..

echo.
echo ===================================================
echo    ALL SYSTEMS DEPLOYED. LAUNCHING DASHBOARD...
echo ===================================================
echo.

:: 7. DASHBOARD
streamlit run dashboard_ui.py
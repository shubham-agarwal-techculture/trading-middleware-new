@echo off
REM Resolve the port where webhook/server.js is listening.
REM Waits for .webhook_http_port written by the Node webhook on bind.
setlocal EnableDelayedExpansion

set "ROOT=%~dp0.."
set "PORT_FILE=%ROOT%\.webhook_http_port"
set "FALLBACK=%~1"
if "%FALLBACK%"=="" set "FALLBACK=5001"
set "MAX_WAIT=%~2"
if "%MAX_WAIT%"=="" set "MAX_WAIT=45"
set "RESOLVED="
set "WAIT=0"

:wait_loop
if exist "%PORT_FILE%" (
    set /p RESOLVED=<"%PORT_FILE%"
    if not "!RESOLVED!"=="" goto :found
)
if !WAIT! GEQ %MAX_WAIT% goto :timeout
timeout /t 1 /nobreak >nul
set /a WAIT+=1
goto :wait_loop

:found
echo [webhook-port] Using port !RESOLVED! from %PORT_FILE%
for /f "delims=" %%P in ("!RESOLVED!") do (
    endlocal
    set "WEBHOOK_PORT=%%P"
    exit /b 0
)

:timeout
echo [webhook-port] Timed out after %MAX_WAIT%s waiting for %PORT_FILE%
echo [webhook-port] Is the webhook running? cd webhook ^&^& node server.js
for /f "delims=" %%P in ("%FALLBACK%") do (
    endlocal
    set "WEBHOOK_PORT=%%P"
    exit /b 1
)

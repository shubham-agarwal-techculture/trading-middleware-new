@echo off
REM Open the dashboard on the port where webhook/server.js is listening.
setlocal

set "FALLBACK=%~1"
if "%FALLBACK%"=="" set "FALLBACK=5001"

call "%~dp0resolve_webhook_port.bat" %FALLBACK% 45
set "PORT=%WEBHOOK_PORT%"
if "%PORT%"=="" set "PORT=%FALLBACK%"

echo [dashboard] Opening http://localhost:%PORT%
start "" "http://localhost:%PORT%"
endlocal

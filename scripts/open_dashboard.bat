@echo off
REM Open the dashboard on the port chosen by webhook/server.js (or fallback).
setlocal EnableDelayedExpansion

set "ROOT=%~dp0.."
set "PORT_FILE=%ROOT%\.webhook_http_port"
set "FALLBACK=%~1"
if "%FALLBACK%"=="" set "FALLBACK=5001"
set "PORT=%FALLBACK%"
set "WAIT=0"
set "MAX_WAIT=20"

:wait_port
if exist "%PORT_FILE%" (
    set /p PORT=<"%PORT_FILE%"
    if not "!PORT!"=="" goto :open
)
if !WAIT! GEQ %MAX_WAIT% goto :open
timeout /t 1 /nobreak >nul
set /a WAIT+=1
goto :wait_port

:open
echo [dashboard] Opening http://localhost:!PORT!
start "" "http://localhost:!PORT!"
endlocal

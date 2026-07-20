@echo off
REM Start ngrok for the webhook/dashboard port.
REM Stops any existing ngrok.exe first to avoid session / port conflicts.
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
    if not "!PORT!"=="" goto :port_ready
)
if !WAIT! GEQ %MAX_WAIT% goto :port_ready
timeout /t 1 /nobreak >nul
set /a WAIT+=1
goto :wait_port

:port_ready
if not "!PORT!"=="!FALLBACK!" (
    echo [ngrok] Webhook auto-selected port !PORT! ^(preferred !FALLBACK!^)
)

set "RETRIES=0"
set "MAX_RETRIES=5"

call :stop_ngrok
goto :start_ngrok

:stop_ngrok
tasklist /FI "IMAGENAME eq ngrok.exe" 2>nul | find /I "ngrok.exe" >nul
if not errorlevel 1 (
    echo [ngrok] Stopping existing ngrok process...
    taskkill /F /IM ngrok.exe >nul 2>&1
    timeout /t 1 /nobreak >nul
)
exit /b 0

:start_ngrok
echo [ngrok] Starting tunnel: localhost:!PORT!
ngrok http !PORT!
set "EC=!ERRORLEVEL!"

if "!EC!"=="0" goto :done

set /a RETRIES+=1
if !RETRIES! GEQ %MAX_RETRIES% (
    echo [ngrok] Failed after %MAX_RETRIES% attempts. Last exit code: !EC!
    goto :done
)

echo [ngrok] Exited with code !EC! — conflict or error. Retrying (!RETRIES!/%MAX_RETRIES%)...
call :stop_ngrok
timeout /t 2 /nobreak >nul
goto :start_ngrok

:done
endlocal

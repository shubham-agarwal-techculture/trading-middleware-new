@echo off
REM Start ngrok on the same port as the local webhook/dashboard (auto-detected).
setlocal EnableDelayedExpansion

set "FALLBACK=%~1"
if "%FALLBACK%"=="" set "FALLBACK=5001"
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

:resolve_port
call "%~dp0resolve_webhook_port.bat" %FALLBACK% 45
set "PORT=%WEBHOOK_PORT%"
if "%PORT%"=="" set "PORT=%FALLBACK%"
exit /b 0

:start_ngrok
call :resolve_port
echo [ngrok] Tunnel target: localhost:!PORT! (webhook/dashboard)
ngrok http !PORT!
set "EC=!ERRORLEVEL!"

if "!EC!"=="0" goto :done

set /a RETRIES+=1
if !RETRIES! GEQ %MAX_RETRIES% (
    echo [ngrok] Failed after %MAX_RETRIES% attempts. Last exit code: !EC!
    goto :done
)

echo [ngrok] Exited with code !EC! — retrying (!RETRIES!/%MAX_RETRIES%)...
call :stop_ngrok
timeout /t 2 /nobreak >nul
goto :start_ngrok

:done
endlocal

@echo off

set ROOT=%~dp0
set ROOT=%ROOT:~0,-1%
set VENV=%ROOT%\.venv\Scripts\activate.bat

wt ^
new-tab --title "OMS Server" cmd /k "cd /d %ROOT% && call %VENV% && python run_oms.py" ^
; new-tab --title "Masters Data" cmd /k "cd /d %ROOT% && call %VENV% && python -m market_data.download_masters" ^
; new-tab --title "Webhook" cmd /k "cd /d %ROOT%\webhook && node server.js" ^
; new-tab --title "Ngrok" cmd /k "cd /d %ROOT% && scripts\ngrok_tunnel.bat" ^

; new-tab --title "Signal Bridge" cmd /k "cd /d %ROOT% && call %VENV% && python run_bridge.py"

REM Wait for webhook to bind, then open dashboard on its actual port
call scripts\open_dashboard.bat 5001

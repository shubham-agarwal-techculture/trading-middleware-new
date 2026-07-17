@echo off

set ROOT=D:\projects-shubham\02.06.2026\trading_middleware-new
set VENV=%ROOT%\.venv\Scripts\activate.bat

wt ^
new-tab --title "OMS Server" cmd /k "cd /d %ROOT% && call %VENV% && python oms_server.py" ^
; new-tab --title "Masters Data" cmd /k "cd /d %ROOT% && call %VENV% && python get_masters_data.py" ^
; new-tab --title "Webhook" cmd /k "cd /d %ROOT%\webhook && node index.js" ^
; new-tab --title "Ngrok" cmd /k "ngrok http 5001" ^
; new-tab --title "Signal Bridge" cmd /k "cd /d %ROOT% && call %VENV% && python nifty_signal_bridge.py"

REM Wait 3 seconds for the server to start, then open the dashboard
timeout /t 3 /nobreak >nul
start http://localhost:5001

@echo off

set ROOT=D:\projects-shubham\02.06.2026\trading_middleware
set VENV=%ROOT%\.venv\Scripts\activate.bat

wt ^
new-tab --title "Masters Data" cmd /k "cd /d %ROOT% && call %VENV% && python get_masters_data.py" ^
; new-tab --title "Webhook" cmd /k "cd /d %ROOT%\webhook && node index.js" ^
; new-tab --title "Ngrok" cmd /k "ngrok http 5001" ^
; new-tab --title "Signal Bridge" cmd /k "cd /d %ROOT% && call %VENV% && python nifty_signal_bridge.py"
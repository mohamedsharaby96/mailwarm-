@echo off
title MailWarm - Setup
color 0A

:: Go to the folder where this bat file lives (fixes System32 issue)
cd /d "%~dp0"

echo.
echo  ========================================
echo    MailWarm - Installing and Starting
echo  ========================================
echo.

echo Installing flask...
pip install flask
echo.

echo Installing flask-cors...
pip install flask-cors
echo.

echo Installing openai...
pip install openai
echo.

echo Installing PySocks...
pip install PySocks
echo.

echo  ========================================
echo    Starting MailWarm server...
echo    Open http://localhost:5000
echo    Do NOT close this window!
echo  ========================================
echo.

python server.py

echo.
echo  Server stopped.
pause

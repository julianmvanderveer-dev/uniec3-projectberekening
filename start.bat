@echo off
cd /d "%~dp0"
echo Installeren...
python -m pip install -r requirements.txt --quiet
echo.
echo Starten op http://127.0.0.1:5000
python app.py
pause

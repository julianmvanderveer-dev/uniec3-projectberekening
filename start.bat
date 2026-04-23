@echo off
cd /d "%~dp0"

:: ── Omgevingsvariabelen (alleen lokaal) ──────────────────────────────────────
set MOLLIE_API_KEY=test_hSuGbKgKEzbKt5nsUW8yraKPGVxxHd
set ADMIN_KEY=Oranjelaan3g!
set SECRET_KEY=uniec3-flask-secret-k9x2w7p4m1q8

:: ── Afhankelijkheden installeren ─────────────────────────────────────────────
echo Installeren...
python -m pip install -r requirements.txt --quiet

:: ── Starten ──────────────────────────────────────────────────────────────────
echo.
echo Tool gestart op: http://127.0.0.1:5000
echo Admin-pagina:    http://127.0.0.1:5000/admin?key=Oranjelaan3g!
echo.
echo Let op: Mollie-webhooks werken niet lokaal.
echo De betaalstatus wordt wel opgehaald zodra je terugkeert van iDEAL.
echo.
python app.py
pause

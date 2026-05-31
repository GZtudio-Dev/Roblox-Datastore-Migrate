@echo off
title GZtudio — Datastore Migrator
echo.
echo  +-----------------------------------------+
echo  ^|   GZtudio - Datastore Migrator  v1.0   ^|
echo  +-----------------------------------------+
echo.

:: Cek Python
python --version >nul 2>&1
if errorlevel 1 (
    echo  [ERROR] Python tidak ditemukan!
    echo  Download Python di: https://www.python.org/downloads/
    echo  Pastikan centang "Add Python to PATH" saat install.
    pause
    exit /b
)

echo  [OK] Python ditemukan
echo  [..] Installing dependencies...
pip install -r requirements.txt --quiet
echo  [OK] Dependencies ready
echo.
echo  [..] Starting server...
echo  [OK] Buka browser: http://localhost:5000
echo.

:: Tunggu 2 detik baru buka browser
timeout /t 2 /nobreak >nul
start "" http://localhost:5000

python app.py

@echo off
setlocal enabledelayedexpansion
title BigDataLab03 - Laboratorio de Big Data

rem Este archivo es la puerta de entrada del proyecto, el que se abre con doble clic.
rem Esta escrito a proposito sin ofuscacion y sin descargar nada, porque esas dos cosas
rem son las que hacen que Windows lo marque como sospechoso. No hay ninguna llamada
rem reflejada ni codigo empaquetado en base64, y no hay ninguna orden que baje un archivo
rem y lo ejecute de una. Todo lo que hay aca es mirar si las piezas estan y, si estan,
rem correr las que ya estan escritas.

set "REPO=%~dp0"
set "PY=%REPO%.venv\Scripts\python.exe"
set "BOOTSTRAP=%REPO%bootstrap.ps1"

echo.
echo =============================================
echo   BigDataLab03
echo   Laboratorio 03 de Big Data
echo =============================================
echo.

rem Un archivo descargado del navegador trae una marca que Windows llama Mark of the Web
rem y por eso Windows lo bloquea al abrirlo. Esto no es un virus, es que el archivo vino de
rem afuera y Windows no lo conoce. Si te paso, la linea de abajo le saca la marca.
powershell -NoProfile -Command "Get-ChildItem -LiteralPath '%REPO%' -Recurse -Include *.bat,*.ps1 -ErrorAction SilentlyContinue | Unblock-File" 2>nul

rem El paso uno es mirar, no instalar. Si falta algo lo decimos y paramos, porque instalar
rem cosas sin preguntar es justo lo que hace que un equipo de confianza se vuelva sospechoso
if not exist "%PY%" (
    echo FALTA la venv, todavia no se instalo el entorno.
    echo.
    echo Para instalarla, corre esto en su lugar:
    echo     powershell -ExecutionPolicy Bypass -File "%BOOTSTRAP%"
    echo.
    pause
    exit /b 1
)

if not exist "%REPO%winutils\bin\winutils.dll" (
    echo FALTA winutils, que es lo que permite escribir archivos.
    echo.
    echo Para instalarlo, corre esto en su lugar:
    echo     powershell -ExecutionPolicy Bypass -File "%BOOTSTRAP%"
    echo.
    pause
    exit /b 1
)

echo El entorno esta completo.
echo.
echo Corriendo la prueba de humo, esto toma un minuto.
echo.

if not exist "%REPO%evidencias\logs" mkdir "%REPO%evidencias\logs"
cmd /c ""%PY%" "%REPO%src\comun\smoke_test.py" > "%REPO%evidencias\logs\00_smoke_test.log" 2>&1"
set "CODIGO=%ERRORLEVEL%"

echo.
powershell -NoProfile -Command "Get-Content -LiteralPath '%REPO%evidencias\logs\00_smoke_test.log' -Tail 4" 2>nul
echo.

if not "%CODIGO%"=="0" (
    echo La prueba de humo fallo. El detalle esta en:
    echo     %REPO%evidencias\logs\00_smoke_test.log
    echo.
    pause
    exit /b 1
)

echo Todo anda bien.
echo.
echo Para correr el pipeline completo, la entrada es:
echo     .\run_all.ps1
echo.
pause
endlocal

@echo off
echo ======================================
echo Running HubSpot Deals Extraction Script
echo ======================================

REM Set Python path
set PYTHON_EXE="C:\Users\User\AppData\Local\Programs\Python\Python311\python.exe"

REM Set script path
set SCRIPT_PATH="C:\Users\User\Downloads\Z\python\CRM-ETL-Integration\crm-integration-project-layers\main.py"

%PYTHON_EXE% %SCRIPT_PATH%

echo.
echo Script finished.


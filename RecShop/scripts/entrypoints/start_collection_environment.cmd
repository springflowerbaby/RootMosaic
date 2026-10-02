@echo off
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0..\..\start_collection_environment.ps1" %*
set "launcher_exit=%errorlevel%"
if "%~1"=="" pause
exit /b %launcher_exit%

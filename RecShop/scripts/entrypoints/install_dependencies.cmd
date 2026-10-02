@echo off
setlocal
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0..\..\install_dependencies.ps1" %*
set "dependency_exit=%errorlevel%"
if "%~1"=="" pause
exit /b %dependency_exit%

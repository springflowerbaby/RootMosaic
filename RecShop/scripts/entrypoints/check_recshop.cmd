@echo off
setlocal
if defined RECSHOP_PYTHON (
  "%RECSHOP_PYTHON%" -B -X utf8 "%~dp0check_recshop.py" %*
) else (
  python -B -X utf8 "%~dp0check_recshop.py" %*
)
exit /b %errorlevel%

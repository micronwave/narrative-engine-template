@echo off
cd /d %~dp0
if not exist logs mkdir logs
python pipeline.py >> logs\pipeline_%date:~-4,4%%date:~-10,2%%date:~-7,2%.log 2>&1
exit /b %ERRORLEVEL%

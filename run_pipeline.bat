@echo off
REM Pipeline runner — configure Task Scheduler for desired frequency.
REM Recommended: every 4 hours (6x/day). Set PIPELINE_FREQUENCY_HOURS in .env.
REM Task Scheduler: Create Basic Task → Trigger: Daily, Repeat every 4 hours → Action: Start Program → this bat
cd /d %~dp0
if not exist logs mkdir logs

REM Prevent concurrent execution
if exist pipeline.lock (
    echo Pipeline already running, skipping. >> logs\pipeline_skip.log
    exit /b 0
)
echo %date% %time% > pipeline.lock

REM Fix leading space in hour (e.g. " 9" -> "09")
set "hr=%time:~0,2%"
set "hr=%hr: =0%"

python pipeline.py >> "logs\pipeline_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%hr%%time:~3,2%.log" 2>&1
set "rc=%ERRORLEVEL%"
del pipeline.lock
exit /b %rc%

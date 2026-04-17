@echo off
REM Pipeline runner — configure Task Scheduler for desired frequency.
REM Recommended: every 4 hours (6x/day). Set PIPELINE_FREQUENCY_HOURS in .env.
REM Task Scheduler: Create Basic Task → Trigger: Daily, Repeat every 4 hours → Action: Start Program → this bat
cd /d %~dp0
if not exist logs mkdir logs

REM Stale-lock threshold. Scheduler runs every 4h and a typical pipeline cycle
REM takes ~3 min (10 min worst case), so any lock older than this is orphaned
REM from a crashed run and safe to clear.
set "STALE_LOCK_HOURS=6"

REM Concurrent-run guard with self-healing stale-lock recovery
if exist pipeline.lock (
    REM Check lock age via file mtime (locale-independent, unlike %date% %time%)
    REM PowerShell exit code: 0 = stale (clear and proceed), 1 = fresh (skip)
    powershell -NoProfile -Command "$l = Get-Item 'pipeline.lock' -ErrorAction SilentlyContinue; if ($l -and $l.LastWriteTime -lt (Get-Date).AddHours(-%STALE_LOCK_HOURS%)) { exit 0 } else { exit 1 }"
    if errorlevel 1 (
        REM Fresh lock — another run is active, skip this cycle
        for %%F in (pipeline.lock) do echo %date% %time% skip [fresh lock from %%~tF] >> logs\pipeline_skip.log
        exit /b 0
    )
    REM Stale lock — previous run crashed before cleanup. Log and clear.
    for %%F in (pipeline.lock) do echo %date% %time% cleared stale lock [from %%~tF, threshold %STALE_LOCK_HOURS%h] >> logs\pipeline_skip.log
    del pipeline.lock
)
echo %date% %time% > pipeline.lock

REM Fix leading space in hour (e.g. " 9" -> "09")
set "hr=%time:~0,2%"
set "hr=%hr: =0%"

python pipeline.py >> "logs\pipeline_%date:~-4,4%%date:~-10,2%%date:~-7,2%_%hr%%time:~3,2%.log" 2>&1
set "rc=%ERRORLEVEL%"
del pipeline.lock
exit /b %rc%

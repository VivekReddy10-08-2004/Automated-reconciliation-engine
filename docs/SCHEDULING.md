# Daily Reconciliation Scheduling

Run `run_equity_reconciliation.py` at **4:30 PM America/New_York, Monday
through Friday**. The regular US equity session closes at 4:00 PM Eastern, so
this gives the providers 30 minutes to publish end-of-day data.

The schedule still runs on market holidays. It also runs after early-close
sessions, because those sessions finish before 4:30 PM. Holiday awareness can
be added later with an exchange-calendar check inside the Python entry point.

Before creating either schedule, run the command manually from the project root
and confirm that `project.env` contains the required Alpaca credentials:

```text
python run_equity_reconciliation.py
```

Clean runs remain silent. Runs with breaks and failed runs use the Task 11
notification behavior. If `WEBHOOK_URL` is absent, the notification is printed
to the process output.

## Windows Task Scheduler

Windows Task Scheduler uses the computer's local timezone. Confirm that the
computer is using Eastern Time if the task must always run at 4:30 PM Eastern:

```powershell
Get-TimeZone
```

If the computer uses another timezone, convert 4:30 PM Eastern to that local
timezone and account for daylight-saving changes.

Open PowerShell as the Windows user who owns the project. Change only the first
line to the actual Windows checkout location, then run the complete block:

```powershell
$ProjectDirectory = "C:\path\to\Automated-reconciliation-engine"
$PythonExecutable = Join-Path $ProjectDirectory ".venv\Scripts\python.exe"
$Runner = Join-Path $ProjectDirectory "run_equity_reconciliation.py"

$Action = New-ScheduledTaskAction `
    -Execute $PythonExecutable `
    -Argument "`"$Runner`"" `
    -WorkingDirectory $ProjectDirectory

$Trigger = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Monday,Tuesday,Wednesday,Thursday,Friday `
    -At 4:30PM

$Settings = New-ScheduledTaskSettingsSet -StartWhenAvailable

Register-ScheduledTask `
    -TaskName "Automated Reconciliation Engine" `
    -Description "Run the daily reconciliation control after the US market close" `
    -Action $Action `
    -Trigger $Trigger `
    -Settings $Settings
```

The scheduled action executes:

```text
C:\path\to\Automated-reconciliation-engine\.venv\Scripts\python.exe C:\path\to\Automated-reconciliation-engine\run_equity_reconciliation.py
```

with this working directory:

```text
C:\path\to\Automated-reconciliation-engine
```

Verify the task and run a manual smoke test:

```powershell
Get-ScheduledTask -TaskName "Automated Reconciliation Engine"
Start-ScheduledTask -TaskName "Automated Reconciliation Engine"
Get-ScheduledTaskInfo -TaskName "Automated Reconciliation Engine"
```

`LastTaskResult` should be `0` after a successful execution. A nonzero value
means the Python process failed. Review the Task Scheduler History tab and the
notification output.

Remove the task with:

```powershell
Unregister-ScheduledTask -TaskName "Automated Reconciliation Engine" -Confirm:$false
```

## Ubuntu cron

The verified project directory in this workspace is:

```text
/home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine
```

Ubuntu cron evaluates schedules in the cron daemon's system timezone. Confirm
the host timezone before installing the entry:

```bash
timedatectl show -p Timezone --value
```

This workspace reports `America/New_York`, so `16:30` remains aligned with the
market close across daylight-saving changes. If another host reports a
different timezone, either convert 4:30 PM Eastern to that host's local time or
have an administrator configure the host timezone before installing the job.
Setting `TZ` or `CRON_TZ` inside an Ubuntu user crontab changes the command's
environment but does not change when this cron implementation triggers it.

Create a local log directory:

```bash
mkdir -p /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine/logs
```

Open the current user's crontab:

```bash
crontab -e
```

Add these lines:

```cron
30 16 * * 1-5 cd /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine && /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine/.venv/bin/python /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine/run_equity_reconciliation.py >> /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine/logs/reconciliation.log 2>&1
```

The explicit `cd` sets the working directory so `project.env` and
`reconciliation.duckdb` resolve inside the project.

Verify the installed schedule:

```bash
crontab -l
```

Smoke-test the exact command without waiting for the scheduled time:

```bash
cd /home/vivek/Documents/Vivek-projects/Automated-reconciliation-engine
./.venv/bin/python run_equity_reconciliation.py >> logs/reconciliation.log 2>&1
echo $?
tail -n 50 logs/reconciliation.log
```

An exit code of `0` means the run completed. Because the runner re-raises
unexpected exceptions, failed scheduled runs produce a nonzero exit code.

Remove the schedule with `crontab -e` and delete the reconciliation line.

## Operational Checks

After the first scheduled run, confirm all of the following:

1. A new row exists in `pipeline_runs`.
2. Source data and reconciliation results share the same `run_id`.
3. Breaks create exceptions and one notification.
4. Clean runs do not create a notification.
5. The scheduler records success or a nonzero failure result.
6. Logs and notifications do not contain credentials or webhook URLs.

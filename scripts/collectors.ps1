<#
    Start, stop and check the three collector processes. Windows / PowerShell.

    The bash twin of this file is scripts/collectors.sh; keep them in step.

    WHY THIS EXISTS. The collectors are three separate long-running processes by
    design (3.1: an entry-side stall must never stop an exit loop), which means
    three windows, three chances to forget one, and three ways to lose a week of
    collection. Outcome data is time-irreversible -- nobody stores what happened
    to the tokens launching today -- so a week not collected cannot be recovered.

    It calls .venv\Scripts\trenches.exe DIRECTLY rather than relying on an
    activated shell, because "trenches is not recognized" is what happens when
    it is not activated.

    RUN IT LIKE THIS (the ExecutionPolicy part matters -- Windows blocks
    unsigned .ps1 files by default and the error does not say that clearly):

        powershell -ExecutionPolicy Bypass -File .\scripts\collectors.ps1 start
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('start', 'stop', 'status', 'logs', 'restart')]
    [string]$Action = 'status'
)

$ErrorActionPreference = 'Stop'

$Root     = Split-Path -Parent $PSScriptRoot
$Trenches = Join-Path $Root '.venv\Scripts\trenches.exe'
$RunDir   = Join-Path $Root '.run'
$LogDir   = Join-Path $Root 'logs'

# `exits` is included even though nothing opens paper positions unless --paper
# is on: it costs nothing idle, and discovering it was off is worse than the
# handful of log lines it writes.
$Procs = @('stream', 'sample', 'exits')

function Require-Venv {
    if (-not (Test-Path $Trenches)) {
        Write-Host "error: $Trenches is missing." -ForegroundColor Red
        Write-Host ""
        Write-Host "Install it first, from inside the repository folder:"
        Write-Host ""
        Write-Host "  py -3 -m venv .venv"
        Write-Host "  .venv\Scripts\pip install -r requirements.txt"
        Write-Host "  .venv\Scripts\pip install -e . --no-deps"
        Write-Host ""
        exit 1
    }
}

function Load-Env {
    $conf = Join-Path $Root 'trenches.local.conf'
    if (Test-Path $conf) {
        $env:TRENCHES_ENV_FILE = $conf
    } elseif (-not $env:TRENCHES_ENV_FILE) {
        Write-Host "note: no trenches.local.conf found; using built-in defaults."
        Write-Host "      copy .env.example trenches.local.conf   to change anything."
    }
}

function Get-CollectorPid([string]$Name) {
    $file = Join-Path $RunDir "$Name.pid"
    if (-not (Test-Path $file)) { return $null }
    $id = (Get-Content $file -ErrorAction SilentlyContinue | Select-Object -First 1)
    if (-not $id) { return $null }
    $proc = Get-Process -Id ([int]$id) -ErrorAction SilentlyContinue
    if ($proc) { return [int]$id }
    return $null
}

function Start-One([string]$Name) {
    $existing = Get-CollectorPid $Name
    if ($existing) {
        Write-Host "  $Name already running (pid $existing)"
        return $true
    }
    # Start-Process refuses to send stdout and stderr to the SAME file, so they
    # are split. `status` reads the .err.log first because that is where a crash
    # lands.
    $out = Join-Path $LogDir "$Name.log"
    $err = Join-Path $LogDir "$Name.err.log"
    $env:PYTHONUNBUFFERED = '1'
    $proc = Start-Process -FilePath $Trenches -ArgumentList $Name `
        -WorkingDirectory $Root -WindowStyle Hidden -PassThru `
        -RedirectStandardOutput $out -RedirectStandardError $err
    Set-Content -Path (Join-Path $RunDir "$Name.pid") -Value $proc.Id
    Start-Sleep -Seconds 2
    if (Get-CollectorPid $Name) {
        Write-Host "  $Name started (pid $($proc.Id))"
        return $true
    }
    Write-Host "  $Name FAILED to start -- last lines of logs\$Name.err.log:" -ForegroundColor Red
    if (Test-Path $err) { Get-Content $err -Tail 15 }
    return $false
}

function Invoke-Start {
    Require-Venv
    Load-Env
    New-Item -ItemType Directory -Force -Path $RunDir, $LogDir | Out-Null
    Write-Host "starting collectors:"
    $ok = $true
    foreach ($name in $Procs) { if (-not (Start-One $name)) { $ok = $false } }
    Write-Host ""
    Write-Host "  logs:   powershell -ExecutionPolicy Bypass -File .\scripts\collectors.ps1 logs"
    Write-Host "  check:  powershell -ExecutionPolicy Bypass -File .\scripts\collectors.ps1 status"
    Write-Host "  data:   .venv\Scripts\trenches stats --hours 24"
    Write-Host ""
    Write-Host "  SLEEP STOPS COLLECTION, and it is the failure that silently costs"
    Write-Host "  the whole week -- you come back to six hours of data. Before you"
    Write-Host "  walk away, in Settings > System > Power & battery:"
    Write-Host "    - Screen and sleep: set 'When plugged in, put my device to sleep'"
    Write-Host "      to Never"
    Write-Host "    - On a laptop, set 'When I close the lid' to 'Do nothing'"
    Write-Host "  Leaving it plugged in is not enough on its own."
    Write-Host ""
    if (-not $ok) { exit 1 }
}

function Invoke-Stop {
    New-Item -ItemType Directory -Force -Path $RunDir | Out-Null
    Write-Host "stopping collectors:"
    foreach ($name in $Procs) {
        $id = Get-CollectorPid $name
        if ($id) {
            Stop-Process -Id $id -ErrorAction SilentlyContinue
            for ($i = 0; $i -lt 20; $i++) {
                if (-not (Get-CollectorPid $name)) { break }
                Start-Sleep -Milliseconds 500
            }
            if (Get-CollectorPid $name) {
                Write-Host "  $name did not stop cleanly, forcing" -ForegroundColor Yellow
                Stop-Process -Id $id -Force -ErrorAction SilentlyContinue
            }
            Write-Host "  $name stopped"
        } else {
            Write-Host "  $name not running"
        }
        Remove-Item (Join-Path $RunDir "$name.pid") -ErrorAction SilentlyContinue
    }
}

function Invoke-Status {
    $allUp = $true
    "{0,-10} {1,-8} {2,-8} {3}" -f 'NAME', 'STATE', 'PID', 'LAST LINE' | Write-Host
    foreach ($name in $Procs) {
        $last = ''
        foreach ($file in @("$name.err.log", "$name.log")) {
            $path = Join-Path $LogDir $file
            if (Test-Path $path) {
                $line = Get-Content $path -Tail 1 -ErrorAction SilentlyContinue
                if ($line) {
                    $last = $line.Substring(0, [Math]::Min(70, $line.Length))
                    break
                }
            }
        }
        $id = Get-CollectorPid $name
        if ($id) {
            "{0,-10} {1,-8} {2,-8} {3}" -f $name, 'UP', $id, $last | Write-Host
        } else {
            "{0,-10} {1,-8} {2,-8} {3}" -f $name, 'DOWN', '-', $last | Write-Host
            $allUp = $false
        }
    }
    Write-Host ""
    if ($allUp) {
        Write-Host "  All three up. Check what they have collected with:"
        Write-Host "      .venv\Scripts\trenches stats --hours 24"
    } else {
        Write-Host "  Something is DOWN. Its last log line above is usually the reason;"
        Write-Host "  the full logs are in logs\. Restart with:"
        Write-Host "      powershell -ExecutionPolicy Bypass -File .\scripts\collectors.ps1 start"
        exit 1
    }
}

function Invoke-Logs {
    New-Item -ItemType Directory -Force -Path $LogDir | Out-Null
    Write-Host "following logs\. Ctrl-C stops watching, NOT the collectors."
    Write-Host ""
    # Get-Content -Wait follows one file. Watching stream is the useful default;
    # the others are in logs\ and `status` surfaces their last line.
    $path = Join-Path $LogDir 'stream.log'
    if (-not (Test-Path $path)) { New-Item -ItemType File -Path $path | Out-Null }
    Get-Content $path -Tail 20 -Wait
}

switch ($Action) {
    'start'   { Invoke-Start }
    'stop'    { Invoke-Stop }
    'status'  { Invoke-Status }
    'logs'    { Invoke-Logs }
    'restart' { Invoke-Stop; Write-Host ''; Invoke-Start }
}

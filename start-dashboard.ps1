$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigPath = [IO.Path]::GetFullPath((Join-Path $Root "default.cfg"))
$AppUrl = "http://127.0.0.1:8000/lendingbot.html?launch=$([Guid]::NewGuid().ToString('N'))#overview"

function Resolve-Python {
    $py = Get-Command py -ErrorAction SilentlyContinue
    if ($py) { return @{ File = $py.Source; Prefix = @("-3") } }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) { return @{ File = $python.Source; Prefix = @() } }
    throw "Python was not found. Run install-dependencies.cmd first."
}

function Get-DashboardHealth {
    try { return Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/health" -TimeoutSec 2 }
    catch { return $null }
}

function Get-LegacyConfigPath {
    try { return (Invoke-RestMethod -Uri "http://127.0.0.1:8000/api/config" -TimeoutSec 2).configPath }
    catch { return $null }
}

function Get-Listener {
    return Get-NetTCPConnection -LocalPort 8000 -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1
}

function Get-DashboardLockMetadata {
    $path = Join-Path $Root ".state\lendingbot-dashboard.lock"
    if (-not (Test-Path -LiteralPath $path)) { return $null }
    $stream = $null
    $reader = $null
    try {
        # Byte 0 is the Windows single-instance lock. Reading the whole file
        # crosses that locked byte and fails even though the JSON metadata
        # beginning at byte 1 is intentionally readable by the launcher.
        $stream = [IO.FileStream]::new(
            $path,
            [IO.FileMode]::Open,
            [IO.FileAccess]::Read,
            [IO.FileShare]::ReadWrite
        )
        if ($stream.Length -le 1) { return $null }
        [void]$stream.Seek(1, [IO.SeekOrigin]::Begin)
        $reader = [IO.StreamReader]::new($stream, [Text.Encoding]::UTF8, $true, 1024, $true)
        $json = $reader.ReadToEnd().Trim([char[]]@(0, 13, 10, 32))
        if (-not $json) { return $null }
        return ($json | ConvertFrom-Json)
    } catch {
        return $null
    } finally {
        if ($reader) { $reader.Dispose() }
        if ($stream) { $stream.Dispose() }
    }
}

try {
    Set-Location -LiteralPath $Root
    $Python = Resolve-Python
    $buildArgs = @($Python.Prefix) + @("-c", "import lendingbot; print(lendingbot.dashboard_build_id())")
    $ExpectedBuild = (& $Python.File @buildArgs | Select-Object -Last 1).Trim()
    if (-not $ExpectedBuild) { throw "Unable to calculate the Dashboard build ID." }

    $listener = Get-Listener
    if ($listener) {
        $pidValue = [int]$listener.OwningProcess
        $health = Get-DashboardHealth
        $lock = Get-DashboardLockMetadata
        $newIdentity = $health -and $health.service -eq "mika-lending-dashboard-v3" -and
            [IO.Path]::GetFullPath([string]$health.configPath) -eq $ConfigPath -and
            [IO.Path]::GetFullPath([string]$health.projectRoot) -eq [IO.Path]::GetFullPath($Root) -and
            [int]$health.pid -eq $pidValue -and $lock -and [int]$lock.pid -eq $pidValue -and
            [string]$lock.buildId -eq [string]$health.buildId -and
            [IO.Path]::GetFullPath([string]$lock.configPath) -eq $ConfigPath
        if ($newIdentity -and $health.ok -eq $true -and $health.buildId -eq $ExpectedBuild) {
            if ($env:MIKA_LAUNCHER_NO_BROWSER -ne "1") { Start-Process $AppUrl }
            exit 0
        }
        if (-not $newIdentity) {
            $process = Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
            $legacyConfig = Get-LegacyConfigPath
            $legacyIdentity = $process -and $process.Name -match '^python(?:w)?\.exe$' -and
                $process.CommandLine -match '(?i)lendingbot\.py' -and $process.CommandLine -match '(?i)--dashboard' -and
                $legacyConfig -and [IO.Path]::GetFullPath([string]$legacyConfig) -eq $ConfigPath
            if (-not $legacyIdentity) {
                $name = if ($process) { $process.Name } else { "unknown" }
                throw "Port 8000 is owned by an unverified process (PID $pidValue, $name). The launcher will not terminate it."
            }
        }
        Write-Host "Safely replacing Dashboard PID $pidValue..."
        Stop-Process -Id $pidValue -Force
        $released = $false
        for ($attempt = 0; $attempt -lt 40; $attempt++) {
            if (-not (Get-Listener)) { $released = $true; break }
            Start-Sleep -Milliseconds 250
        }
        if (-not $released) { throw "The old Dashboard did not release port 8000 within 10 seconds." }
    } else {
        # Recover an older/headless Dashboard whose HTTP thread died while its
        # main process continued holding the single-instance lock.
        $lock = Get-DashboardLockMetadata
        if ($lock -and $lock.pid) {
            $pidValue = [int]$lock.pid
            $process = Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue" -ErrorAction SilentlyContinue
            $headlessIdentity = $process -and $process.Name -match '^python(?:w)?\.exe$' -and
                $process.CommandLine -match '(?i)lendingbot\.py' -and $process.CommandLine -match '(?i)--dashboard' -and
                [string]$lock.role -eq "dashboard" -and [string]$lock.service -eq "mika-lending-dashboard-v3" -and
                [IO.Path]::GetFullPath([string]$lock.configPath) -eq $ConfigPath -and
                [IO.Path]::GetFullPath([string]$lock.projectRoot) -eq [IO.Path]::GetFullPath($Root)
            if ($headlessIdentity) {
                Write-Host "Safely replacing headless Dashboard PID $pidValue..."
                Stop-Process -Id $pidValue -Force
                Wait-Process -Id $pidValue -Timeout 10 -ErrorAction SilentlyContinue
            }
        }
    }

    $startArgs = @($Python.Prefix) + @("lendingbot.py", "--dashboard")
    Start-Process -FilePath $Python.File -ArgumentList $startArgs -WorkingDirectory $Root -WindowStyle Hidden

    $ready = $false
    for ($attempt = 0; $attempt -lt 80; $attempt++) {
        $health = Get-DashboardHealth
        if ($health -and $health.service -eq "mika-lending-dashboard-v3" -and $health.buildId -eq $ExpectedBuild -and
            [IO.Path]::GetFullPath([string]$health.configPath) -eq $ConfigPath) {
            $ready = $true
            break
        }
        Start-Sleep -Milliseconds 250
    }
    if (-not $ready) { throw "The new Dashboard did not complete its build handshake within 20 seconds." }
    if ($env:MIKA_LAUNCHER_NO_BROWSER -ne "1") { Start-Process $AppUrl }
    exit 0
} catch {
    Write-Host ""
    Write-Host "Dashboard startup failed: $($_.Exception.Message)" -ForegroundColor Red
    exit 1
}

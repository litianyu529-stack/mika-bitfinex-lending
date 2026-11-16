import json
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("build,expected_calls", [
    ("current", []),
    ("old", ["stop:4242", "start"]),
])
def test_launcher_reuses_only_a_healthy_current_dashboard(tmp_path, monkeypatch, build, expected_calls):
    powershell = shutil.which("powershell")
    if not powershell:
        pytest.skip("Windows PowerShell is required to exercise the launcher")
    source = Path(__file__).resolve().parents[1] / "start-dashboard.ps1"
    shutil.copyfile(source, tmp_path / source.name)
    state = tmp_path / ".state"
    state.mkdir()
    config = str(tmp_path / "default.cfg")
    (state / "lendingbot-dashboard.lock").write_bytes(b"\0" + json.dumps({
        "pid": 4242, "buildId": build, "configPath": config,
    }).encode())
    monkeypatch.setenv("MIKA_TEST_ROOT", str(tmp_path))
    monkeypatch.setenv("MIKA_TEST_BUILD", build)
    monkeypatch.setenv("MIKA_LAUNCHER_NO_BROWSER", "1")
    harness = r"""
    $global:fixtureListenerRunning = $true
    $global:fixtureBuild = $env:MIKA_TEST_BUILD
    function Get-Command { @{ Source = "Get-FixtureBuild" } }
    function Get-FixtureBuild { "current" }
    function Get-NetTCPConnection {
        [CmdletBinding()]
        param($LocalPort, $State)
        if ($global:fixtureListenerRunning) { @{ OwningProcess = 4242 } }
    }
    function Invoke-RestMethod {
        param($Uri, $TimeoutSec)
        if ($Uri -notlike "*/api/health") { throw "Unexpected API call: $Uri" }
        @{
            ok = $true
            pid = 4242
            buildId = $global:fixtureBuild
            service = "mika-lending-dashboard-v3"
            projectRoot = $env:MIKA_TEST_ROOT
            configPath = Join-Path $env:MIKA_TEST_ROOT "default.cfg"
        }
    }
    function Stop-Process {
        param($Id, [switch]$Force)
        Write-Host "stop:${Id}"
        $global:fixtureListenerRunning = $false
    }
    function Start-Process {
        param($FilePath, $ArgumentList, $WorkingDirectory, $WindowStyle)
        Write-Host "start"
        $global:fixtureBuild = "current"
        $global:fixtureListenerRunning = $true
    }
    & (Join-Path $env:MIKA_TEST_ROOT "start-dashboard.ps1")
    exit $LASTEXITCODE
    """
    result = subprocess.run([powershell, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass",
                             "-Command", harness], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr
    calls = [line for line in result.stdout.splitlines() if line == "start" or line.startswith("stop:")]
    assert calls == expected_calls

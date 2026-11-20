$ErrorActionPreference = "Stop"
$V4Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepositoryRoot = Split-Path -Parent $V4Root
function Assert-NativeSuccess([string]$Step) {
    if ($LASTEXITCODE -ne 0) {
        throw "$Step failed with exit code $LASTEXITCODE"
    }
}
Push-Location -LiteralPath $V4Root
try {
    python -m ruff check .
    Assert-NativeSuccess "Ruff"
    python -m compileall -q .
    Assert-NativeSuccess "Python compilation"
    node --check www/app.js
    Assert-NativeSuccess "JavaScript syntax"
    python -m pytest -q --cov=mika_v4 --cov-report=term --cov-fail-under=85
    Assert-NativeSuccess "V4 overall coverage"
    python -m pytest -q `
        --cov=mika_v4.market `
        --cov=mika_v4.strategy `
        --cov=mika_v4.execution `
        --cov=mika_v4.store `
        --cov-report=term `
        --cov-fail-under=90
    Assert-NativeSuccess "V4 core coverage"
} finally {
    Pop-Location
}
Push-Location -LiteralPath $RepositoryRoot
try {
    git diff --check
    Assert-NativeSuccess "Git whitespace check"
} finally {
    Pop-Location
}

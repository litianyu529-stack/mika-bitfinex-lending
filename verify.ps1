$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location -LiteralPath $Root

python -m ruff check .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m compileall -q -f .
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m pytest -q `
    --cov=RuntimeV3 `
    --cov=StateStore `
    --cov=StrategyV3 `
    --cov=StrategyState `
    --cov=MarketHistory `
    --cov=ManualProtection `
    --cov=EventReplay `
    --cov=StrategyResearch `
    --cov=V36Validation `
    --cov=WriteRecovery `
    --cov=Recovery `
    --cov-branch `
    --cov-report=term-missing `
    --cov-fail-under=75
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

# V3.6 pure strategy core includes branch data from the run above and must stay
# above the release threshold independently of the integration-heavy runtime.
python -m coverage report --include=StrategyV3.py --fail-under=90
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

python -m coverage report --include=Recovery.py --fail-under=90
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

node --check www/lendingbot.js
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
node --check www/v3-dashboard.js
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

git diff --check
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "Bitfinex-lendingbot verification passed."

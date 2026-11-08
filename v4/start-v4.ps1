$ErrorActionPreference = "Stop"
$V4Root = Split-Path -Parent $MyInvocation.MyCommand.Path
$ConfigPath = Join-Path $V4Root "default.cfg"
if (-not (Test-Path -LiteralPath $ConfigPath)) {
    Copy-Item -LiteralPath (Join-Path $V4Root "default.cfg.example") -Destination $ConfigPath
    Write-Host "已生成 v4/default.cfg，请通过环境变量配置 API 凭据。"
}
Set-Location -LiteralPath $V4Root
python main.py --config $ConfigPath serve

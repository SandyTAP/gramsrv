#!/usr/bin/env pwsh
# Capture a gramsrv backup bundle with sensible defaults for a monolith
# deployment. Every unrecognised argument is forwarded to gramsrv-backup.
#
#   ./scripts/backup.ps1 -Out D:\gramsrv-backup\20261004 -Component all
#
# The tool reads .env for database credentials, so nothing secret is passed on
# the command line.
[CmdletBinding()]
param(
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$ToolArgs
)

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent $PSScriptRoot
$Binary = Join-Path $RepoRoot 'bin/gramsrv-backup.exe'

function Test-HasFlag([string]$Name, [string[]]$Items) {
    foreach ($item in $Items) {
        if ($item -eq $Name -or $item.StartsWith("$Name=")) { return $true }
    }
    return $false
}

function Build-Binary {
    if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
        throw "go is required to build $Binary"
    }
    Write-Host "building $Binary"
    # Serial compilation keeps peak memory low enough for small hosts.
    Push-Location $RepoRoot
    try {
        $env:GOFLAGS = '-p=1'
        go build -o $Binary ./cmd/gramsrv-backup
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    } finally {
        Pop-Location
    }
}

if ($ToolArgs -contains '--help') {
    Get-Content -Raw (Join-Path $PSScriptRoot 'backup.sh') | Select-String -Pattern '(?s)usage\(\).*?^}' | ForEach-Object { $_.Matches.Value }
    exit 0
}

if (-not (Test-Path $Binary)) { Build-Binary }

$forwarded = @()
if (-not (Test-HasFlag '--out' $ToolArgs)) {
    $stamp = (Get-Date).ToUniversalTime().ToString('yyyyMMdd')
    $forwarded += @('--out', (Join-Path (Split-Path -Parent $RepoRoot) "gramsrv-backup-$stamp"))
}
if (-not (Test-HasFlag '--repo-dir' $ToolArgs)) {
    $forwarded += @('--repo-dir', $RepoRoot)
}
if (-not (Test-HasFlag '--pg-tools' $ToolArgs) -and (Get-Command docker -ErrorAction SilentlyContinue)) {
    $forwarded += @('--pg-tools', 'docker')
}
if (-not (Test-HasFlag '--redis-tools' $ToolArgs) -and (Get-Command docker -ErrorAction SilentlyContinue)) {
    $forwarded += @('--redis-tools', 'docker')
}

Push-Location $RepoRoot
try {
    & $Binary backup @forwarded @ToolArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

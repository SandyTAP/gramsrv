#!/usr/bin/env pwsh
# Restore a gramsrv backup bundle produced by ./scripts/backup.ps1 or
# ./scripts/backup.sh.
#
# The bundle carries the database, the media and key tree, and the configuration,
# so a restore is: put the repository in place, replay the bundle, then start the
# services. Read this script before running it against a live instance; it will
# refuse to overwrite files unless -Force is given.
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)]
    [string]$From,

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
    Push-Location $RepoRoot
    try {
        $env:GOFLAGS = '-p=1'
        go build -o $Binary ./cmd/gramsrv-backup
        if ($LASTEXITCODE -ne 0) { throw "go build failed" }
    } finally {
        Pop-Location
    }
}

if (-not (Test-Path $From)) {
    throw "bundle directory not found: $From"
}
if (-not (Test-Path $Binary)) { Build-Binary }

$forwarded = @()
if (-not (Test-HasFlag '--repo-dir' $ToolArgs)) {
    $forwarded += @('--repo-dir', $RepoRoot)
}
if (-not (Test-HasFlag '--pg-tools' $ToolArgs) -and (Get-Command docker -ErrorAction SilentlyContinue)) {
    $forwarded += @('--pg-tools', 'docker')
}

Push-Location $RepoRoot
try {
    & $Binary restore @forwarded '--from' $From @ToolArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

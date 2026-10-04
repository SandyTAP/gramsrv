#!/usr/bin/env pwsh
# Cross-check a gramsrv deployment against a live database: every object the
# database references must exist on disk with the right content digest.
#
# This is the check that catches a half-copied media tree, which is the failure
# mode a plain copy of data/ produces when a file is truncated.
[CmdletBinding()]
param(
    [switch]$Fast,

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

if (-not (Test-Path $Binary)) { Build-Binary }

$forwarded = @()
if (-not (Test-HasFlag '--repo-dir' $ToolArgs)) {
    $forwarded += @('--repo-dir', $RepoRoot)
}
if (-not (Test-HasFlag '--pg-tools' $ToolArgs) -and (Get-Command docker -ErrorAction SilentlyContinue)) {
    $forwarded += @('--pg-tools', 'docker')
}
if (-not $Fast) {
    $forwarded += '--check-digests'
}

Push-Location $RepoRoot
try {
    & $Binary verify @forwarded @ToolArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}

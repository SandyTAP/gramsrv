[CmdletBinding()]
param(
    [Parameter()]
    [string]$AdvertiseIP = "",

    [Parameter()]
    [string]$PublicBaseURL = "",

    [Parameter()]
    [string]$PublicWebBaseURL = "",

    [Parameter()]
    [switch]$AllowInsecureDevelopmentAuth,

    [Parameter()]
    [switch]$Build
)

$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$dockerDir = Join-Path $repoRoot "deploy\docker"
$composePath = Join-Path $dockerDir "compose.yaml"
$envPath = Join-Path $dockerDir ".env"
$generatorPath = Join-Path $PSScriptRoot "new-docker-env.ps1"

if (-not (Test-Path -LiteralPath $envPath -PathType Leaf)) {
    if ([string]::IsNullOrWhiteSpace($AdvertiseIP)) {
        $AdvertiseIP = "127.0.0.1"
    }
    $generatorArguments = @{
        AdvertiseIP = $AdvertiseIP
    }
    if (-not [string]::IsNullOrWhiteSpace($PublicBaseURL)) {
        $generatorArguments.PublicBaseURL = $PublicBaseURL
    }
    if (-not [string]::IsNullOrWhiteSpace($PublicWebBaseURL)) {
        $generatorArguments.PublicWebBaseURL = $PublicWebBaseURL
    }
    if ($AllowInsecureDevelopmentAuth) {
        $generatorArguments.AllowInsecureDevelopmentAuth = $true
    }
    & $generatorPath @generatorArguments
}
elseif ($PSBoundParameters.ContainsKey("AdvertiseIP") -or
        $PSBoundParameters.ContainsKey("PublicBaseURL") -or
        $PSBoundParameters.ContainsKey("PublicWebBaseURL") -or
        $PSBoundParameters.ContainsKey("AllowInsecureDevelopmentAuth")) {
    Write-Warning "deploy/docker/.env already exists; address parameters are ignored so existing credentials and deployment identity stay unchanged."
}

$composeBase = @(
    "compose",
    "--project-directory", $dockerDir,
    "--env-file", $envPath,
    "--file", $composePath
)

function Invoke-Compose {
    param([string[]]$Arguments)

    & docker @composeBase @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Invoke-Compose -Arguments @("config", "--quiet")

if ($Build) {
    Invoke-Compose -Arguments @("build", "--pull")
}
else {
    Invoke-Compose -Arguments @("pull")
}

try {
    Invoke-Compose -Arguments @("up", "--detach", "--no-build", "--wait", "--wait-timeout", "300")
}
catch {
    & docker @composeBase logs --no-color --tail 120
    throw
}

Invoke-Compose -Arguments @("ps", "--all")
Write-Host "telesrv Docker stack is ready. Configuration: $envPath"

$phoneDelivery = Get-Content -LiteralPath $envPath | Where-Object { $_ -match '^TELESRV_PHONE_CODE_DELIVERY_PROVIDER=' } | Select-Object -First 1
if ($phoneDelivery -eq 'TELESRV_PHONE_CODE_DELIVERY_PROVIDER=development') {
    $developmentCode = Get-Content -LiteralPath $envPath | Where-Object { $_ -match '^TELESRV_DEV_AUTH_CODE=[0-9]{5,6}$' } | Select-Object -First 1
    if ($null -ne $developmentCode) {
        Write-Host "Development login code: $($developmentCode.Substring($developmentCode.IndexOf('=') + 1))"
    }
}

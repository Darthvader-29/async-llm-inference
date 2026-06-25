#Requires -Version 5.1
<#
.SYNOPSIS
    Manual end-to-end smoke test for the Asynchronous AI Serving Engine.

.DESCRIPTION
    POSTs a rag_query job to the running API (X-API-Key auth), captures the
    job id, then polls GET /v1/jobs/{id} until SUCCESS/FAILED or a timeout.

    MANUAL DEMO ONLY. This script is intentionally excluded from the pytest
    suite so the automated tests remain deterministic and clock-free. Run it
    by hand after `docker compose up --build -d`.

    Compatible with Windows PowerShell 5.1 AND PowerShell 7+ (pwsh).

.PARAMETER BaseUrl
    API base URL. Default http://localhost:8000 (the compose-published port).

.PARAMETER ApiKey
    X-API-Key value. Defaults to AIE_API_KEYS (first key) from the env var or
    .env, else "local-dev-key".

.PARAMETER TimeoutSeconds
    Max seconds to poll before declaring failure. Default 60.

.PARAMETER PollSeconds
    Seconds between polls. Default 2.

.EXAMPLE
    pwsh ./scripts/smoke.ps1
.EXAMPLE
    powershell -ExecutionPolicy Bypass -File .\scripts\smoke.ps1 -TimeoutSeconds 90
#>
[CmdletBinding()]
param(
    [string] $BaseUrl        = $(if ($env:SMOKE_BASE_URL) { $env:SMOKE_BASE_URL } else { "http://localhost:8000" }),
    [string] $ApiKey         = "",
    [int]    $TimeoutSeconds = 60,
    [int]    $PollSeconds    = 2
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"   # fail fast on any unhandled error

# --- ASCII status helpers (no Unicode/emoji per PowerShell-Windows rules) ----
function Write-Info { param([string] $Message) Write-Host "[i] $Message" }
function Write-Ok   { param([string] $Message) Write-Host "[OK] $Message" -ForegroundColor Green }
function Write-Err  { param([string] $Message) Write-Host "[X] $Message" -ForegroundColor Red }

# --- Resolve the script directory (so relative .env lookup is robust) --------
$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectDir = Split-Path -Parent $ScriptDir   # scripts/ -> repo root

# --- Resolve the API key: param > env var > .env (first AIE_API_KEYS) > default
function Resolve-ApiKey {
    param([string] $Provided, [string] $ProjectRoot)

    if ($Provided) { return $Provided }
    if ($env:AIE_API_KEYS) { return ($env:AIE_API_KEYS -split ",")[0].Trim() }

    $envFile = Join-Path $ProjectRoot ".env"
    if (Test-Path $envFile) {
        # Find the AIE_API_KEYS line; take the first comma-separated key.
        $line = Get-Content $envFile |
                Where-Object { $_ -match '^\s*AIE_API_KEYS\s*=' } |
                Select-Object -First 1
        if ($line) {
            $value = ($line -split "=", 2)[1].Trim()
            if ($value) { return ($value -split ",")[0].Trim() }
        }
    }
    return "local-dev-key"   # matches the .env.example default
}

$resolvedKey = Resolve-ApiKey -Provided $ApiKey -ProjectRoot $ProjectDir

# Precompute the masked key in a variable (avoid method calls inside an
# interpolated string - a PowerShell-Windows pitfall).
$maskLen   = [Math]::Min(4, $resolvedKey.Length)
$maskedKey = $resolvedKey.Substring(0, $maskLen) + "*** (masked)"

Write-Info "Base URL : $BaseUrl"
Write-Info "API key  : $maskedKey"
Write-Info "Timeout  : ${TimeoutSeconds}s (poll every ${PollSeconds}s)"

# --- 1) Submit a rag_query job ----------------------------------------------
# Phase 6 JobSubmission wraps the discriminated union in a `payload` field;
# `job_type` is the discriminator INSIDE that payload, not a top-level field.
$body = @{
    payload = @{
        job_type = "rag_query"
        query    = "What is event-loop starvation and how does this engine avoid it?"
        top_k    = 3
    }
} | ConvertTo-Json -Depth 10   # -Depth required for nested objects (PS rule)

$headers = @{
    "X-API-Key"    = $resolvedKey
    "Content-Type" = "application/json"
}

Write-Info "POST $BaseUrl/v1/jobs (job_type=rag_query) ..."
try {
    $submit = Invoke-RestMethod -Method Post -Uri "$BaseUrl/v1/jobs" `
                                -Headers $headers -Body $body
}
catch {
    $exMessage = $_.Exception.Message
    Write-Err "Submission failed: $exMessage"
    # Surface the HTTP status if present (401 = bad/missing key, 422 = bad payload).
    if ($_.Exception.Response) {
        $status = [int] $_.Exception.Response.StatusCode
        Write-Err "HTTP status: $status"
        if ($status -eq 401) { Write-Err "Check AIE_API_KEYS / -ApiKey." }
        if ($status -eq 422) { Write-Err "Check the request payload schema." }
    }
    exit 1
}

# The 202 body is JobAccepted { job_id, status, status_url } (Phase 6). Use
# defensive PSObject property checks (safe under Set-StrictMode -Version Latest).
$jobId = $null
if ($submit -and ($submit.PSObject.Properties.Name -contains "job_id")) {
    $jobId = $submit.job_id
}
if (-not $jobId) {
    $raw = $submit | ConvertTo-Json -Depth 10
    Write-Err "Response did not contain a job_id. Raw: $raw"
    exit 1
}
$initialStatus = if ($submit.PSObject.Properties.Name -contains "status") { $submit.status } else { "?" }
Write-Ok "Accepted. job_id = $jobId (initial status: $initialStatus)"

# --- 2) Poll until terminal or timeout ---------------------------------------
$deadline   = (Get-Date).AddSeconds($TimeoutSeconds)
$terminal   = @("SUCCESS", "FAILED")   # -contains is case-insensitive (matches lowercase enum values)
$finalState = $null
$lastStatus = ""
$job        = $null

while ((Get-Date) -lt $deadline) {
    try {
        $job = Invoke-RestMethod -Method Get -Uri "$BaseUrl/v1/jobs/$jobId" `
                                 -Headers @{ "X-API-Key" = $resolvedKey }
    }
    catch {
        # Transient blip (e.g., app still warming) - report and keep polling.
        $pollErr = $_.Exception.Message
        Write-Info "Poll error (continuing): $pollErr"
        Start-Sleep -Seconds $PollSeconds
        continue
    }

    $status = if ($job.PSObject.Properties.Name -contains "status") { $job.status } else { "" }
    if ($status -ne $lastStatus) {
        Write-Info "status -> $status"
        $lastStatus = $status
    }

    if ($terminal -contains $status) {
        $finalState = $status
        break
    }

    Start-Sleep -Seconds $PollSeconds   # human-facing poll interval (NOT a test clock)
}

# --- 3) Report ---------------------------------------------------------------
if ($finalState -eq "SUCCESS") {
    Write-Ok "Job $jobId reached SUCCESS."
    if ($job -and ($job.PSObject.Properties.Name -contains "result_ref") -and $job.result_ref) {
        $resultRef = $job.result_ref
        Write-Info "Artifact: $resultRef  (browse MinIO console at http://localhost:9001)"
    }
    exit 0
}
elseif ($finalState -eq "FAILED") {
    Write-Err "Job $jobId reached FAILED."
    if ($job -and ($job.PSObject.Properties.Name -contains "error") -and $job.error) {
        $jobError = $job.error
        Write-Err "error: $jobError"
    }
    exit 1
}
else {
    Write-Err "Timed out after ${TimeoutSeconds}s; last status: '$lastStatus'."
    Write-Err "Check: docker compose ps ; docker compose logs worker"
    exit 1
}

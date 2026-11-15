#!/usr/bin/env pwsh
# Smoke test: API health + pipeline overview (Compose or port-forward).
$ErrorActionPreference = "Stop"
$base = $env:DAP_API_BASE
if (-not $base) { $base = "http://localhost:8080" }

Write-Host "GET $base/health"
$h = curl.exe -s -f "$base/health"
Write-Host $h
if ($h -notmatch "ok") { throw "health failed" }

Write-Host "GET $base/v1/pipeline/overview"
$o = curl.exe -s -f "$base/v1/pipeline/overview"
Write-Host $o

Write-Host "GET $base/metrics (best-effort)"
curl.exe -s "$base/metrics" | Select-Object -First 5

Write-Host "SMOKE_OK"

#!/usr/bin/env pwsh
$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

# Prefer kind on PATH; fall back to common WinGet install path on Windows.
$kind = Get-Command kind -ErrorAction SilentlyContinue
if ($kind) {
  $KIND = $kind.Source
} else {
  $wingetKind = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Kubernetes.kind_*\kind.exe" -ErrorAction SilentlyContinue | Select-Object -First 1
  if (-not $wingetKind) { throw "kind not found. Install: winget install Kubernetes.kind" }
  $KIND = $wingetKind.FullName
}

Write-Host "Building images..."
docker build -t dap/api:local --target api .
docker build -t dap/iot-edge:local --target iot-edge .
docker build -t dap/ingest:local --target ingest .
docker build -t dap/etl:local --target etl .
docker build -t dap/embed:local --target embed .
docker build -t dap/index:local --target index .
docker build -t dap/console:local ./console

$cluster = "dap"
$existing = & $KIND get clusters 2>$null
if ($existing -notcontains $cluster) {
  & $KIND create cluster --name $cluster
}

Write-Host "Loading images into kind..."
& $KIND load docker-image dap/api:local --name $cluster
& $KIND load docker-image dap/iot-edge:local --name $cluster
& $KIND load docker-image dap/ingest:local --name $cluster
& $KIND load docker-image dap/etl:local --name $cluster
& $KIND load docker-image dap/embed:local --name $cluster
& $KIND load docker-image dap/index:local --name $cluster
& $KIND load docker-image dap/console:local --name $cluster

Write-Host "Applying manifests (requires KEDA already installed)..."
kubectl apply -f deploy/k8s/base/namespace-config.yaml
kubectl apply -f deploy/k8s/base/secrets.example.yaml
kubectl apply -f deploy/k8s/apps/deployments.yaml
kubectl apply -f deploy/k8s/apps/console.yaml
kubectl apply -f deploy/k8s/keda/scaledobjects.yaml
# Optional: kubectl apply -f deploy/k8s/apps/ingress.yaml  (needs ingress-nginx)

Write-Host @"
Done.
  Port-forward API:     kubectl -n dap port-forward svc/dap-api 8080:8080
  Port-forward console: kubectl -n dap port-forward svc/dap-console 5174:80
  Watch scale:          kubectl get pods -n dap -w
  Kustomize dev:        kubectl apply -k deploy/k8s/overlays/dev
  Docs:                 docs/how-to-run-k8s.md
"@

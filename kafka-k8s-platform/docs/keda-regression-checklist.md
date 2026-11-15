# KEDA demo regression checklist (do not skip after enhancements)

1. Compose infra up; Compose app workers **stopped**.
2. Kind + KEDA + `.\scripts\kind-deploy.ps1`.
3. Port-forward API; activate several vehicles.
4. `kubectl get hpa -n dap` — lag metric moves above threshold.
5. `kubectl get deploy -n dap` — ingest/etl replicas rise from 0.
6. Pause edge (`kubectl scale deploy/dap-iot-edge --replicas=0`); after cooldown replicas fall.
7. Restore edge; existing Activate → stream → search still works.
8. `/health` and `/v1/pipeline/overview` still succeed.

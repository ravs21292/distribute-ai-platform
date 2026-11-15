import { useEffect, useMemo, useState, type FormEvent } from "react";
import { useAppDispatch, useAppSelector } from "../app/hooks";
import {
  activateVehicle,
  clearMessage,
  createFleet,
  deactivateVehicle,
  provisionVehicle,
  refreshAll,
  setSelectedFleet,
} from "../features/fleet/fleetSlice";
import {
  validateFleetForm,
  validateVehicleForm,
  type FieldErrors,
} from "../features/fleet/validation";
import { pushToast } from "../features/ui/toastSlice";

export function OnboardingPanel() {
  const dispatch = useAppDispatch();
  const { overview, fleets, selectedFleetId, vehicles, telemetry, lastPlugin } = useAppSelector(
    (s) => s.fleet,
  );

  const [fleetId, setFleetId] = useState("");
  const [fleetName, setFleetName] = useState("");
  const [vehicleId, setVehicleId] = useState("");
  const [vin, setVin] = useState("");
  const [model, setModel] = useState("");
  const [fleetErrors, setFleetErrors] = useState<FieldErrors>({});
  const [vehicleErrors, setVehicleErrors] = useState<FieldErrors>({});

  const existingFleetIds = useMemo(() => fleets.map((f) => f.id), [fleets]);
  const existingVehicleIds = useMemo(() => vehicles.map((v) => v.id), [vehicles]);

  useEffect(() => {
    dispatch(refreshAll(selectedFleetId));
    const id = window.setInterval(() => {
      dispatch(refreshAll(selectedFleetId));
    }, 4000);
    return () => window.clearInterval(id);
  }, [dispatch, selectedFleetId]);

  const onCreateFleet = async (e: FormEvent) => {
    e.preventDefault();
    dispatch(clearMessage());

    const result = validateFleetForm({
      id: fleetId,
      name: fleetName,
      existingIds: existingFleetIds,
    });
    setFleetErrors(result.errors);
    if (!result.ok) {
      dispatch(
        pushToast({
          kind: "error",
          title: "Validation failed",
          message: Object.values(result.errors)[0] || "Invalid fleet details.",
        }),
      );
      return;
    }

    if (result.duplicate) {
      setFleetErrors({ id: "Fleet ID already exists." });
      dispatch(setSelectedFleet(fleetId.trim()));
      dispatch(
        pushToast({
          kind: "warning",
          title: "Fleet already exists",
          message: `${fleetId.trim()} is already registered.`,
        }),
      );
      dispatch(refreshAll(fleetId.trim()));
      return;
    }

    await dispatch(createFleet({ id: fleetId.trim(), name: fleetName.trim() }));
    setFleetId("");
    setFleetName("");
    setFleetErrors({});
    dispatch(refreshAll(fleetId.trim()));
  };

  const onProvision = async (e: FormEvent) => {
    e.preventDefault();
    dispatch(clearMessage());

    const result = validateVehicleForm({
      vehicleId,
      vin,
      model,
      fleetId: selectedFleetId,
      existingVehicleIds,
    });
    setVehicleErrors(result.errors);
    if (!result.ok) {
      dispatch(
        pushToast({
          kind: "error",
          title: "Validation failed",
          message: Object.values(result.errors)[0] || "Invalid vehicle details.",
        }),
      );
      return;
    }

    if (result.duplicate) {
      setVehicleErrors({ vehicleId: "Vehicle ID already exists." });
      dispatch(
        pushToast({
          kind: "warning",
          title: "Vehicle already exists",
          message: `${vehicleId.trim()} is already provisioned.`,
        }),
      );
      return;
    }

    const action = await dispatch(
      provisionVehicle({
        fleetId: selectedFleetId,
        vehicle_id: vehicleId.trim(),
        vin: vin.trim() || undefined,
        model: model.trim(),
      }),
    );
    if (provisionVehicle.fulfilled.match(action)) {
      setVehicleId("");
      setVin("");
      setModel("");
      setVehicleErrors({});
    }
    dispatch(refreshAll(selectedFleetId));
  };

  return (
    <div className="layout">
      <header className="hero">
        <p className="eyebrow">EV Fleet Telematics</p>
        <h1>Fleet console</h1>
      </header>

      {overview && (
        <section className="stats">
          <div>
            <strong>{overview.streaming_vehicles}</strong>
            <span>Streaming</span>
          </div>
          <div>
            <strong>{overview.vehicles}</strong>
            <span>Vehicles</span>
          </div>
          <div>
            <strong>{overview.telematics_events}</strong>
            <span>Events</span>
          </div>
          <div>
            <strong>{overview.alerts}</strong>
            <span>Alerts</span>
          </div>
        </section>
      )}

      <div className="grid">
        <section className="panel">
          <h2>Fleet</h2>
          <form onSubmit={onCreateFleet} className="form" noValidate>
            <label>
              Fleet ID
              <input
                value={fleetId}
                aria-invalid={!!fleetErrors.id}
                onChange={(e) => {
                  setFleetId(e.target.value);
                  setFleetErrors((prev) => ({ ...prev, id: "" }));
                }}
              />
              {fleetErrors.id && <span className="field-error">{fleetErrors.id}</span>}
            </label>
            <label>
              Name
              <input
                value={fleetName}
                aria-invalid={!!fleetErrors.name}
                onChange={(e) => {
                  setFleetName(e.target.value);
                  setFleetErrors((prev) => ({ ...prev, name: "" }));
                }}
              />
              {fleetErrors.name && <span className="field-error">{fleetErrors.name}</span>}
            </label>
            <button type="submit">Create fleet</button>
          </form>
          <label>
            Active fleet
            <select
              value={selectedFleetId}
              onChange={(e) => dispatch(setSelectedFleet(e.target.value))}
            >
              {fleets.length === 0 && <option value="">No fleets</option>}
              {fleets.map((f) => (
                <option key={f.id} value={f.id}>
                  {f.name} ({f.id})
                </option>
              ))}
            </select>
          </label>
        </section>

        <section className="panel">
          <h2>Vehicle</h2>
          <form onSubmit={onProvision} className="form" noValidate>
            {vehicleErrors.fleet && <span className="field-error">{vehicleErrors.fleet}</span>}
            <label>
              Vehicle ID
              <input
                value={vehicleId}
                aria-invalid={!!vehicleErrors.vehicleId}
                onChange={(e) => {
                  setVehicleId(e.target.value);
                  setVehicleErrors((prev) => ({ ...prev, vehicleId: "" }));
                }}
              />
              {vehicleErrors.vehicleId && (
                <span className="field-error">{vehicleErrors.vehicleId}</span>
              )}
            </label>
            <label>
              VIN
              <input
                value={vin}
                aria-invalid={!!vehicleErrors.vin}
                onChange={(e) => {
                  setVin(e.target.value);
                  setVehicleErrors((prev) => ({ ...prev, vin: "" }));
                }}
              />
              {vehicleErrors.vin && <span className="field-error">{vehicleErrors.vin}</span>}
            </label>
            <label>
              Model
              <input
                value={model}
                aria-invalid={!!vehicleErrors.model}
                onChange={(e) => {
                  setModel(e.target.value);
                  setVehicleErrors((prev) => ({ ...prev, model: "" }));
                }}
              />
              {vehicleErrors.model && <span className="field-error">{vehicleErrors.model}</span>}
            </label>
            <button type="submit">Provision vehicle</button>
          </form>
          {lastPlugin && (
            <div className="plugin">
              <h3>Device credentials</h3>
              <p>
                <b>Protocol:</b> {lastPlugin.protocol.toUpperCase()}
              </p>
              <p>
                <b>Topic:</b> <code>{lastPlugin.mqtt.topic}</code>
              </p>
              <p>
                <b>Intake:</b> <code>{lastPlugin.platform_intake_topic}</code>
              </p>
            </div>
          )}
        </section>

        <section className="panel wide">
          <h2>Vehicles</h2>
          <table>
            <thead>
              <tr>
                <th>Vehicle</th>
                <th>Status</th>
                <th>SoC</th>
                <th>Speed</th>
                <th>Last seen</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {vehicles.map((v) => (
                <tr key={v.id}>
                  <td>
                    <div>{v.id}</div>
                    <div className="muted">
                      {v.model || "—"} · {v.vin || "—"}
                    </div>
                  </td>
                  <td>
                    <span className={`pill ${v.status}`}>{v.status}</span>
                  </td>
                  <td>{v.last_soc_percent?.toFixed?.(1) ?? "—"}%</td>
                  <td>{v.last_speed_kph?.toFixed?.(1) ?? "—"}</td>
                  <td className="muted">
                    {v.last_seen_at ? new Date(v.last_seen_at).toLocaleTimeString() : "—"}
                  </td>
                  <td className="actions">
                    <button
                      type="button"
                      disabled={v.status === "streaming"}
                      onClick={() =>
                        dispatch(
                          activateVehicle({
                            fleetId: selectedFleetId,
                            vehicleId: v.id,
                          }),
                        ).then(() => dispatch(refreshAll(selectedFleetId)))
                      }
                    >
                      Activate
                    </button>
                    <button
                      type="button"
                      className="ghost"
                      disabled={v.status !== "streaming"}
                      onClick={() =>
                        dispatch(
                          deactivateVehicle({
                            fleetId: selectedFleetId,
                            vehicleId: v.id,
                          }),
                        ).then(() => dispatch(refreshAll(selectedFleetId)))
                      }
                    >
                      Pause
                    </button>
                  </td>
                </tr>
              ))}
              {vehicles.length === 0 && (
                <tr>
                  <td colSpan={6} className="muted">
                    No vehicles
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </section>

        <section className="panel wide">
          <h2>Telemetry</h2>
          <table>
            <thead>
              <tr>
                <th>Time</th>
                <th>Vehicle</th>
                <th>SoC</th>
                <th>Speed</th>
                <th>Charge</th>
                <th>Alert</th>
                <th>Note</th>
              </tr>
            </thead>
            <tbody>
              {telemetry.map((t) => (
                <tr key={t.event_id}>
                  <td className="muted">{new Date(t.event_time).toLocaleTimeString()}</td>
                  <td>{t.vehicle_id}</td>
                  <td>{t.soc_percent}%</td>
                  <td>{t.speed_kph}</td>
                  <td>{t.charging_state}</td>
                  <td>{t.alert_code || "—"}</td>
                  <td className="note">{t.note || "—"}</td>
                </tr>
              ))}
              {telemetry.length === 0 && (
                <tr>
                  <td colSpan={7} className="muted">
                    No events
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </section>
      </div>
    </div>
  );
}

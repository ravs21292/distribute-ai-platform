export type Fleet = {
  id: string;
  name: string;
  classification: string;
  pii_policy: string;
  stats?: {
    events_ingested: number;
    events_cleaned: number;
    alerts_indexed: number;
    embeddings_ready: number;
    last_event_at: string | null;
  };
};

export type Vehicle = {
  id: string;
  fleet_id: string;
  vin: string | null;
  model: string | null;
  status: string;
  plugin_protocol?: string;
  device_token?: string | null;
  activated_at?: string | null;
  last_seen_at: string | null;
  last_soc_percent: number | null;
  last_speed_kph: number | null;
  last_location: { lat: number; lon: number } | null;
};

export type PluginBundle = {
  protocol: string;
  mqtt: {
    topic: string;
    qos: number;
    auth: string;
    note: string;
  };
  platform_intake_topic: string;
  activate_api: string;
  deactivate_api: string;
  local_simulator: string;
};

export type TelemetryEvent = {
  event_id: string;
  event_time: string;
  vehicle_id: string;
  speed_kph: number;
  soc_percent: number;
  battery_temp_c: number | null;
  charging_state: string;
  alert_code: string | null;
  alert_severity: string;
  note: string | null;
  pii_redacted: boolean;
};

export type PipelineOverview = {
  domain: string;
  intake: string;
  fleets: number;
  vehicles: number;
  streaming_vehicles: number;
  telematics_events: number;
  alerts: number;
  embeddings: number;
};

const BASE = "";

export class ApiError extends Error {
  status: number;
  detail: string;

  constructor(status: number, detail: string) {
    super(detail);
    this.name = "ApiError";
    this.status = status;
    this.detail = detail;
  }
}

function parseDetail(text: string, fallback: string): string {
  try {
    const parsed = JSON.parse(text) as { detail?: unknown };
    if (typeof parsed.detail === "string") return parsed.detail;
  } catch {
    /* plain text */
  }
  return text || fallback;
}

async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, {
    ...init,
    headers: {
      "content-type": "application/json",
      ...(init?.headers || {}),
    },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new ApiError(res.status, parseDetail(text, res.statusText));
  }
  return res.json() as Promise<T>;
}

export const fleetApi = {
  overview: () => api<PipelineOverview>("/v1/pipeline/overview"),
  listFleets: () => api<Fleet[]>("/v1/fleets"),
  createFleet: (body: { id: string; name: string; classification?: string; pii_policy?: string }) =>
    api<Fleet>("/v1/fleets", { method: "POST", body: JSON.stringify(body) }),
  listVehicles: (fleetId: string) => api<Vehicle[]>(`/v1/fleets/${fleetId}/vehicles`),
  provision: (
    fleetId: string,
    body: { vehicle_id: string; vin?: string; model: string; plugin_protocol?: string },
  ) =>
    api<{ vehicle: Vehicle; plugin: PluginBundle; next_step: string }>(
      `/v1/fleets/${fleetId}/vehicles`,
      { method: "POST", body: JSON.stringify(body) },
    ),
  activate: (fleetId: string, vehicleId: string) =>
    api<{ vehicle: Vehicle; capture: string; message: string }>(
      `/v1/fleets/${fleetId}/vehicles/${vehicleId}/activate`,
      { method: "POST" },
    ),
  deactivate: (fleetId: string, vehicleId: string) =>
    api<{ vehicle: Vehicle; capture: string }>(
      `/v1/fleets/${fleetId}/vehicles/${vehicleId}/deactivate`,
      { method: "POST" },
    ),
  telemetry: (fleetId: string, limit = 12) =>
    api<TelemetryEvent[]>(`/v1/fleets/${fleetId}/telemetry?limit=${limit}`),
};

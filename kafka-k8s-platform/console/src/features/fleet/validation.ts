export type FieldErrors = Record<string, string>;

const FLEET_ID_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{1,62}$/;
const VEHICLE_ID_RE = /^[a-zA-Z0-9][a-zA-Z0-9_-]{1,62}$/;
const VIN_RE = /^[A-HJ-NPR-Z0-9]{11,17}$/i;

export function validateFleetForm(input: {
  id: string;
  name: string;
  existingIds: string[];
}): { ok: boolean; errors: FieldErrors; duplicate: boolean } {
  const errors: FieldErrors = {};
  const id = input.id.trim();
  const name = input.name.trim();

  if (!id) errors.id = "Fleet ID is required.";
  else if (!FLEET_ID_RE.test(id)) {
    errors.id = "Use 2–63 chars: letters, numbers, _ or -.";
  }

  if (!name) errors.name = "Fleet name is required.";
  else if (name.length < 2) errors.name = "Name must be at least 2 characters.";
  else if (name.length > 256) errors.name = "Name is too long.";

  const duplicate = !!id && input.existingIds.includes(id);
  return { ok: Object.keys(errors).length === 0, errors, duplicate };
}

export function validateVehicleForm(input: {
  vehicleId: string;
  vin: string;
  model: string;
  fleetId: string;
  existingVehicleIds: string[];
}): { ok: boolean; errors: FieldErrors; duplicate: boolean } {
  const errors: FieldErrors = {};
  const vehicleId = input.vehicleId.trim();
  const vin = input.vin.trim();
  const model = input.model.trim();

  if (!input.fleetId) {
    errors.fleet = "Select or create a fleet first.";
  }

  if (!vehicleId) errors.vehicleId = "Vehicle ID is required.";
  else if (!VEHICLE_ID_RE.test(vehicleId)) {
    errors.vehicleId = "Use 2–63 chars: letters, numbers, _ or -.";
  }

  if (vin && !VIN_RE.test(vin)) {
    errors.vin = "VIN must be 11–17 characters (no I, O, or Q).";
  }

  if (!model) errors.model = "Model is required.";
  else if (model.length > 128) errors.model = "Model name is too long.";

  const duplicate = !!vehicleId && input.existingVehicleIds.includes(vehicleId);
  return { ok: Object.keys(errors).length === 0, errors, duplicate };
}

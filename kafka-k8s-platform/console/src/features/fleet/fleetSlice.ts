import { createAsyncThunk, createSlice, type PayloadAction } from "@reduxjs/toolkit";
import {
  ApiError,
  fleetApi,
  type Fleet,
  type PipelineOverview,
  type PluginBundle,
  type TelemetryEvent,
  type Vehicle,
} from "./api";
import { pushToast } from "../ui/toastSlice";

type FleetState = {
  overview: PipelineOverview | null;
  fleets: Fleet[];
  selectedFleetId: string;
  vehicles: Vehicle[];
  telemetry: TelemetryEvent[];
  lastPlugin: PluginBundle | null;
  status: "idle" | "loading" | "error";
  message: string | null;
  error: string | null;
};

const initialState: FleetState = {
  overview: null,
  fleets: [],
  selectedFleetId: "fleet-east-01",
  vehicles: [],
  telemetry: [],
  lastPlugin: null,
  status: "idle",
  message: null,
  error: null,
};

export const refreshAll = createAsyncThunk(
  "fleet/refreshAll",
  async (fleetId: string) => {
    const [overview, fleets, vehicles, telemetry] = await Promise.all([
      fleetApi.overview(),
      fleetApi.listFleets(),
      fleetApi.listVehicles(fleetId),
      fleetApi.telemetry(fleetId),
    ]);
    return { overview, fleets, vehicles, telemetry, fleetId };
  },
);

export const createFleet = createAsyncThunk(
  "fleet/createFleet",
  async (input: { id: string; name: string }, { dispatch, rejectWithValue }) => {
    try {
      const fleet = await fleetApi.createFleet(input);
      dispatch(
        pushToast({
          kind: "success",
          title: "Fleet created",
          message: `${fleet.id} created.`,
        }),
      );
      return { fleet, alreadyExisted: false };
    } catch (err) {
      // Backend returns 409 when the default demo fleet already exists — still usable.
      if (err instanceof ApiError && err.status === 409) {
        const fleets = await fleetApi.listFleets();
        const existing = fleets.find((f) => f.id === input.id);
        if (existing) {
      dispatch(
        pushToast({
          kind: "warning",
          title: "Fleet already exists",
          message: `${input.id} is already registered.`,
        }),
      );
          return { fleet: existing, alreadyExisted: true };
        }
      }
      const msg = err instanceof ApiError ? err.detail : "create fleet failed";
      dispatch(pushToast({ kind: "error", title: "Fleet create failed", message: msg }));
      return rejectWithValue(msg);
    }
  },
);

export const provisionVehicle = createAsyncThunk(
  "fleet/provision",
  async (
    input: {
      fleetId: string;
      vehicle_id: string;
      vin?: string;
      model: string;
    },
    { dispatch, rejectWithValue },
  ) => {
    try {
      const res = await fleetApi.provision(input.fleetId, {
        vehicle_id: input.vehicle_id,
        vin: input.vin,
        model: input.model,
        plugin_protocol: "mqtt",
      });
      dispatch(
        pushToast({
          kind: "success",
          title: "Vehicle provisioned",
          message: `${input.vehicle_id} registered.`,
        }),
      );
      return res;
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        const msg = `${input.vehicle_id} is already provisioned.`;
        dispatch(
          pushToast({
            kind: "warning",
            title: "Vehicle already exists",
            message: msg,
          }),
        );
        return rejectWithValue(msg);
      }
      const msg = err instanceof ApiError ? err.detail : "provision failed";
      dispatch(pushToast({ kind: "error", title: "Provision failed", message: msg }));
      return rejectWithValue(msg);
    }
  },
);

export const activateVehicle = createAsyncThunk(
  "fleet/activate",
  async (input: { fleetId: string; vehicleId: string }, { dispatch, rejectWithValue }) => {
    try {
      const res = await fleetApi.activate(input.fleetId, input.vehicleId);
      dispatch(
        pushToast({
          kind: "success",
          title: "Stream activated",
          message: `${input.vehicleId} streaming.`,
        }),
      );
      return res;
    } catch (err) {
      const msg = err instanceof ApiError ? err.detail : "activate failed";
      dispatch(pushToast({ kind: "error", title: "Activate failed", message: msg }));
      return rejectWithValue(msg);
    }
  },
);

export const deactivateVehicle = createAsyncThunk(
  "fleet/deactivate",
  async (input: { fleetId: string; vehicleId: string }, { dispatch, rejectWithValue }) => {
    try {
      const res = await fleetApi.deactivate(input.fleetId, input.vehicleId);
      dispatch(
        pushToast({
          kind: "info",
          title: "Stream paused",
          message: `Capture stopped for ${input.vehicleId}.`,
        }),
      );
      return res;
    } catch (err) {
      const msg = err instanceof ApiError ? err.detail : "deactivate failed";
      dispatch(pushToast({ kind: "error", title: "Pause failed", message: msg }));
      return rejectWithValue(msg);
    }
  },
);

const fleetSlice = createSlice({
  name: "fleet",
  initialState,
  reducers: {
    setSelectedFleet(state, action: PayloadAction<string>) {
      state.selectedFleetId = action.payload;
    },
    clearMessage(state) {
      state.message = null;
      state.error = null;
    },
  },
  extraReducers: (builder) => {
    builder
      .addCase(refreshAll.pending, (state) => {
        state.status = "loading";
      })
      .addCase(refreshAll.fulfilled, (state, action) => {
        state.status = "idle";
        state.overview = action.payload.overview;
        state.fleets = action.payload.fleets;
        state.vehicles = action.payload.vehicles;
        state.telemetry = action.payload.telemetry;
        state.selectedFleetId = action.payload.fleetId;
      })
      .addCase(refreshAll.rejected, (state, action) => {
        state.status = "error";
        state.error = action.error.message || "refresh failed";
      })
      .addCase(createFleet.fulfilled, (state, action) => {
        const { fleet, alreadyExisted } = action.payload;
        if (!state.fleets.some((f) => f.id === fleet.id)) {
          state.fleets.push(fleet);
        }
        state.selectedFleetId = fleet.id;
        state.error = null;
        state.message = alreadyExisted
          ? `Fleet ${fleet.id} already exists — selected. Continue to provision a vehicle.`
          : `Fleet ${fleet.id} created`;
      })
      .addCase(createFleet.rejected, (state, action) => {
        state.error = (action.payload as string) || action.error.message || "create fleet failed";
      })
      .addCase(provisionVehicle.fulfilled, (state, action) => {
        state.vehicles = [
          action.payload.vehicle,
          ...state.vehicles.filter((v) => v.id !== action.payload.vehicle.id),
        ];
        state.lastPlugin = action.payload.plugin;
        state.error = null;
        state.message = action.payload.next_step;
      })
      .addCase(provisionVehicle.rejected, (state, action) => {
        state.error = (action.payload as string) || action.error.message || "provision failed";
      })
      .addCase(activateVehicle.fulfilled, (state, action) => {
        const idx = state.vehicles.findIndex((v) => v.id === action.payload.vehicle.id);
        if (idx >= 0) state.vehicles[idx] = action.payload.vehicle;
        else state.vehicles.unshift(action.payload.vehicle);
        state.message = action.payload.message;
      })
      .addCase(deactivateVehicle.fulfilled, (state, action) => {
        const idx = state.vehicles.findIndex((v) => v.id === action.payload.vehicle.id);
        if (idx >= 0) state.vehicles[idx] = action.payload.vehicle;
        state.message = `Capture stopped for ${action.payload.vehicle.id}`;
      });
  },
});

export const { setSelectedFleet, clearMessage } = fleetSlice.actions;
export default fleetSlice.reducer;

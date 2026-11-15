import { configureStore } from "@reduxjs/toolkit";
import fleetReducer from "../features/fleet/fleetSlice";
import toastReducer from "../features/ui/toastSlice";

export const store = configureStore({
  reducer: {
    fleet: fleetReducer,
    toast: toastReducer,
  },
});

export type RootState = ReturnType<typeof store.getState>;
export type AppDispatch = typeof store.dispatch;

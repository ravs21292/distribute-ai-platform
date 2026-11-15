import { createSlice, type PayloadAction } from "@reduxjs/toolkit";

export type ToastKind = "success" | "warning" | "error" | "info";

export type Toast = {
  id: string;
  kind: ToastKind;
  title: string;
  message: string;
};

type ToastState = {
  items: Toast[];
};

const initialState: ToastState = { items: [] };

let toastSeq = 0;

export function makeToastId(): string {
  toastSeq += 1;
  return `toast-${Date.now()}-${toastSeq}`;
}

const toastSlice = createSlice({
  name: "toast",
  initialState,
  reducers: {
    pushToast(
      state,
      action: PayloadAction<{ kind: ToastKind; title: string; message: string; id?: string }>,
    ) {
      state.items.push({
        id: action.payload.id ?? makeToastId(),
        kind: action.payload.kind,
        title: action.payload.title,
        message: action.payload.message,
      });
      if (state.items.length > 5) state.items.shift();
    },
    dismissToast(state, action: PayloadAction<string>) {
      state.items = state.items.filter((t) => t.id !== action.payload);
    },
    clearToasts(state) {
      state.items = [];
    },
  },
});

export const { pushToast, dismissToast, clearToasts } = toastSlice.actions;
export default toastSlice.reducer;

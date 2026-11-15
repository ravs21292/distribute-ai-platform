import { useEffect } from "react";
import { useAppDispatch, useAppSelector } from "../app/hooks";
import { dismissToast } from "../features/ui/toastSlice";

const AUTO_MS: Record<string, number> = {
  success: 4000,
  info: 4500,
  warning: 6500,
  error: 8000,
};

export function ToastHost() {
  const dispatch = useAppDispatch();
  const items = useAppSelector((s) => s.toast.items);

  useEffect(() => {
    const timers = items.map((t) =>
      window.setTimeout(() => dispatch(dismissToast(t.id)), AUTO_MS[t.kind] ?? 5000),
    );
    return () => timers.forEach((id) => window.clearTimeout(id));
  }, [items, dispatch]);

  if (items.length === 0) return null;

  return (
    <div className="toast-host" aria-live="polite" aria-relevant="additions">
      {items.map((t) => (
        <div key={t.id} className={`toast toast-${t.kind}`} role="status">
          <div className="toast-body">
            <strong>{t.title}</strong>
            <p>{t.message}</p>
          </div>
          <button
            type="button"
            className="toast-close"
            aria-label="Dismiss"
            onClick={() => dispatch(dismissToast(t.id))}
          >
            ×
          </button>
        </div>
      ))}
    </div>
  );
}

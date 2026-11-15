import { Provider } from "react-redux";
import { store } from "./app/store";
import { OnboardingPanel } from "./components/OnboardingPanel";
import { ToastHost } from "./components/ToastHost";
import "./App.css";

export default function App() {
  return (
    <Provider store={store}>
      <OnboardingPanel />
      <ToastHost />
    </Provider>
  );
}

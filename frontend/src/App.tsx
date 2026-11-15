import { useEffect } from 'react'
import { useAppDispatch, useAppSelector } from './app/hooks'
import { connectRequested, setApiKey } from './features/ops/opsSlice'
import {
  ActivityPanel,
  DagPanel,
  DataPanel,
  FailuresPanel,
  FleetPanel,
  HeaderBar,
  Kpis,
  ProgressPanel,
} from './components/OpsPanels'
import './App.css'

export default function App() {
  const dispatch = useAppDispatch()
  const apiKey = useAppSelector((s) => s.ops.apiKey)
  const err = useAppSelector((s) => s.ops.lastError)

  useEffect(() => {
    dispatch(connectRequested())
  }, [dispatch])

  return (
    <div className="app">
      <HeaderBar />
      <div className="toolbar">
        <label>
          API key (optional)
          <input
            value={apiKey}
            onChange={(e) => dispatch(setApiKey(e.target.value))}
            placeholder="X-API-Key for production auth"
          />
        </label>
        <button type="button" onClick={() => dispatch(connectRequested())}>
          Reconnect WS
        </button>
        {err && <span className="err">{err}</span>}
      </div>
      <Kpis />
      <ProgressPanel />
      <main className="grid">
        <FleetPanel />
        <DataPanel />
        <DagPanel />
        <ActivityPanel />
        <FailuresPanel />
      </main>
    </div>
  )
}

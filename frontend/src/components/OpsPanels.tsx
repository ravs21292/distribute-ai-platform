import type { CSSProperties } from 'react'
import { useAppSelector } from '../app/hooks'

const pill = (status: string): CSSProperties => {
  const s = status.toLowerCase()
  let color = '#8b9bb4'
  if (['succeeded', 'completed', 'ready', 'idle', 'healthy'].includes(s)) color = '#2ee59d'
  if (['running', 'busy', 'queued', 'processing', 'ingesting'].includes(s)) color = '#5aa8ff'
  if (['failed', 'dead_letter', 'cancelled', 'error'].includes(s)) color = '#ff5d73'
  return {
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: 999,
    border: `1px solid ${color}55`,
    color,
    fontSize: 11,
    background: `${color}18`,
  }
}

export function HeaderBar() {
  const connection = useAppSelector((s) => s.ops.connection)
  const overview = useAppSelector((s) => s.ops.overview)
  const mode = overview?.cluster.mode ?? '—'
  return (
    <header className="top">
      <div>
        <h1>DML Execution Console</h1>
        <p>Live worker fleet, DAG handoff, embedding progress, and ETA — Redux + WebSocket</p>
      </div>
      <div className="mode">
        <span className={`dot ${mode === 'processing' ? 'on' : connection === 'live' ? 'ok' : 'bad'}`} />
        <div>
          <strong>{mode === 'processing' ? 'IN ACTION' : mode.toUpperCase()}</strong>
          <div className="muted">WS {connection} · {overview?.server_time ? new Date(overview.server_time).toLocaleTimeString() : '—'}</div>
        </div>
      </div>
    </header>
  )
}

export function Kpis() {
  const c = useAppSelector((s) => s.ops.overview?.cluster)
  if (!c) return null
  const items: [string, string | number][] = [
    ['Live workers', c.workers_live],
    ['Busy / idle', `${c.workers_busy} / ${c.workers_idle}`],
    ['Pipelines', c.pipelines_active],
    ['Tasks running', c.tasks_running],
    ['Queued', c.tasks_queued],
    ['Dead letter', c.tasks_dead_letter],
  ]
  return (
    <div className="kpis">
      {items.map(([k, v]) => (
        <div className="kpi" key={k}>
          <span>{k}</span>
          <b>{v}</b>
        </div>
      ))}
    </div>
  )
}

export function ProgressPanel() {
  const p = useAppSelector((s) => s.ops.overview?.progress)
  if (!p) return <section className="panel"><h2>Progress / ETA</h2><div className="muted">Connecting…</div></section>
  return (
    <section className="panel wide">
      <h2>Progress / ETA</h2>
      <div className="progress-head">
        <div>
          <div className="phase">Phase: <strong>{p.phase}</strong></div>
          <div className="muted">{p.message}</div>
        </div>
        <div className="eta">
          <div className="eta-big">{p.eta_human ?? (p.percent >= 100 ? 'Done' : 'Estimating…')}</div>
          <div className="muted">
            {p.rate_embeddings_per_min ? `${p.rate_embeddings_per_min} embed tasks/min` : ''}
            {p.rate_tasks_per_min ? ` · ${p.rate_tasks_per_min} tasks/min` : ''}
          </div>
        </div>
      </div>
      <div className="bar"><i style={{ width: `${Math.min(100, p.percent)}%` }} /></div>
      <div className="progress-meta">
        <span>{p.percent}%</span>
        <span>DAG nodes {p.nodes_done}/{p.nodes_total}</span>
        <span>Embeddings {p.embeddings_done}/{p.embeddings_total}</span>
        <span>Tasks {p.tasks_done}/{p.tasks_total}</span>
      </div>
    </section>
  )
}

export function FleetPanel() {
  const workers = useAppSelector((s) => s.ops.overview?.workers) ?? []
  return (
    <section className="panel">
      <h2>Worker fleet</h2>
      <div className="fleet">
        {workers.length === 0 && <div className="muted">No live workers yet — start docker compose.</div>}
        {workers.map((w) => (
          <div className="worker" key={w.worker_id}>
            <span className={`dot ${w.status === 'busy' ? 'on' : 'ok'}`} />
            <div>
              <div className="wid">{w.worker_id}</div>
              <div className="muted">{w.role || 'worker'} · {(w.capabilities || []).join(', ')}</div>
            </div>
            <div>
              <span style={pill(w.status)}>{w.status}</span>
              <div className="muted">
                {w.current_task_type
                  ? `running ${w.current_task_type} · attempt ${w.current_attempt ?? 1}`
                  : 'waiting for work'}
              </div>
            </div>
          </div>
        ))}
      </div>
    </section>
  )
}

export function DagPanel() {
  const overview = useAppSelector((s) => s.ops.overview)
  const focus = overview?.pipeline_focus
  const pipe =
    overview?.pipelines.find((p) => p.status === 'running') ||
    (focus
      ? overview?.pipelines.find((p) => p.id === focus.pipeline_id)
      : undefined) ||
    overview?.pipelines[0] ||
    null
  const template = overview?.dag
  if (!pipe || !template) {
    return (
      <section className="panel wide">
        <h2>Pipeline DAG</h2>
        <div className="muted">No pipeline yet. Upload a real dataset version and POST /v1/pipelines/run.</div>
      </section>
    )
  }
  const byName = Object.fromEntries(pipe.nodes.map((n) => [n.name, n]))
  const groups: [string, Array<{ name: string }>][] = [
    ['Data: ingest → vector index', template.data],
    ['Train / eval / promote', template.train],
  ]
  const mb = focus ? (focus.source_bytes / (1024 * 1024)).toFixed(2) : null
  return (
    <section className="panel wide">
      <h2>
        Pipeline DAG · {focus?.dataset_name || 'dataset'} {focus?.version_label || ''}
      </h2>
      <div className="muted" style={{ marginBottom: 10 }}>
        {focus?.note ||
          `Pipeline ${pipe.id.slice(0, 8)}… · status ${pipe.status}. Green nodes mean this version finished — not that multi‑GB corpora embedded in seconds.`}
        {mb !== null && (
          <>
            {' '}
            <strong>
              {mb} MB · {focus?.chunks ?? 0} chunks · {focus?.embeddings ?? 0} embeds
            </strong>
          </>
        )}
      </div>
      {groups.map(([label, group]) =>
        group.length ? (
          <div key={label} className="dag-group">
            <div className="muted">{label}</div>
            <div className="dag">
              {group.map((n, i) => {
                const live = byName[n.name] || { status: 'pending', worker_id: null }
                return (
                  <div key={n.name} className="dag-item">
                    {i > 0 && <span className="arrow">→</span>}
                    <div className="node">
                      <strong>{n.name}</strong>
                      <span style={pill(live.status)}>{live.status}</span>
                      <small>{live.worker_id || (live.status === 'running' ? 'claiming…' : '—')}</small>
                    </div>
                  </div>
                )
              })}
            </div>
          </div>
        ) : null,
      )}
    </section>
  )
}

export function ActivityPanel() {
  const activity = useAppSelector((s) => s.ops.overview?.activity) ?? []
  return (
    <section className="panel">
      <h2>Task handoffs (worker activity)</h2>
      <table>
        <thead>
          <tr>
            <th>time</th>
            <th>worker</th>
            <th>task</th>
            <th>status</th>
            <th>ms</th>
            <th>detail</th>
          </tr>
        </thead>
        <tbody>
          {activity.slice(0, 25).map((a) => (
            <tr key={`${a.task_id}-${a.at}-${a.status}`}>
              <td>{new Date(a.at).toLocaleTimeString()}</td>
              <td>{a.worker_id || '—'}</td>
              <td>{a.task_type}</td>
              <td><span style={pill(a.status)}>{a.status}</span></td>
              <td>{a.duration_ms ?? '—'}</td>
              <td className="muted">{a.detail || ''}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  )
}

export function DataPanel() {
  const d = useAppSelector((s) => s.ops.overview?.data)
  const focus = useAppSelector((s) => s.ops.overview?.pipeline_focus)
  if (!d) return null
  return (
    <section className="panel">
      <h2>Data plane</h2>
      <table>
        <tbody>
          <tr>
            <th>Latest version</th>
            <td>
              {d.latest_dataset || '—'} {d.latest_version_label}{' '}
              <span style={pill(d.latest_status || 'none')}>{d.latest_status || 'none'}</span>
            </td>
          </tr>
          <tr>
            <th>Latest source</th>
            <td>{(d.source_bytes / (1024 * 1024)).toFixed(1)} MB</td>
          </tr>
          <tr>
            <th>Postgres (all versions)</th>
            <td>
              {d.documents} docs · {d.chunks.toLocaleString()} chunks
              <div className="muted">Includes failed/partial ETL leftovers — not only the green DAG.</div>
            </td>
          </tr>
          <tr>
            <th>pgvector (all)</th>
            <td>
              {d.embeddings.toLocaleString()} embeddings · {d.embedding_backend} · dim {d.embedding_dim}
            </td>
          </tr>
          {focus && (
            <tr>
              <th>DAG version scope</th>
              <td>
                {focus.dataset_name} {focus.version_label}: {focus.chunks.toLocaleString()} chunks ·{' '}
                {focus.embeddings.toLocaleString()} embeds · {(focus.source_bytes / (1024 * 1024)).toFixed(2)} MB
              </td>
            </tr>
          )}
          <tr>
            <th>Model</th>
            <td className="muted">{d.embedding_model}</td>
          </tr>
        </tbody>
      </table>
    </section>
  )
}

export function FailuresPanel() {
  const failures = useAppSelector((s) => s.ops.overview?.failures) ?? []
  const policy = useAppSelector((s) => s.ops.overview?.failure_policy)
  return (
    <section className="panel">
      <h2>Failure handling</h2>
      {policy && (
        <p className="muted">
          Lease {String(policy.lease_seconds)}s · reclaim via {String(policy.reclaim)} · ACK after{' '}
          {String(policy.ack_after)} · max {String(policy.max_attempts)} attempts then dead-letter.
        </p>
      )}
      {failures.length === 0 ? (
        <div className="muted">No failed / dead-letter tasks.</div>
      ) : (
        <table>
          <thead>
            <tr><th>type</th><th>status</th><th>attempts</th><th>worker</th><th>error</th></tr>
          </thead>
          <tbody>
            {failures.map((f) => (
              <tr key={f.task_id}>
                <td>{f.task_type}</td>
                <td><span style={pill(f.status)}>{f.status}</span></td>
                <td>{f.attempts}/{f.max_attempts}</td>
                <td>{f.worker_id || '—'}</td>
                <td className="muted">{(f.error || '').slice(0, 120)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}

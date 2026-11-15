export type Worker = {
  worker_id: string
  status: string
  healthy: boolean
  role?: string
  device?: string
  capabilities?: string[]
  current_task_type?: string | null
  current_attempt?: number | null
  heartbeat_age_s?: number | null
}

export type PipelineNode = {
  name: string
  task_type: string
  depends_on: string[]
  status: string
  job_id?: string | null
  worker_id?: string | null
}

export type Pipeline = {
  id: string
  dataset_version_id: string
  status: string
  include_training: boolean
  nodes: PipelineNode[]
}

export type Progress = {
  phase: string
  percent: number
  embeddings_done: number
  embeddings_total: number
  tasks_done: number
  tasks_total: number
  nodes_done: number
  nodes_total: number
  eta_seconds: number | null
  eta_human: string | null
  rate_embeddings_per_min: number | null
  rate_tasks_per_min: number | null
  message: string
}

export type Activity = {
  at: string
  worker_id: string | null
  task_type: string
  status: string
  task_id: string
  job_id: string | null
  duration_ms: number | null
  detail?: string | null
}

export type Overview = {
  cluster: {
    mode: string
    workers_live: number
    workers_busy: number
    workers_idle: number
    pipelines_active: number
    jobs_running: number
    tasks_queued: number
    tasks_running: number
    tasks_failed: number
    tasks_dead_letter: number
    tasks_succeeded: number
  }
  data: {
    datasets: number
    versions_ready: number
    documents: number
    chunks: number
    embeddings: number
    embedding_model: string
    embedding_backend: string
    embedding_dim: number
    latest_dataset: string | null
    latest_version_id: string | null
    latest_version_label: string | null
    latest_status: string | null
    source_bytes: number
  }
  progress: Progress
  pipeline_focus?: {
    pipeline_id: string
    status: string
    dataset_name: string | null
    version_label: string | null
    dataset_version_id: string
    version_status: string | null
    source_bytes: number
    chunks: number
    embeddings: number
    note: string
  } | null
  workers: Worker[]
  pipelines: Pipeline[]
  jobs: Array<{
    id: string
    name: string | null
    job_type: string
    status: string
    task_count: number
    created_at?: string | null
  }>
  activity: Activity[]
  failures: Array<{
    task_id: string
    task_type: string
    status: string
    attempts: number
    max_attempts: number
    error?: string | null
    worker_id?: string | null
  }>
  failure_policy: Record<string, unknown>
  dag: {
    data: Array<{ name: string; depends_on: string[] }>
    train: Array<{ name: string; depends_on: string[] }>
  }
  server_time: string
}

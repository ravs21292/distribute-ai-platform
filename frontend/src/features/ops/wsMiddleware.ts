import type { Middleware } from '@reduxjs/toolkit'
import {
  connectRequested,
  connectionError,
  connectionLive,
  disconnectRequested,
  overviewReceived,
} from './opsSlice'
import type { Overview } from './types'

let socket: WebSocket | null = null
let reconnectTimer: number | null = null

function wsUrl(apiKey: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss' : 'ws'
  const host = window.location.host
  const q = apiKey ? `?api_key=${encodeURIComponent(apiKey)}` : ''
  return `${proto}://${host}/v1/ops/ws${q}`
}

export const wsMiddleware: Middleware = (storeApi) => (next) => (action) => {
  const result = next(action)
  if (connectRequested.match(action)) {
    if (socket) {
      socket.close()
      socket = null
    }
    const state = storeApi.getState() as { ops: { apiKey: string } }
    try {
      socket = new WebSocket(wsUrl(state.ops.apiKey))
    } catch (err) {
      storeApi.dispatch(connectionError(String(err)))
      return result
    }
    socket.onopen = () => storeApi.dispatch(connectionLive())
    socket.onmessage = (ev) => {
      try {
        const data = JSON.parse(ev.data as string) as Overview & { error?: string }
        if (data.error) {
          storeApi.dispatch(connectionError(String(data.error)))
          return
        }
        storeApi.dispatch(overviewReceived(data))
      } catch (err) {
        storeApi.dispatch(connectionError(`bad frame: ${String(err)}`))
      }
    }
    socket.onerror = () => storeApi.dispatch(connectionError('websocket error'))
    socket.onclose = () => {
      storeApi.dispatch(connectionError('websocket closed'))
      if (reconnectTimer) window.clearTimeout(reconnectTimer)
      reconnectTimer = window.setTimeout(() => {
        storeApi.dispatch(connectRequested())
      }, 3000)
    }
  }
  if (disconnectRequested.match(action)) {
    if (reconnectTimer) window.clearTimeout(reconnectTimer)
    reconnectTimer = null
    if (socket) {
      socket.onclose = null
      socket.close()
      socket = null
    }
  }
  return result
}

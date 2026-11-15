import { createSlice, type PayloadAction } from '@reduxjs/toolkit'
import type { Overview } from './types'

export type ConnState = 'idle' | 'connecting' | 'live' | 'error'

type OpsState = {
  overview: Overview | null
  connection: ConnState
  lastError: string | null
  apiKey: string
  autoConnect: boolean
}

const initialState: OpsState = {
  overview: null,
  connection: 'idle',
  lastError: null,
  apiKey: localStorage.getItem('dml_api_key') || '',
  autoConnect: true,
}

const opsSlice = createSlice({
  name: 'ops',
  initialState,
  reducers: {
    connectRequested(state) {
      state.connection = 'connecting'
      state.lastError = null
    },
    connectionLive(state) {
      state.connection = 'live'
    },
    connectionError(state, action: PayloadAction<string>) {
      state.connection = 'error'
      state.lastError = action.payload
    },
    overviewReceived(state, action: PayloadAction<Overview>) {
      state.overview = action.payload
      state.connection = 'live'
      state.lastError = null
    },
    setApiKey(state, action: PayloadAction<string>) {
      state.apiKey = action.payload
      localStorage.setItem('dml_api_key', action.payload)
    },
    disconnectRequested(state) {
      state.connection = 'idle'
    },
  },
})

export const {
  connectRequested,
  connectionLive,
  connectionError,
  overviewReceived,
  setApiKey,
  disconnectRequested,
} = opsSlice.actions

export default opsSlice.reducer

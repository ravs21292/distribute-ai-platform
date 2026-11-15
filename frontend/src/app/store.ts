import { configureStore } from '@reduxjs/toolkit'
import opsReducer from '../features/ops/opsSlice'
import { wsMiddleware } from '../features/ops/wsMiddleware'

export const store = configureStore({
  reducer: {
    ops: opsReducer,
  },
  middleware: (getDefault) => getDefault().concat(wsMiddleware),
})

export type RootState = ReturnType<typeof store.getState>
export type AppDispatch = typeof store.dispatch

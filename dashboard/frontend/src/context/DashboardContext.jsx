import { createContext, useContext, useState } from 'react'

const DashboardContext = createContext(null)

export function DashboardProvider({ children }) {
  const [symbol, setSymbol] = useState('BTCUSDT')
  const [interval, setInterval] = useState('1h')

  return (
    <DashboardContext.Provider value={{ symbol, setSymbol, interval, setInterval }}>
      {children}
    </DashboardContext.Provider>
  )
}

export const useDashboard = () => useContext(DashboardContext)

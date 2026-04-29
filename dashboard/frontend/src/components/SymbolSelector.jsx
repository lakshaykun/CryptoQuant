import { useQuery } from '@tanstack/react-query'
import { fetchSymbols } from '../api/client'
import { useDashboard } from '../context/DashboardContext'

export function SymbolSelector() {
  const { symbol, setSymbol } = useDashboard()
  const { data: symbols = [] } = useQuery({
    queryKey: ['symbols'],
    queryFn: fetchSymbols,
    staleTime: 60_000,
    refetchInterval: 60_000,
  })

  return (
    <select
      className="select-input"
      value={symbol}
      onChange={e => setSymbol(e.target.value)}
    >
      {symbols.length === 0
        ? <option value={symbol}>{symbol}</option>
        : symbols.map(s => <option key={s} value={s}>{s}</option>)
      }
    </select>
  )
}

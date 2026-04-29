export function Spinner() {
  return <div className="spinner" />
}

export function ChartPlaceholder({ loading, error, empty }) {
  if (loading) return <div className="chart-placeholder"><Spinner /><span>Loading…</span></div>
  if (error)   return <div className="chart-placeholder"><p className="error-msg">⚠ {error?.message || 'Failed to load'}</p></div>
  if (empty)   return <div className="chart-placeholder"><p>No data available</p></div>
  return null
}

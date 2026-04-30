export function KpiCard({ label, value, sub, variant = '' }) {
  return (
    <div className={`kpi-card ${variant}`}>
      <div className="kpi-label">{label}</div>
      <div className="kpi-value">{value ?? '—'}</div>
      {sub && <div className="kpi-sub">{sub}</div>}
    </div>
  )
}

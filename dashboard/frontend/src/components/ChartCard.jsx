export function ChartCard({ title, controls, children, minHeight = 280 }) {
  return (
    <div className="card">
      <div className="card-header">
        <span className="card-title">{title}</span>
        {controls && <div className="card-controls">{controls}</div>}
      </div>
      <div className="card-body" style={{ minHeight }}>
        {children}
      </div>
    </div>
  )
}

export function IntervalSelector({ value, onChange }) {
  const options = ['1m', '5m', '1h', '1d']
  return (
    <div className="btn-group">
      {options.map(iv => (
        <button
          key={iv}
          className={value === iv ? 'active' : ''}
          onClick={() => onChange(iv)}
        >
          {iv}
        </button>
      ))}
    </div>
  )
}

export function LimitInput({ value, onChange, max = 500, label = 'Limit' }) {
  return (
    <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span className="ctrl-label">{label}</span>
      <input
        type="number"
        className="number-input"
        value={value}
        min={10}
        max={max}
        step={10}
        onChange={e => onChange(Number(e.target.value))}
      />
    </label>
  )
}

export function DateRangePicker({ from, to, onFromChange, onToChange }) {
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span className="ctrl-label">From</span>
      <input type="datetime-local" className="date-input" value={from} onChange={e => onFromChange(e.target.value)} />
      <span className="ctrl-label">To</span>
      <input type="datetime-local" className="date-input" value={to} onChange={e => onToChange(e.target.value)} />
    </div>
  )
}

export function LagInput({ value, onChange }) {
  return (
    <label style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <span className="ctrl-label">Max Lag</span>
      <input
        type="number"
        className="number-input"
        value={value}
        min={2}
        max={20}
        onChange={e => onChange(Number(e.target.value))}
      />
    </label>
  )
}

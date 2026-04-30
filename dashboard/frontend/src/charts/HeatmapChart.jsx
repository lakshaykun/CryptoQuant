// Hourly × Weekday heatmap rendered as an SVG grid
const DAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']

function lerp(a, b, t) { return a + (a - b) * -t }

function returnColor(val) {
  if (val == null) return '#1e2842'
  const t = Math.max(-1, Math.min(1, val * 50)) // scale tiny floats
  if (t >= 0) {
    // green channel
    const g = Math.round(lerp(40, 197, t))
    return `rgba(34,${g},94,${0.15 + 0.7 * Math.abs(t)})`
  } else {
    // red channel
    const r = Math.round(lerp(150, 244, -t))
    return `rgba(${r},63,94,${0.15 + 0.7 * Math.abs(t)})`
  }
}

export function HeatmapChart({ data }) {
  // Build 7×24 grid: grid[dayOfWeek][hour] = avg_return
  const grid = Array.from({ length: 7 }, () => Array(24).fill(null))
  ;(data || []).forEach(d => {
    const dow = Number(d.day_of_week) // 0=Mon … 6=Sun (or depends on DB)
    const h   = Number(d.hour)
    if (dow >= 0 && dow < 7 && h >= 0 && h < 24) {
      grid[dow][h] = d.avg_return
    }
  })

  const CELL = 28
  const LEFT = 36
  const TOP  = 24
  const W = LEFT + 24 * CELL + 4
  const H = TOP  + 7  * CELL + 4

  return (
    <div className="heatmap-wrap">
      <svg
        width={W}
        height={H}
        style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, letterSpacing: 0 }}
      >
        {/* Hour labels */}
        {Array.from({ length: 24 }, (_, h) => (
          <text
            key={h}
            x={LEFT + h * CELL + CELL / 2}
            y={TOP - 6}
            textAnchor="middle"
            fill="#4a5a7a"
          >
            {h}
          </text>
        ))}

        {/* Day rows */}
        {DAYS.map((day, di) => (
          <g key={day}>
            <text
              x={LEFT - 4}
              y={TOP + di * CELL + CELL / 2 + 4}
              textAnchor="end"
              fill="#8899bb"
            >
              {day}
            </text>
            {Array.from({ length: 24 }, (_, h) => {
              const val = grid[di][h]
              return (
                <g key={h}>
                  <rect
                    x={LEFT + h * CELL + 1}
                    y={TOP + di * CELL + 1}
                    width={CELL - 2}
                    height={CELL - 2}
                    rx={3}
                    fill={returnColor(val)}
                  />
                  <title>{`${day} ${h}:00 — avg return: ${val != null ? val.toFixed(6) : 'N/A'}`}</title>
                </g>
              )
            })}
          </g>
        ))}
      </svg>

      {/* Legend */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginTop: 8, fontSize: 11, color: '#8899bb' }}>
        <span>Bearish</span>
        <div style={{
          width: 120, height: 10, borderRadius: 4,
          background: 'linear-gradient(to right, rgba(244,63,94,0.8), #1e2842, rgba(34,197,94,0.8))'
        }} />
        <span>Bullish</span>
      </div>
    </div>
  )
}

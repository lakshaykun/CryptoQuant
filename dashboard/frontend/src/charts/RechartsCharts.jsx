import {
  AreaChart, Area, BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend,
  ResponsiveContainer, ReferenceLine, Cell, ScatterChart, Scatter
} from 'recharts'
import { format } from 'date-fns'

const fmt = ts => format(new Date(ts), 'MM/dd HH:mm')
const fmtDate = ts => format(new Date(ts), 'MM/dd')
const TICK_COLOR = '#8899bb'
const GRID_COLOR = '#1e2842'

// ── tooltip shared style ──────────────────────────────────────────────────
const TT_STYLE = {
  backgroundColor: '#161d2f',
  border: '1px solid #2a3550',
  borderRadius: 8,
  fontSize: 11,
  fontFamily: "'JetBrains Mono', monospace",
}

// ── 1. Volatility Ribbon ─────────────────────────────────────────────────
export function VolatilityRibbonChart({ data }) {
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={260}>
      <AreaChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <defs>
          <linearGradient id="gVol" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#4f8ef7" stopOpacity={0.35} />
            <stop offset="95%" stopColor="#4f8ef7" stopOpacity={0.02} />
          </linearGradient>
          <linearGradient id="gVol5" x1="0" y1="0" x2="0" y2="1">
            <stop offset="5%" stopColor="#a78bfa" stopOpacity={0.3} />
            <stop offset="95%" stopColor="#a78bfa" stopOpacity={0.02} />
          </linearGradient>
        </defs>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Area type="monotone" dataKey="volatility"   name="Volatility"    stroke="#4f8ef7" fill="url(#gVol)"  strokeWidth={1.5} dot={false} />
        <Area type="monotone" dataKey="volatility_5" name="Volatility 5p"  stroke="#a78bfa" fill="url(#gVol5)" strokeWidth={1.5} dot={false} />
      </AreaChart>
    </ResponsiveContainer>
  )
}

// ── 2. Volatility Ribbon + Ratio (Diagnostic) ─────────────────────────────
export function VolatilityRatioChart({ data }) {
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis yAxisId="v" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis yAxisId="r" orientation="right" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Line yAxisId="v" type="monotone" dataKey="volatility"       name="Volatility"       stroke="#4f8ef7" dot={false} strokeWidth={1.5} />
        <Line yAxisId="v" type="monotone" dataKey="volatility_5"     name="Volatility 5p"    stroke="#a78bfa" dot={false} strokeWidth={1.5} />
        <Line yAxisId="r" type="monotone" dataKey="volatility_ratio" name="Vol Ratio (right)" stroke="#f59e0b" dot={false} strokeWidth={1.5} />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ── 3. Volume Spike Timeline ─────────────────────────────────────────────
export function VolumeSpikeChart({ data }) {
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={220}>
      <BarChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmtDate} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis dataKey="momentum" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Bar dataKey="momentum" name="Momentum" radius={[2,2,0,0]}>
          {sorted.map((d, i) => (
            <Cell key={i} fill={d.volume_spike ? '#f43f5e' : '#4f8ef774'} />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── 4. Sentiment + Price Overlay ─────────────────────────────────────────
export function SentimentPriceChart({ candles, sentiment }) {
  const priceMap = {}
  ;(candles || []).forEach(c => {
    priceMap[new Date(c.open_time).toISOString()] = c.close
  })

  const merged = [...(sentiment || [])]
    .reverse()
    .map(s => ({
      time:  s.window_start,
      score: s.sentiment_index,
      price: priceMap[new Date(s.window_start).toISOString()] ?? null,
    }))
    .filter(d => d.price !== null)

  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={merged} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis yAxisId="p" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis yAxisId="s" orientation="right" tick={{ fill: TICK_COLOR, fontSize: 10 }} domain={[-1, 1]} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Line yAxisId="p" type="monotone" dataKey="price" name="Price"     stroke="#22c55e" dot={false} strokeWidth={1.5} />
        <Line yAxisId="s" type="monotone" dataKey="score" name="Sentiment" stroke="#f59e0b" dot={false} strokeWidth={1.5} />
        <ReferenceLine yAxisId="s" y={0} stroke="#4a5a7a" strokeDasharray="4 2" />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ── 5. Log Return Histogram ───────────────────────────────────────────────
export function ReturnHistogram({ data }) {
  const values = (data || []).map(d => d.log_return).filter(v => v != null)
  if (!values.length) return null

  const min = Math.min(...values)
  const max = Math.max(...values)
  const bins = 40
  const binSize = (max - min) / bins
  const counts = Array.from({ length: bins }, (_, i) => ({
    bin: +(min + i * binSize).toFixed(5),
    count: 0,
  }))
  values.forEach(v => {
    const idx = Math.min(Math.floor((v - min) / binSize), bins - 1)
    counts[idx].count++
  })

  return (
    <ResponsiveContainer width="100%" height={260}>
      <BarChart data={counts} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="bin" tick={{ fill: TICK_COLOR, fontSize: 9 }} />
        <YAxis tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} />
        <Bar dataKey="count" name="Frequency" radius={[2,2,0,0]}>
          {counts.map((d, i) => (
            <Cell key={i} fill={d.bin >= 0 ? '#22c55e99' : '#f43f5e99'} />
          ))}
        </Bar>
        <ReferenceLine x={0} stroke="#8899bb" strokeDasharray="4 2" />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── 6. Order Flow Imbalance ───────────────────────────────────────────────
export function OrderFlowChart({ data }) {
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={260}>
      <LineChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <ReferenceLine y={0} stroke="#4a5a7a" strokeDasharray="4 2" />
        <Line type="monotone" dataKey="imbalance_ratio" name="Imbalance Ratio" stroke="#f43f5e" dot={false} strokeWidth={1.5} />
        <Line type="monotone" dataKey="buy_ratio"        name="Buy Ratio"       stroke="#22c55e" dot={false} strokeWidth={1.5} />
        <Line type="monotone" dataKey="buy_ratio_5"      name="Buy Ratio 5p"    stroke="#4f8ef7" dot={false} strokeWidth={1.5} />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ── 7. Momentum Profile Scatter ───────────────────────────────────────────
export function MomentumScatterChart({ data }) {
  const points = (data || []).map(d => ({
    momentum:       d.momentum,
    trend_strength: d.trend_strength,
    volume_spike:   d.volume_spike,
  }))

  const spikes = points.filter(p => p.volume_spike)
  const normal = points.filter(p => !p.volume_spike)

  return (
    <ResponsiveContainer width="100%" height={280}>
      <ScatterChart margin={{ top: 8, right: 16, bottom: 8, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="momentum" name="Momentum" type="number" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis dataKey="trend_strength" name="Trend Strength" type="number" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} cursor={{ strokeDasharray: '3 3' }} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Scatter name="Normal"       data={normal} fill="#4f8ef7" opacity={0.6} />
        <Scatter name="Vol Spike"    data={spikes} fill="#f43f5e" opacity={0.85} />
      </ScatterChart>
    </ResponsiveContainer>
  )
}

// ── 8. Candlestick Anatomy ────────────────────────────────────────────────
export function CandleAnatomyChart({ data }) {
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={240}>
      <LineChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Line type="monotone" dataKey="body_size"         name="Body Size"         stroke="#4f8ef7" dot={false} strokeWidth={1.5} />
        <Line type="monotone" dataKey="price_range_ratio" name="Price Range Ratio" stroke="#f59e0b" dot={false} strokeWidth={1.5} />
      </LineChart>
    </ResponsiveContainer>
  )
}

// ── 9. Sentiment by Source bar chart ─────────────────────────────────────
export function SentimentBySourceChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={240}>
      <BarChart data={data || []} layout="vertical" margin={{ top: 4, right: 16, bottom: 4, left: 60 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis type="number" tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis type="category" dataKey="source" tick={{ fill: TICK_COLOR, fontSize: 11 }} />
        <Tooltip contentStyle={TT_STYLE} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Bar dataKey="message_count"    name="Messages"    fill="#4f8ef7" radius={[0,4,4,0]} />
        <Bar dataKey="total_engagement" name="Engagement"  fill="#a78bfa" radius={[0,4,4,0]} />
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── 10. Sentiment-Price Lag Correlation ──────────────────────────────────
export function LagCorrelationChart({ data }) {
  return (
    <ResponsiveContainer width="100%" height={240}>
      <BarChart data={data || []} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="lag" tick={{ fill: TICK_COLOR, fontSize: 10 }} label={{ value: 'Lag (periods)', position: 'insideBottom', fill: TICK_COLOR, fontSize: 11 }} />
        <YAxis domain={[-1, 1]} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} formatter={v => [v?.toFixed(4), 'Correlation']} />
        <ReferenceLine y={0} stroke="#4a5a7a" />
        <Bar dataKey="correlation" name="Correlation" radius={[2,2,0,0]}>
          {(data || []).map((d, i) => (
            <Cell key={i} fill={
              d.correlation == null ? '#4a5a7a' :
              d.correlation > 0.1 ? '#22c55e99' :
              d.correlation < -0.1 ? '#f43f5e99' : '#4f8ef755'
            } />
          ))}
        </Bar>
      </BarChart>
    </ResponsiveContainer>
  )
}

// ── 11. Prediction vs Actual ──────────────────────────────────────────────
export function PredictionVsActualChart({ data }) {
  // data is forecast array which has predicted_close; actual_close comes from same endpoint
  const sorted = [...(data || [])].reverse()
  return (
    <ResponsiveContainer width="100%" height={280}>
      <LineChart data={sorted} margin={{ top: 4, right: 16, bottom: 0, left: 0 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={GRID_COLOR} />
        <XAxis dataKey="open_time" tickFormatter={fmt} tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <YAxis tick={{ fill: TICK_COLOR, fontSize: 10 }} />
        <Tooltip contentStyle={TT_STYLE} labelFormatter={fmt} />
        <Legend wrapperStyle={{ fontSize: 12, color: TICK_COLOR }} />
        <Line type="monotone" dataKey="predicted_close" name="Predicted" stroke="#4f8ef7" dot={false} strokeWidth={2} strokeDasharray="5 3" />
        <Line type="monotone" dataKey="actual_close"    name="Actual"    stroke="#22c55e" dot={false} strokeWidth={1.5} />
      </LineChart>
    </ResponsiveContainer>
  )
}

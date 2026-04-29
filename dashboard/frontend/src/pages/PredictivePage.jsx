import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useDashboard } from '../context/DashboardContext'
import { fetchCandles, fetchForecast, fetchAccuracy } from '../api/client'
import { ChartCard } from '../components/ChartCard'
import { KpiCard } from '../components/KpiCard'
import { ChartPlaceholder } from '../components/Spinner'
import { LimitInput, IntervalSelector } from '../components/Controls'
import { ForecastCandlestickChart } from '../charts/CandlestickChart'
import { PredictionVsActualChart } from '../charts/RechartsCharts'

const REFETCH = 30_000

export default function PredictivePage() {
  const { symbol, interval, setInterval } = useDashboard()
  const [candleLimit, setCandleLimit] = useState(100)
  const [forecastLimit, setForecastLimit] = useState(30)
  const [accuracyLimit, setAccuracyLimit] = useState(100)

  const candlesQ    = useQuery({ queryKey: ['candles', symbol, interval, candleLimit],        queryFn: () => fetchCandles({ symbol, interval, limit: candleLimit }),     staleTime: 15_000, refetchInterval: REFETCH })
  const forecastQ   = useQuery({ queryKey: ['forecast', symbol, forecastLimit],               queryFn: () => fetchForecast({ symbol, limit: forecastLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const accuracyQ   = useQuery({ queryKey: ['accuracy', symbol, accuracyLimit],               queryFn: () => fetchAccuracy({ symbol, limit: accuracyLimit }),            staleTime: 60_000, refetchInterval: 120_000 })

  // ── Direction indicator ───────────────────────────────────────────────
  const latestForecast = forecastQ.data?.[0]
  const latestCandle   = candlesQ.data?.[0]
  const predictedClose = latestForecast?.predicted_close
  const currentClose   = latestCandle?.close
  const isUp = predictedClose != null && currentClose != null && predictedClose > currentClose
  const directionLabel = predictedClose == null ? '—' : isUp ? '▲ UP' : '▼ DOWN'
  const dirVariant = predictedClose == null ? '' : isUp ? 'success' : 'danger'
  const changePct = predictedClose != null && currentClose != null
    ? (((predictedClose - currentClose) / currentClose) * 100).toFixed(3) + '%'
    : '—'

  // ── Accuracy KPIs ─────────────────────────────────────────────────────
  const acc = accuracyQ.data
  const mae              = acc?.mae              != null ? acc.mae.toFixed(4)                  : '—'
  const dirAcc           = acc?.directional_accuracy != null ? acc.directional_accuracy.toFixed(1) + '%' : '—'
  const totalPredictions = acc?.total_predictions    != null ? acc.total_predictions.toLocaleString() : '—'
  const matched          = acc?.matched_predictions  != null ? acc.matched_predictions.toLocaleString() : '—'

  return (
    <>
      {/* ── Accuracy KPI strip ───────────────────────────────────────── */}
      <div className="kpi-strip">
        <KpiCard label="Total Predictions" value={totalPredictions} sub="All stored predictions" variant="accent" />
        <KpiCard label="Matched (w/ actual)" value={matched}         sub="Have actual_close"     variant="purple" />
        <KpiCard label="MAE"                 value={mae}             sub="Mean absolute error"   variant="warning" />
        <KpiCard label="Directional Acc."   value={dirAcc}          sub="% direction correct"   variant={acc?.directional_accuracy > 50 ? 'success' : 'danger'} />
      </div>

      {/* ── Direction Indicator ───────────────────────────────────────── */}
      <div className="kpi-strip">
        <div className={`kpi-card ${dirVariant}`} style={{ flexDirection: 'row', alignItems: 'center', gap: 16 }}>
          <div>
            <div className="kpi-label">Predicted Direction</div>
            <div className={`direction-badge ${isUp ? 'up' : predictedClose != null ? 'down' : ''}`}>
              {directionLabel}
            </div>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div className="kpi-label">Current Close</div>
            <div className="kpi-value" style={{ fontSize: 18 }}>
              {currentClose != null ? '$' + currentClose.toFixed(2) : '—'}
            </div>
            <div className="kpi-label" style={{ marginTop: 4 }}>Predicted Close</div>
            <div className="kpi-value" style={{ fontSize: 18 }}>
              {predictedClose != null ? '$' + predictedClose.toFixed(2) : '—'}
            </div>
          </div>
          <div>
            <div className="kpi-label">Expected Δ</div>
            <div className={`kpi-value ${dirVariant}`} style={{ fontSize: 18 }}>{changePct}</div>
          </div>
        </div>
      </div>

      {/* ── Forecast Candlestick chart ────────────────────────────────── */}
      <ChartCard
        title={`${symbol} — Price Forecast Extension`}
        controls={
          <>
            <IntervalSelector value={interval} onChange={setInterval} />
            <LimitInput value={candleLimit} onChange={setCandleLimit} max={1000} label="Candles" />
            <LimitInput value={forecastLimit} onChange={setForecastLimit} max={60} label="Forecast pts" />
          </>
        }
        minHeight={420}
      >
        {(candlesQ.isLoading || forecastQ.isLoading)
          ? <ChartPlaceholder loading />
          : candlesQ.isError
            ? <ChartPlaceholder error={candlesQ.error} />
            : <ForecastCandlestickChart candles={candlesQ.data} forecast={forecastQ.data} />
        }
      </ChartCard>

      {/* ── Prediction vs Actual ─────────────────────────────────────── */}
      <ChartCard
        title="Prediction vs Actual Close"
        controls={<LimitInput value={forecastLimit} onChange={setForecastLimit} max={60} />}
        minHeight={300}
      >
        {forecastQ.isLoading || forecastQ.isError || !forecastQ.data?.length
          ? <ChartPlaceholder loading={forecastQ.isLoading} error={forecastQ.error} empty={!forecastQ.data?.length} />
          : <PredictionVsActualChart data={forecastQ.data} />
        }
      </ChartCard>
    </>
  )
}

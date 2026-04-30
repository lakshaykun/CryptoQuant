import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useDashboard } from '../context/DashboardContext'
import {
  fetchCandles, fetchVolatility, fetchMomentum, fetchSentimentTimeline, fetchSentimentSummary, fetchReturns,
  fetchOrderFlow, fetchMicrostructure, fetchSentimentBySource, fetchSentimentCorrelation,
  fetchForecast, fetchAccuracy
} from '../api/client'
import { ChartCard } from '../components/ChartCard'
import { KpiCard } from '../components/KpiCard'
import { ChartPlaceholder } from '../components/Spinner'
import { IntervalSelector, LimitInput, LagInput } from '../components/Controls'
import { CandlestickChart, ForecastCandlestickChart } from '../charts/CandlestickChart'
import {
  VolatilityRibbonChart, VolumeSpikeChart, SentimentPriceChart,
  ReturnHistogram, VolatilityRatioChart, OrderFlowChart,
  MomentumScatterChart, CandleAnatomyChart, SentimentBySourceChart, LagCorrelationChart,
  PredictionVsActualChart
} from '../charts/RechartsCharts'
import { HeatmapChart } from '../charts/HeatmapChart'
import { format } from 'date-fns'

const REFETCH = 30_000

export default function DashboardPage() {
  const { symbol, interval, setInterval } = useDashboard()
  
  // State for all limits
  const [candleLimit, setCandleLimit]       = useState(300)
  const [forecastLimit, setForecastLimit]   = useState(30)
  const [accuracyLimit, setAccuracyLimit]   = useState(100)
  const [returnLimit, setReturnLimit]       = useState(1000)
  const [tableReturnLimit, setTableReturnLimit] = useState(500)
  const [volatilityLimit, setVolatilityLimit] = useState(300)
  const [momentumLimit, setMomentumLimit]   = useState(300)
  const [sentimentLimit, setSentimentLimit] = useState(300)
  const [ofLimit, setOfLimit]               = useState(200)
  const [microLimit, setMicroLimit]         = useState(300)
  const [maxLag, setMaxLag]                 = useState(10)

  // Queries
  const candlesQ     = useQuery({ queryKey: ['candles', symbol, interval, candleLimit],     queryFn: () => fetchCandles({ symbol, interval, limit: candleLimit }),       staleTime: 15_000, refetchInterval: REFETCH })
  const forecastQ    = useQuery({ queryKey: ['forecast', symbol, forecastLimit],            queryFn: () => fetchForecast({ symbol, limit: forecastLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const accuracyQ    = useQuery({ queryKey: ['accuracy', symbol, accuracyLimit],            queryFn: () => fetchAccuracy({ symbol, limit: accuracyLimit }),            staleTime: 60_000, refetchInterval: 120_000 })
  const volQ         = useQuery({ queryKey: ['volatility', symbol, volatilityLimit],        queryFn: () => fetchVolatility({ symbol, limit: volatilityLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const momQ         = useQuery({ queryKey: ['momentum', symbol, momentumLimit],            queryFn: () => fetchMomentum({ symbol, limit: momentumLimit }),                staleTime: 15_000, refetchInterval: REFETCH })
  const sentQ        = useQuery({ queryKey: ['sentTimeline', symbol, sentimentLimit],       queryFn: () => fetchSentimentTimeline({ symbol, limit: sentimentLimit }),      staleTime: 15_000, refetchInterval: REFETCH })
  const sentSumQ     = useQuery({ queryKey: ['sentSummary', symbol],                        queryFn: () => fetchSentimentSummary({ symbol }),                              staleTime: 15_000, refetchInterval: REFETCH })
  const returnsQ     = useQuery({ queryKey: ['returns', symbol, returnLimit],               queryFn: () => fetchReturns({ symbol, limit: returnLimit }),                   staleTime: 30_000, refetchInterval: 60_000  })
  const tableReturnsQ= useQuery({ queryKey: ['returns', symbol, tableReturnLimit],          queryFn: () => fetchReturns({ symbol, limit: tableReturnLimit }),              staleTime: 30_000, refetchInterval: 60_000  })
  const ofQ          = useQuery({ queryKey: ['orderflow', symbol, ofLimit],                 queryFn: () => fetchOrderFlow({ symbol, limit: ofLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const microQ       = useQuery({ queryKey: ['micro-ts', symbol, microLimit],               queryFn: () => fetchMicrostructure({ symbol, mode: 'timeseries', limit: microLimit }), staleTime: 15_000, refetchInterval: REFETCH })
  const heatmapQ     = useQuery({ queryKey: ['micro-hm', symbol],                           queryFn: () => fetchMicrostructure({ symbol, mode: 'heatmap' }),      staleTime: 300_000, refetchInterval: 300_000 })
  const sentSrcQ     = useQuery({ queryKey: ['sentBySource', symbol],                       queryFn: () => fetchSentimentBySource({ symbol }),                    staleTime: 60_000, refetchInterval: 120_000 })
  const lagQ         = useQuery({ queryKey: ['lagCorr', symbol, maxLag],                    queryFn: () => fetchSentimentCorrelation({ symbol, max_lag: maxLag }), staleTime: 60_000, refetchInterval: 120_000 })

  // ── Sentiment summary calculations ──
  const ss = sentSumQ.data
  const sentScore = ss?.latest_sentiment != null ? ss.latest_sentiment.toFixed(4) : '—'
  const sentConf  = ss?.avg_confidence   != null ? (ss.avg_confidence * 100).toFixed(1) + '%' : '—'
  const sentMsgs  = ss?.total_messages   != null ? ss.total_messages.toLocaleString() : '—'
  const sentVariant = ss?.latest_sentiment > 0 ? 'success' : ss?.latest_sentiment < 0 ? 'danger' : 'accent'

  // ── Direction indicator calculations ──
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

  // ── Accuracy KPIs calculations ──
  const acc = accuracyQ.data
  const mae              = acc?.mae              != null ? acc.mae.toFixed(4)                  : '—'
  const dirAcc           = acc?.directional_accuracy != null ? acc.directional_accuracy.toFixed(1) + '%' : '—'
  const totalPredictions = acc?.total_predictions    != null ? acc.total_predictions.toLocaleString() : '—'
  const matched          = acc?.matched_predictions  != null ? acc.matched_predictions.toLocaleString() : '—'

  // ── Rolling stats table calculations ──
  const returnMap = {}
  ;(tableReturnsQ.data || []).forEach(r => {
    returnMap[new Date(r.open_time).toISOString()] = r.log_return
  })
  const tableRows = (candlesQ.data || []).slice(0, 20).map(c => ({
    time:       format(new Date(c.open_time), 'MM/dd HH:mm'),
    open:       c.open,
    close:      c.close,
    volume:     c.volume,
    log_return: returnMap[new Date(c.open_time).toISOString()] ?? null,
  }))

  return (
    <div className="dashboard-page">
      {/* ── SECTION 1: Executive Summary ── */}
      <h2 className="section-title" style={{ marginTop: 0, marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Executive Summary</h2>
      <div className="kpi-strip">
        <KpiCard label="Sentiment Score" value={sentScore} sub={ss?.latest_time ? 'Latest: ' + format(new Date(ss.latest_time), 'HH:mm') : ''} variant={sentVariant} />
        <div className={`kpi-card ${dirVariant}`} style={{ flex: 2, flexDirection: 'row', alignItems: 'center', gap: 24, borderLeft: '1px solid var(--border)' }}>
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
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            <div className="kpi-label">Predicted Close</div>
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

      {/* ── SECTION 2: Price Action & Forecast ── */}
      <h2 className="section-title" style={{ marginTop: '2rem', marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Price Action & Forecast</h2>
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
        {(!candlesQ.data?.length || !forecastQ.data)
          ? <ChartPlaceholder loading={candlesQ.isLoading || forecastQ.isLoading} error={candlesQ.error} empty={!candlesQ.data?.length} />
          : <ForecastCandlestickChart candles={candlesQ.data} forecast={forecastQ.data} />
        }
      </ChartCard>

      <ChartCard
        title="Sentiment Timeline Overlay on Price"
        controls={<LimitInput value={sentimentLimit} onChange={setSentimentLimit} max={2000} />}
        minHeight={280}
      >
        {(!sentQ.data?.length || !candlesQ.data?.length)
          ? <ChartPlaceholder loading={sentQ.isLoading || candlesQ.isLoading} error={sentQ.error} empty={!sentQ.data?.length} />
          : <SentimentPriceChart candles={candlesQ.data} sentiment={sentQ.data} />
      }
      </ChartCard>

      {/* ── SECTION 3: Market Dynamics ── */}
      <h2 className="section-title" style={{ marginTop: '2rem', marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Market Dynamics</h2>
      <div className="grid-2">
        <ChartCard
          title="Volatility Ribbon + Ratio"
          controls={<LimitInput value={volatilityLimit} onChange={setVolatilityLimit} max={2000} />}
        >
          {!volQ.data?.length
            ? <ChartPlaceholder loading={volQ.isLoading} error={volQ.error} empty={!volQ.data?.length} />
            : <VolatilityRatioChart data={volQ.data} />
          }
        </ChartCard>
        
        <ChartCard
          title="Momentum Profile Scatter"
          controls={<LimitInput value={momentumLimit} onChange={setMomentumLimit} max={1000} />}
        >
          {!momQ.data?.length
            ? <ChartPlaceholder loading={momQ.isLoading} error={momQ.error} empty={!momQ.data?.length} />
            : <MomentumScatterChart data={momQ.data} />
          }
        </ChartCard>
      </div>

      <ChartCard
        title="Volume Spike Timeline"
        controls={<LimitInput value={momentumLimit} onChange={setMomentumLimit} max={1000} />}
        minHeight={250}
      >
        {!momQ.data?.length
            ? <ChartPlaceholder loading={momQ.isLoading} error={momQ.error} empty={!momQ.data?.length} />
          : <VolumeSpikeChart data={momQ.data} />
        }
      </ChartCard>

      {/* ── SECTION 4: Microstructure & Order Flow ── */}
      <h2 className="section-title" style={{ marginTop: '2rem', marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Microstructure & Order Flow</h2>
      <ChartCard
        title="Order Flow Imbalance"
        controls={<LimitInput value={ofLimit} onChange={setOfLimit} max={1000} />}
        minHeight={280}
      >
        {!ofQ.data?.length
            ? <ChartPlaceholder loading={ofQ.isLoading} error={ofQ.error} empty={!ofQ.data?.length} />
          : <OrderFlowChart data={ofQ.data} />
        }
      </ChartCard>

      <div className="grid-2">
        <ChartCard
          title="Candlestick Anatomy (Body Size · Price Range Ratio)"
          controls={<LimitInput value={microLimit} onChange={setMicroLimit} max={2000} />}
        >
          {!microQ.data?.length
            ? <ChartPlaceholder loading={microQ.isLoading} error={microQ.error} empty={!microQ.data?.length} />
            : <CandleAnatomyChart data={microQ.data} />
          }
        </ChartCard>

        <ChartCard
          title="Log Return Distribution"
          controls={<LimitInput value={returnLimit} onChange={setReturnLimit} max={5000} />}
        >
          {!returnsQ.data?.length
            ? <ChartPlaceholder loading={returnsQ.isLoading} error={returnsQ.error} empty={!returnsQ.data?.length} />
            : <ReturnHistogram data={returnsQ.data} />
          }
        </ChartCard>
      </div>

      <ChartCard title="Hourly × Weekday Return Heatmap" minHeight={220}>
        {!heatmapQ.data?.length
            ? <ChartPlaceholder loading={heatmapQ.isLoading} error={heatmapQ.error} empty={!heatmapQ.data?.length} />
          : <HeatmapChart data={heatmapQ.data} />
        }
      </ChartCard>

      {/* ── SECTION 5: Sentiment Deep Dive ── */}
      <h2 className="section-title" style={{ marginTop: '2rem', marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Sentiment Deep Dive</h2>
      <div className="kpi-strip">
        <KpiCard label="Avg Confidence"  value={sentConf}  sub="Weighted avg" variant="purple" />
        <KpiCard label="Message Count"   value={sentMsgs}  sub="Total messages in window" variant="accent" />
      </div>
      <div className="grid-2">
        <ChartCard title="Sentiment per Source">
          {!sentSrcQ.data?.length
            ? <ChartPlaceholder loading={sentSrcQ.isLoading} error={sentSrcQ.error} empty={!sentSrcQ.data?.length} />
            : <SentimentBySourceChart data={sentSrcQ.data} />
          }
        </ChartCard>

        <ChartCard
          title="Sentiment–Price Lag Correlation"
          controls={<LagInput value={maxLag} onChange={setMaxLag} />}
        >
          {!lagQ.data?.length
            ? <ChartPlaceholder loading={lagQ.isLoading} error={lagQ.error} empty={!lagQ.data?.length} />
            : <LagCorrelationChart data={lagQ.data} />
          }
        </ChartCard>
      </div>

      {/* ── SECTION 6: Model Evaluation & Raw Data ── */}
      <h2 className="section-title" style={{ marginTop: '2rem', marginBottom: '1rem', fontSize: '1.25rem', color: 'var(--text-sec)' }}>Model Evaluation & Raw Data</h2>
      <div className="kpi-strip">
        <KpiCard label="Total Predictions" value={totalPredictions} sub="All stored predictions" variant="accent" />
        <KpiCard label="Matched (w/ actual)" value={matched}         sub="Have actual_close"     variant="purple" />
        <KpiCard label="MAE"                 value={mae}             sub="Mean absolute error"   variant="warning" />
        <KpiCard label="Directional Acc."   value={dirAcc}          sub="% direction correct"   variant={acc?.directional_accuracy > 50 ? 'success' : 'danger'} />
      </div>

      <div className="grid-2">
        <ChartCard
          title="Prediction vs Actual Close"
          controls={<LimitInput value={forecastLimit} onChange={setForecastLimit} max={60} />}
          minHeight={300}
        >
          {!forecastQ.data?.length
            ? <ChartPlaceholder loading={forecastQ.isLoading} error={forecastQ.error} empty={!forecastQ.data?.length} />
            : <PredictionVsActualChart data={forecastQ.data} />
          }
        </ChartCard>

        <ChartCard
          title="Rolling Stats (Latest 20 Candles)"
          controls={<LimitInput value={tableReturnLimit} onChange={setTableReturnLimit} max={5000} label="Returns limit" />}
          minHeight={300}
        >
          {tableReturnsQ.isLoading || candlesQ.isLoading
            ? <ChartPlaceholder loading />
            : (
              <div style={{ overflowX: 'auto', maxHeight: 320, overflowY: 'auto' }}>
                <table className="data-table">
                  <thead>
                    <tr>
                      <th>Time</th>
                      <th>Open</th>
                      <th>Close</th>
                      <th>Volume</th>
                      <th>Log Return</th>
                    </tr>
                  </thead>
                  <tbody>
                    {tableRows.map((r, i) => (
                      <tr key={i}>
                        <td>{r.time}</td>
                        <td>{r.open?.toFixed(2)}</td>
                        <td className={r.close > r.open ? 'val-up' : 'val-down'}>{r.close?.toFixed(2)}</td>
                        <td>{r.volume?.toLocaleString(undefined, { maximumFractionDigits: 2 })}</td>
                        <td className={r.log_return == null ? 'val-dim' : r.log_return > 0 ? 'val-up' : 'val-down'}>
                          {r.log_return != null ? r.log_return.toFixed(6) : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          }
        </ChartCard>
      </div>
    </div>
  )
}

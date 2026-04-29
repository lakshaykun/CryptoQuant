import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useDashboard } from '../context/DashboardContext'
import { fetchCandles, fetchVolatility, fetchMomentum, fetchSentimentTimeline, fetchSentimentSummary, fetchReturns } from '../api/client'
import { ChartCard } from '../components/ChartCard'
import { KpiCard } from '../components/KpiCard'
import { ChartPlaceholder } from '../components/Spinner'
import { IntervalSelector, LimitInput } from '../components/Controls'
import { CandlestickChart } from '../charts/CandlestickChart'
import {
  VolatilityRibbonChart,
  VolumeSpikeChart,
  SentimentPriceChart,
} from '../charts/RechartsCharts'
import { format } from 'date-fns'

const REFETCH = 30_000

export default function DescriptivePage() {
  const { symbol, interval, setInterval } = useDashboard()
  const [candleLimit, setCandleLimit]       = useState(300)
  const [volatilityLimit, setVolatilityLimit] = useState(300)
  const [momentumLimit, setMomentumLimit]   = useState(300)
  const [sentimentLimit, setSentimentLimit] = useState(300)
  const [returnLimit, setReturnLimit]       = useState(500)

  // ── Queries ───────────────────────────────────────────────────────────
  const candlesQ  = useQuery({ queryKey: ['candles', symbol, interval, candleLimit],    queryFn: () => fetchCandles({ symbol, interval, limit: candleLimit }),       staleTime: 15_000, refetchInterval: REFETCH })
  const volQ      = useQuery({ queryKey: ['volatility', symbol, volatilityLimit],        queryFn: () => fetchVolatility({ symbol, limit: volatilityLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const momQ      = useQuery({ queryKey: ['momentum', symbol, momentumLimit],            queryFn: () => fetchMomentum({ symbol, limit: momentumLimit }),                staleTime: 15_000, refetchInterval: REFETCH })
  const sentQ     = useQuery({ queryKey: ['sentTimeline', symbol, sentimentLimit],       queryFn: () => fetchSentimentTimeline({ symbol, limit: sentimentLimit }),      staleTime: 15_000, refetchInterval: REFETCH })
  const sentSumQ  = useQuery({ queryKey: ['sentSummary', symbol],                        queryFn: () => fetchSentimentSummary({ symbol }),                              staleTime: 15_000, refetchInterval: REFETCH })
  const returnsQ  = useQuery({ queryKey: ['returns', symbol, returnLimit],               queryFn: () => fetchReturns({ symbol, limit: returnLimit }),                   staleTime: 30_000, refetchInterval: 60_000  })

  // ── Sentiment summary ─────────────────────────────────────────────────
  const ss = sentSumQ.data
  const sentScore = ss?.latest_sentiment != null ? ss.latest_sentiment.toFixed(4) : '—'
  const sentConf  = ss?.avg_confidence   != null ? (ss.avg_confidence * 100).toFixed(1) + '%' : '—'
  const sentMsgs  = ss?.total_messages   != null ? ss.total_messages.toLocaleString() : '—'
  const sentVariant = ss?.latest_sentiment > 0 ? 'success' : ss?.latest_sentiment < 0 ? 'danger' : 'accent'

  // ── Rolling stats table ───────────────────────────────────────────────
  const returnMap = {}
  ;(returnsQ.data || []).forEach(r => {
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
    <>
      {/* ── Sentiment KPI strip ─────────────────────────────────────── */}
      <div className="kpi-strip">
        <KpiCard label="Sentiment Score" value={sentScore} sub={ss?.latest_time ? 'Latest: ' + format(new Date(ss.latest_time), 'HH:mm') : ''} variant={sentVariant} />
        <KpiCard label="Avg Confidence"  value={sentConf}  sub="Weighted avg" variant="purple" />
        <KpiCard label="Message Count"   value={sentMsgs}  sub="Total messages in window" variant="accent" />
      </div>

      {/* ── Candlestick + MA + Volume ───────────────────────────────── */}
      <ChartCard
        title={`${symbol} — Candlestick · MA-5 · MA-20 · Volume`}
        controls={
          <>
            <IntervalSelector value={interval} onChange={setInterval} />
            <LimitInput value={candleLimit} onChange={setCandleLimit} max={1000} />
          </>
        }
        minHeight={420}
      >
        {candlesQ.isLoading || candlesQ.isError || !candlesQ.data?.length
          ? <ChartPlaceholder loading={candlesQ.isLoading} error={candlesQ.error} empty={!candlesQ.data?.length} />
          : <CandlestickChart candles={candlesQ.data} />
        }
      </ChartCard>

      {/* ── Volatility Ribbon ────────────────────────────────────────── */}
      <ChartCard
        title="Volatility Ribbon Over Time"
        controls={<LimitInput value={volatilityLimit} onChange={setVolatilityLimit} max={2000} />}
        minHeight={280}
      >
        {volQ.isLoading || volQ.isError || !volQ.data?.length
          ? <ChartPlaceholder loading={volQ.isLoading} error={volQ.error} empty={!volQ.data?.length} />
          : <VolatilityRibbonChart data={volQ.data} />
        }
      </ChartCard>

      {/* ── Volume Spike Timeline ────────────────────────────────────── */}
      <ChartCard
        title="Volume Spike Timeline"
        controls={<LimitInput value={momentumLimit} onChange={setMomentumLimit} max={1000} />}
        minHeight={250}
      >
        {momQ.isLoading || momQ.isError || !momQ.data?.length
          ? <ChartPlaceholder loading={momQ.isLoading} error={momQ.error} empty={!momQ.data?.length} />
          : <VolumeSpikeChart data={momQ.data} />
        }
      </ChartCard>

      {/* ── Sentiment + Price Overlay ────────────────────────────────── */}
      <ChartCard
        title="Sentiment Timeline Overlay on Price"
        controls={<LimitInput value={sentimentLimit} onChange={setSentimentLimit} max={2000} />}
        minHeight={280}
      >
        {(sentQ.isLoading || candlesQ.isLoading)
          ? <ChartPlaceholder loading />
          : sentQ.isError || !sentQ.data?.length
            ? <ChartPlaceholder error={sentQ.error} empty={!sentQ.data?.length} />
            : <SentimentPriceChart candles={candlesQ.data} sentiment={sentQ.data} />
        }
      </ChartCard>

      {/* ── Rolling Stats Table ──────────────────────────────────────── */}
      <ChartCard
        title="Rolling Stats (Latest 20 Candles)"
        controls={<LimitInput value={returnLimit} onChange={setReturnLimit} max={5000} label="Returns limit" />}
        minHeight={200}
      >
        {returnsQ.isLoading || candlesQ.isLoading
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
    </>
  )
}

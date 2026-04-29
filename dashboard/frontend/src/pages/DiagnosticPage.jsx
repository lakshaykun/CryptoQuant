import { useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { useDashboard } from '../context/DashboardContext'
import {
  fetchReturns, fetchVolatility, fetchOrderFlow, fetchMomentum,
  fetchMicrostructure, fetchSentimentBySource, fetchSentimentCorrelation
} from '../api/client'
import { ChartCard } from '../components/ChartCard'
import { ChartPlaceholder } from '../components/Spinner'
import { LimitInput, LagInput } from '../components/Controls'
import {
  ReturnHistogram, VolatilityRatioChart, OrderFlowChart,
  MomentumScatterChart, CandleAnatomyChart, SentimentBySourceChart, LagCorrelationChart
} from '../charts/RechartsCharts'
import { HeatmapChart } from '../charts/HeatmapChart'

const REFETCH = 30_000

export default function DiagnosticPage() {
  const { symbol } = useDashboard()
  const [returnLimit,  setReturnLimit]  = useState(1000)
  const [volLimit,     setVolLimit]     = useState(300)
  const [ofLimit,      setOfLimit]      = useState(200)
  const [momLimit,     setMomLimit]     = useState(300)
  const [microLimit,   setMicroLimit]   = useState(300)
  const [maxLag,       setMaxLag]       = useState(10)

  const returnsQ     = useQuery({ queryKey: ['returns', symbol, returnLimit],       queryFn: () => fetchReturns({ symbol, limit: returnLimit }),         staleTime: 30_000, refetchInterval: 60_000 })
  const volQ         = useQuery({ queryKey: ['volatility', symbol, volLimit],        queryFn: () => fetchVolatility({ symbol, limit: volLimit }),          staleTime: 15_000, refetchInterval: REFETCH })
  const ofQ          = useQuery({ queryKey: ['orderflow', symbol, ofLimit],          queryFn: () => fetchOrderFlow({ symbol, limit: ofLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const momQ         = useQuery({ queryKey: ['momentum', symbol, momLimit],          queryFn: () => fetchMomentum({ symbol, limit: momLimit }),            staleTime: 15_000, refetchInterval: REFETCH })
  const microQ       = useQuery({ queryKey: ['micro-ts', symbol, microLimit],        queryFn: () => fetchMicrostructure({ symbol, mode: 'timeseries', limit: microLimit }), staleTime: 15_000, refetchInterval: REFETCH })
  const heatmapQ     = useQuery({ queryKey: ['micro-hm', symbol],                   queryFn: () => fetchMicrostructure({ symbol, mode: 'heatmap' }),      staleTime: 300_000, refetchInterval: 300_000 })
  const sentSrcQ     = useQuery({ queryKey: ['sentBySource', symbol],               queryFn: () => fetchSentimentBySource({ symbol }),                    staleTime: 60_000, refetchInterval: 120_000 })
  const lagQ         = useQuery({ queryKey: ['lagCorr', symbol, maxLag],            queryFn: () => fetchSentimentCorrelation({ symbol, max_lag: maxLag }), staleTime: 60_000, refetchInterval: 120_000 })

  return (
    <>
      <div className="grid-2">
        {/* Log Return Histogram */}
        <ChartCard
          title="Log Return Distribution"
          controls={<LimitInput value={returnLimit} onChange={setReturnLimit} max={5000} />}
        >
          {returnsQ.isLoading || returnsQ.isError || !returnsQ.data?.length
            ? <ChartPlaceholder loading={returnsQ.isLoading} error={returnsQ.error} empty={!returnsQ.data?.length} />
            : <ReturnHistogram data={returnsQ.data} />
          }
        </ChartCard>

        {/* Volatility Ribbon + Ratio */}
        <ChartCard
          title="Volatility Ribbon + Ratio"
          controls={<LimitInput value={volLimit} onChange={setVolLimit} max={2000} />}
        >
          {volQ.isLoading || volQ.isError || !volQ.data?.length
            ? <ChartPlaceholder loading={volQ.isLoading} error={volQ.error} empty={!volQ.data?.length} />
            : <VolatilityRatioChart data={volQ.data} />
          }
        </ChartCard>
      </div>

      {/* Order Flow Imbalance */}
      <ChartCard
        title="Order Flow Imbalance"
        controls={<LimitInput value={ofLimit} onChange={setOfLimit} max={1000} />}
        minHeight={280}
      >
        {ofQ.isLoading || ofQ.isError || !ofQ.data?.length
          ? <ChartPlaceholder loading={ofQ.isLoading} error={ofQ.error} empty={!ofQ.data?.length} />
          : <OrderFlowChart data={ofQ.data} />
        }
      </ChartCard>

      <div className="grid-2">
        {/* Momentum Profile Scatter */}
        <ChartCard
          title="Momentum Profile Scatter"
          controls={<LimitInput value={momLimit} onChange={setMomLimit} max={1000} />}
        >
          {momQ.isLoading || momQ.isError || !momQ.data?.length
            ? <ChartPlaceholder loading={momQ.isLoading} error={momQ.error} empty={!momQ.data?.length} />
            : <MomentumScatterChart data={momQ.data} />
          }
        </ChartCard>

        {/* Candlestick Anatomy */}
        <ChartCard
          title="Candlestick Anatomy (Body Size · Price Range Ratio)"
          controls={<LimitInput value={microLimit} onChange={setMicroLimit} max={2000} />}
        >
          {microQ.isLoading || microQ.isError || !microQ.data?.length
            ? <ChartPlaceholder loading={microQ.isLoading} error={microQ.error} empty={!microQ.data?.length} />
            : <CandleAnatomyChart data={microQ.data} />
          }
        </ChartCard>
      </div>

      {/* Hourly × Weekday Heatmap */}
      <ChartCard title="Hourly × Weekday Return Heatmap" minHeight={220}>
        {heatmapQ.isLoading || heatmapQ.isError || !heatmapQ.data?.length
          ? <ChartPlaceholder loading={heatmapQ.isLoading} error={heatmapQ.error} empty={!heatmapQ.data?.length} />
          : <HeatmapChart data={heatmapQ.data} />
        }
      </ChartCard>

      <div className="grid-2">
        {/* Sentiment by Source */}
        <ChartCard title="Sentiment per Source">
          {sentSrcQ.isLoading || sentSrcQ.isError || !sentSrcQ.data?.length
            ? <ChartPlaceholder loading={sentSrcQ.isLoading} error={sentSrcQ.error} empty={!sentSrcQ.data?.length} />
            : <SentimentBySourceChart data={sentSrcQ.data} />
          }
        </ChartCard>

        {/* Sentiment-Price Lag Correlation */}
        <ChartCard
          title="Sentiment–Price Lag Correlation"
          controls={<LagInput value={maxLag} onChange={setMaxLag} />}
        >
          {lagQ.isLoading || lagQ.isError || !lagQ.data?.length
            ? <ChartPlaceholder loading={lagQ.isLoading} error={lagQ.error} empty={!lagQ.data?.length} />
            : <LagCorrelationChart data={lagQ.data} />
          }
        </ChartCard>
      </div>
    </>
  )
}

import { useEffect, useRef } from 'react'
import { createChart, ColorType, CrosshairMode, CandlestickSeries, LineSeries, HistogramSeries } from 'lightweight-charts'

const COLORS = {
  bg:        '#181d24',
  border:    '#252c38',
  text:      '#94a3b8',
  gridLine:  '#1d232c',
  upBar:     '#00e676',
  downBar:   '#ff4d6d',
  ma5:       '#ffea00',
  ma20:      '#1de9b6',
  volume:    '#00e5ff73',
  forecast:  '#00e5ff',
}

function buildChart(container) {
  return createChart(container, {
    layout: {
      background: { type: ColorType.Solid, color: COLORS.bg },
      textColor: COLORS.text,
      fontSize: 11,
      fontFamily: "'JetBrains Mono', monospace",
    },
    grid: {
      vertLines: { color: COLORS.gridLine },
      horzLines: { color: COLORS.gridLine },
    },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: { borderColor: COLORS.border },
    timeScale: { borderColor: COLORS.border, timeVisible: true, secondsVisible: false },
    handleScroll: true,
    handleScale: true,
  })
}

// ── Main OHLCV candlestick chart with MA overlays ─────────────────────────
export function CandlestickChart({ candles }) {
  const containerRef = useRef(null)
  const chartRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return
    const chart = buildChart(containerRef.current)
    chartRef.current = chart

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor:        COLORS.upBar,
      downColor:      COLORS.downBar,
      borderVisible:  false,
      wickUpColor:    COLORS.upBar,
      wickDownColor:  COLORS.downBar,
    })

    const ma5Series = chart.addSeries(LineSeries, {
      color:       COLORS.ma5,
      lineWidth:   1,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    const ma20Series = chart.addSeries(LineSeries, {
      color:       COLORS.ma20,
      lineWidth:   1,
      priceLineVisible: false,
      lastValueVisible: false,
    })

    // Volume pane — separate price scale
    const volSeries = chart.addSeries(HistogramSeries, {
      color:           COLORS.volume,
      priceFormat:     { type: 'volume' },
      priceScaleId:    'vol',
    })
    chart.priceScale('vol').applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    })

    if (candles?.length) {
      const sorted = [...candles].reverse()

      candleSeries.setData(sorted.map(c => ({
        time:  new Date(c.open_time).getTime() / 1000,
        open:  c.open, high: c.high, low: c.low, close: c.close,
      })))

      ma5Series.setData(sorted
        .filter(c => c.ma_5 != null)
        .map(c => ({ time: new Date(c.open_time).getTime() / 1000, value: c.ma_5 })))

      ma20Series.setData(sorted
        .filter(c => c.ma_20 != null)
        .map(c => ({ time: new Date(c.open_time).getTime() / 1000, value: c.ma_20 })))

      volSeries.setData(sorted.map(c => ({
        time:  new Date(c.open_time).getTime() / 1000,
        value: c.volume,
        color: c.close >= c.open ? '#00e67644' : '#ff4d6d44',
      })))

      chart.timeScale().fitContent()
    }

    const ro = new ResizeObserver(() => {
      if (containerRef.current) {
        chart.applyOptions({ width: containerRef.current.clientWidth })
      }
    })
    ro.observe(containerRef.current)

    return () => { ro.disconnect(); chart.remove() }
  }, [candles])

  return <div ref={containerRef} style={{ width: '100%', height: 380 }} />
}

// ── Forecast chart: candlestick history + predicted_close extension ────────
export function ForecastCandlestickChart({ candles, forecast }) {
  const containerRef = useRef(null)

  useEffect(() => {
    if (!containerRef.current) return
    const chart = buildChart(containerRef.current)

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: COLORS.upBar, downColor: COLORS.downBar,
      borderVisible: false, wickUpColor: COLORS.upBar, wickDownColor: COLORS.downBar,
    })

    const forecastLine = chart.addSeries(LineSeries, {
      color:           COLORS.forecast,
      lineWidth:       2,
      lineStyle:       2,   // dashed
      priceLineVisible: false,
      lastValueVisible: true,
    })

    if (candles?.length) {
      const sorted = [...candles].reverse()
      candleSeries.setData(sorted.map(c => ({
        time: new Date(c.open_time).getTime() / 1000,
        open: c.open, high: c.high, low: c.low, close: c.close,
      })))
    }

    if (forecast?.length) {
      const sorted = [...forecast].reverse()
      forecastLine.setData(sorted.map(f => ({
        time:  new Date(f.open_time).getTime() / 1000,
        value: f.predicted_close,
      })))
    }

    chart.timeScale().fitContent()

    const ro = new ResizeObserver(() => {
      if (containerRef.current) chart.applyOptions({ width: containerRef.current.clientWidth })
    })
    ro.observe(containerRef.current)
    return () => { ro.disconnect(); chart.remove() }
  }, [candles, forecast])

  return <div ref={containerRef} style={{ width: '100%', height: 380 }} />
}

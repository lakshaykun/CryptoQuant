import axios from 'axios'

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 15000,
})

// ── Symbols ──────────────────────────────────────────────────────────────────
export const fetchSymbols = () =>
  api.get('/symbols/').then(r => r.data)

// ── Candles ──────────────────────────────────────────────────────────────────
export const fetchCandles = ({ symbol, interval = '1h', limit = 300, from, to }) =>
  api.get(`/candles/${symbol}`, {
    params: { interval, limit, from, to },
  }).then(r => r.data)

// ── Features ─────────────────────────────────────────────────────────────────
export const fetchReturns = ({ symbol, limit = 1000, from, to }) =>
  api.get(`/features/${symbol}/returns`, { params: { limit, from, to } }).then(r => r.data)

export const fetchVolatility = ({ symbol, limit = 300, from, to }) =>
  api.get(`/features/${symbol}/volatility`, { params: { limit, from, to } }).then(r => r.data)

export const fetchOrderFlow = ({ symbol, limit = 200, from, to }) =>
  api.get(`/features/${symbol}/orderflow`, { params: { limit, from, to } }).then(r => r.data)

export const fetchMomentum = ({ symbol, limit = 200, from, to }) =>
  api.get(`/features/${symbol}/momentum`, { params: { limit, from, to } }).then(r => r.data)

export const fetchMicrostructure = ({ symbol, mode = 'timeseries', limit = 300, from, to }) =>
  api.get(`/features/${symbol}/microstructure`, { params: { mode, limit, from, to } }).then(r => r.data)

// ── Sentiment ─────────────────────────────────────────────────────────────────
export const fetchSentimentTimeline = ({ symbol, limit = 300, from, to }) =>
  api.get(`/sentiment/${symbol}/timeline`, { params: { limit, from, to } }).then(r => r.data)

export const fetchSentimentBySource = ({ symbol, from, to }) =>
  api.get(`/sentiment/${symbol}/by-source`, { params: { from, to } }).then(r => r.data)

export const fetchSentimentSummary = ({ symbol }) =>
  api.get(`/sentiment/${symbol}/summary`).then(r => r.data)

// ── Analytics ─────────────────────────────────────────────────────────────────
export const fetchSentimentCorrelation = ({ symbol, max_lag = 10 }) =>
  api.get(`/analytics/${symbol}/sentiment-correlation`, { params: { max_lag } }).then(r => r.data)

// ── Predictions ───────────────────────────────────────────────────────────────
export const fetchForecast = ({ symbol, limit = 30 }) =>
  api.get(`/predictions/${symbol}/forecast`, { params: { limit } }).then(r => r.data)

export const fetchAccuracy = ({ symbol, limit = 100 }) =>
  api.get(`/predictions/${symbol}/accuracy`, { params: { limit } }).then(r => r.data)

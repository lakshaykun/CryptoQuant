import React, { useState } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import {
  PieChart, Pie, Cell, BarChart, Bar, CartesianGrid, XAxis, YAxis, Tooltip as RechartsTooltip, Legend, ResponsiveContainer,
  ScatterChart, Scatter, ZAxis
} from 'recharts';
import { ChartCard } from '../components/ChartCard';
import { ChartPlaceholder } from '../components/Spinner';

const API_BASE = "http://localhost:8000/api/v1/portfolio";

const BarCrosshair = (props) => {
  const { x, y, width, height, top, bottom, left, right, background, stroke = "#94a3b8" } = props;
  if (x == null || y == null) return null;
  const cx = x + width / 2;
  const cy = y + height / 2;
  return (
    <g>
      <rect x={x} y={y} width={width} height={height} fill={background || '#1d232c'} opacity={0.5} pointerEvents="none" />
      <line x1={cx} y1={top} x2={cx} y2={bottom} stroke={stroke} strokeDasharray="3 3" strokeWidth={1} pointerEvents="none" />
      <line x1={left} y1={cy} x2={right} y2={cy} stroke={stroke} strokeDasharray="3 3" strokeWidth={1} pointerEvents="none" />
    </g>
  );
};

export default function PortfolioPage() {
  const queryClient = useQueryClient();
  const [selectedPfId, setSelectedPfId] = useState(null);
  const [newPfName, setNewPfName] = useState('');
  const [newPos, setNewPos] = useState({ symbol: '', quantity: '', buy_price: '' });

  const portfoliosQ = useQuery({
    queryKey: ['portfolios'],
    queryFn: async () => {
      const res = await fetch(`${API_BASE}/`);
      if (!res.ok) throw new Error("Failed to fetch portfolios");
      return res.json();
    }
  });

  const symbolsQ = useQuery({
    queryKey: ['symbols'],
    queryFn: async () => {
      const res = await fetch("http://localhost:8000/api/v1/symbols/");
      if (!res.ok) throw new Error("Failed to fetch symbols");
      return res.json();
    }
  });

  const pfId = selectedPfId || portfoliosQ.data?.[0]?.id;

  const createPortfolioM = useMutation({
    mutationFn: async (name) => {
      const res = await fetch(`${API_BASE}/`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name })
      });
      if (!res.ok) throw new Error("Failed to create portfolio");
      return res.json();
    },
    onSuccess: (data) => {
      queryClient.invalidateQueries(['portfolios']);
      setSelectedPfId(data.id);
      setNewPfName('');
    }
  });

  const addPositionM = useMutation({
    mutationFn: async (pos) => {
      const res = await fetch(`${API_BASE}/${pfId}/positions`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(pos)
      });
      if (!res.ok) throw new Error("Failed to add position");
      return res.json();
    },
    onSuccess: () => {
      queryClient.invalidateQueries(['portfolioDetail', pfId]);
      queryClient.invalidateQueries(['portfolioRisk', pfId]);
      queryClient.invalidateQueries(['portfolioScores', pfId]);
      queryClient.invalidateQueries(['portfolioSuggestions', pfId]);
      setNewPos({ symbol: '', quantity: '', buy_price: '' });
    }
  });

  // Queries using pfId
  const detailQ = useQuery({
    queryKey: ['portfolioDetail', pfId],
    queryFn: async () => (await fetch(`${API_BASE}/${pfId}`)).json(),
    enabled: !!pfId,
    refetchInterval: 30000
  });

  const riskQ = useQuery({
    queryKey: ['portfolioRisk', pfId],
    queryFn: async () => (await fetch(`${API_BASE}/${pfId}/risk`)).json(),
    enabled: !!pfId,
    refetchInterval: 30000
  });

  const scoresQ = useQuery({
    queryKey: ['portfolioScores', pfId],
    queryFn: async () => (await fetch(`${API_BASE}/${pfId}/scores`)).json(),
    enabled: !!pfId,
    refetchInterval: 30000
  });

  const suggestionsQ = useQuery({
    queryKey: ['portfolioSuggestions', pfId],
    queryFn: async () => (await fetch(`${API_BASE}/${pfId}/suggestions`)).json(),
    enabled: !!pfId,
    refetchInterval: 30000
  });

  if (portfoliosQ.isLoading) {
    return <div className="dashboard-page"><ChartPlaceholder loading /></div>;
  }

  // Define components for the inputs to keep styling robust
  const inputStyle = {
    padding: '8px 12px',
    background: 'var(--bg-card)',
    color: 'var(--text-main)',
    border: '1px solid var(--border)',
    borderRadius: '4px',
    fontFamily: 'inherit',
    fontSize: '0.9rem'
  };

  const btnStyle = {
    padding: '8px 16px',
    background: 'var(--accent)',
    color: '#fff',
    border: 'none',
    borderRadius: '4px',
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: '0.9rem',
    transition: 'opacity 0.2s'
  };

  if (!pfId) {
    return (
      <div className="dashboard-page" style={{ padding: '2rem' }}>
        <h2>No Portfolio Found</h2>
        <div style={{ display: 'flex', gap: '1rem', marginTop: '1rem' }}>
          <input 
            type="text" 
            placeholder="e.g. My Ledger" 
            value={newPfName} 
            onChange={e => setNewPfName(e.target.value)} 
            style={inputStyle} 
          />
          <button 
            onClick={() => newPfName && createPortfolioM.mutate(newPfName)} 
            style={btnStyle}
            disabled={createPortfolioM.isPending}>
            {createPortfolioM.isPending ? 'Creating...' : 'Create Portfolio'}
          </button>
        </div>
      </div>
    );
  }

  const isLoading = detailQ.isLoading || riskQ.isLoading || scoresQ.isLoading || suggestionsQ.isLoading;

  const positions = detailQ.data?.positions || [];
  const riskItems = riskQ.data?.positions || [];
  const scores = scoresQ.data || [];
  const suggestions = suggestionsQ.data || [];

  const allocationData = positions.map(p => ({
    name: p.symbol,
    value: p.current_value || 0
  })).filter(d => d.value > 0);

  const COLORS = ['#00e5ff', '#1de9b6', '#00e676', '#ffea00', '#ff9100', '#ff4d6d', '#b388ff', '#f8fafc'];

  const expectedAllocationData = suggestions.map(s => ({
    name: s.symbol,
    value: s.suggested_weight_pct || 0
  })).filter(d => d.value > 0);

  const riskBarData = riskItems.map(r => ({
    name: r.symbol,
    risk: r.weighted_risk || 0
  })).sort((a,b) => b.risk - a.risk);

  const scatterData = scores.filter(s => s.in_portfolio).map(s => {
    const pos = positions.find(p => p.symbol === s.symbol);
    return {
      symbol: s.symbol,
      volatility: s.volatility_5 || 0,
      return: s.predicted_log_return || 0,
      size: pos?.current_value || 0
    };
  });

  const scoreLeaders = [...scores].slice(0, 10).map(s => ({
    name: s.symbol,
    score: s.score || 0,
    in_portfolio: s.in_portfolio
  }));

  const getSentimentVariant = (si) => {
    if (!si) return 'gray';
    if (si > 1.05) return 'var(--success)';
    if (si < 0.95) return 'var(--danger)';
    return 'gray';
  }

  const sentMap = {};
  scores.forEach(s => { sentMap[s.symbol] = s.sentiment_index });

  return (
    <div className="dashboard-page">

      {/* Control Strip */}
      <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem', marginBottom: '2rem', padding: '1rem', background: 'var(--bg-card)', borderRadius: '8px', border: '1px solid var(--border)' }}>
        
        {/* Portfolio Selection & Creation */}
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
          <h3 style={{ margin: 0, fontSize: '1rem', color: 'var(--text-sec)' }}>Active Portfolio</h3>
          <select 
            value={pfId || ''} 
            onChange={e => setSelectedPfId(e.target.value)} 
            style={{ ...inputStyle, minWidth: '200px' }}
          >
            {portfoliosQ.data?.map(pf => <option key={pf.id} value={pf.id}>{pf.name}</option>)}
          </select>

          <span style={{ color: 'var(--border)' }}>|</span>

          <input 
            type="text" 
            placeholder="New Portfolio Name" 
            value={newPfName} 
            onChange={e => setNewPfName(e.target.value)} 
            style={inputStyle} 
          />
          <button 
            onClick={() => { if(newPfName) createPortfolioM.mutate(newPfName) }} 
            style={{ ...btnStyle, backgroundColor: 'transparent', border: '1px solid var(--accent)', color: 'var(--accent)' }}
          >
            Create New
          </button>
        </div>

        <hr style={{ borderTop: '1px solid var(--border)', borderBottom: 'none', margin: '0.5rem 0' }} />

        {/* Add Position Form */}
        <div style={{ display: 'flex', gap: '1rem', alignItems: 'center', flexWrap: 'wrap' }}>
          <h3 style={{ margin: 0, fontSize: '1rem', color: 'var(--text-sec)' }}>Add Position</h3>
          <select 
            value={newPos.symbol} 
            onChange={e => setNewPos({...newPos, symbol: e.target.value})} 
            style={{ ...inputStyle, width: '160px' }}
          >
            <option value="" disabled>Select Symbol...</option>
            {(symbolsQ.data || []).map(sym => (
              <option key={sym} value={sym}>{sym}</option>
            ))}
          </select>
          <input 
            type="number" 
            placeholder="Quantity" 
            value={newPos.quantity} 
            onChange={e => setNewPos({...newPos, quantity: e.target.value})} 
            style={{ ...inputStyle, width: '120px' }} 
            step="0.0001"
          />
          <input 
            type="number" 
            placeholder="Buy Price" 
            value={newPos.buy_price} 
            onChange={e => setNewPos({...newPos, buy_price: e.target.value})} 
            style={{ ...inputStyle, width: '120px' }} 
            step="0.01"
          />
          <button 
            onClick={() => addPositionM.mutate({ symbol: newPos.symbol, quantity: Number(newPos.quantity), buy_price: Number(newPos.buy_price) })} 
            style={{ ...btnStyle, background: 'var(--success)' }}
            disabled={!newPos.symbol || !newPos.quantity || !newPos.buy_price || addPositionM.isPending}
          >
            {addPositionM.isPending ? 'Adding...' : 'Add Holding'}
          </button>
        </div>

      </div>

      {/* Logical Storyboard Layout */}
      
      {/* 1. Allocation Comparison */}
      <div className="grid-2">
        <ChartCard title="Current Allocation" minHeight={280}>
          {isLoading ? <ChartPlaceholder loading /> :
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={allocationData} cx="50%" cy="50%" innerRadius={50} outerRadius={80} fill="#b388ff" paddingAngle={3} dataKey="value">
                  {allocationData.map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
                </Pie>
                <RechartsTooltip formatter={(value) => `$${value.toFixed(2)}`} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          }
        </ChartCard>

        <ChartCard title="Suggested Allocation (Model Optimized)" minHeight={280}>
          {isLoading ? <ChartPlaceholder loading /> :
            <ResponsiveContainer width="100%" height="100%">
              <PieChart>
                <Pie data={expectedAllocationData} cx="50%" cy="50%" innerRadius={50} outerRadius={80} fill="#1de9b6" paddingAngle={3} dataKey="value">
                  {expectedAllocationData.map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[(index+3) % COLORS.length]} />)}
                </Pie>
                <RechartsTooltip formatter={(value) => `${value.toFixed(2)}%`} />
                <Legend />
              </PieChart>
            </ResponsiveContainer>
          }
        </ChartCard>
      </div>

      {/* 2. Holdings & Rebalance Tables */}
      <div className="grid-2">
        <ChartCard title="Current Holdings" minHeight={300}>
          {isLoading ? <ChartPlaceholder loading /> :
            <div style={{ overflowX: 'auto', maxHeight: 280, overflowY: 'auto' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Qty</th>
                    <th>Price</th>
                    <th>P&L %</th>
                    <th>Value</th>
                    <th>Sentiment</th>
                  </tr>
                </thead>
                <tbody>
                  {positions.map(p => {
                     const si = sentMap[p.symbol];
                     const sCol = getSentimentVariant(si);
                     return (
                      <tr key={p.symbol}>
                        <td style={{ fontWeight: 600 }}>{p.symbol}</td>
                        <td>{p.quantity?.toFixed(2)}</td>
                        <td>${p.buy_price?.toFixed(2)}</td>
                        <td className={p.pnl_pct > 0 ? 'val-up' : p.pnl_pct < 0 ? 'val-down' : ''}>
                          {p.pnl_pct ? `${p.pnl_pct.toFixed(2)}%` : '—'}
                        </td>
                        <td>{p.current_value ? `$${p.current_value.toFixed(2)}` : '—'}</td>
                        <td>
                          <span style={{ 
                            backgroundColor: sCol, color: '#fff', padding: '2px 6px', borderRadius: '4px', fontSize: '0.8rem', opacity: sCol === 'gray' ? 0.5 : 1
                          }}>
                            {si ? si.toFixed(2) : '—'}
                          </span>
                        </td>
                      </tr>
                     )
                  })}
                </tbody>
              </table>
            </div>
          }
        </ChartCard>

        <ChartCard title="Rebalance Suggestions" minHeight={300}>
          {isLoading ? <ChartPlaceholder loading /> :
            <div style={{ overflowX: 'auto', maxHeight: 280, overflowY: 'auto' }}>
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Action</th>
                    <th>Current %</th>
                    <th>Target %</th>
                  </tr>
                </thead>
                <tbody>
                  {suggestions.map(s => {
                     let acClass = '';
                     if (s.action === 'buy' || s.action === 'increase') acClass = 'val-up';
                     if (s.action === 'reduce') acClass = 'val-down';
                     if (s.action === 'hold') acClass = 'val-dim';
                     return (
                      <tr key={s.symbol}>
                        <td style={{ fontWeight: 600 }}>{s.symbol}</td>
                        <td className={acClass} style={{ textTransform: 'uppercase', fontWeight: 'bold' }}>{s.action}</td>
                        <td>{s.current_weight_pct?.toFixed(2)}%</td>
                        <td>{s.suggested_weight_pct?.toFixed(2)}%</td>
                      </tr>
                     )
                  })}
                </tbody>
              </table>
            </div>
          }
        </ChartCard>
      </div>

      {/* 3. Risk Profile */}
      <div className="grid-2">
        <ChartCard title="Risk Exposure (Weighted Volatility)" minHeight={300}>
           {isLoading ? <ChartPlaceholder loading /> :
             <ResponsiveContainer width="100%" height="100%">
               <BarChart data={riskBarData} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                 <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                 <XAxis dataKey="name" stroke="var(--text-sec)" />
                 <YAxis stroke="var(--text-sec)" />
                 <RechartsTooltip cursor={<BarCrosshair background="rgba(255,255,255,0.05)" />} />
                 <Bar dataKey="risk" fill="#ff9100" radius={[4,4,0,0]} />
               </BarChart>
             </ResponsiveContainer>
           }
        </ChartCard>

        <ChartCard title="Risk vs Return Tradeoff" minHeight={300}>
          {isLoading ? <ChartPlaceholder loading /> :
            <ResponsiveContainer width="100%" height="100%">
              <ScatterChart margin={{ top: 20, right: 20, bottom: 20, left: 20 }}>
                <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
                <XAxis type="number" dataKey="volatility" name="Volatility" stroke="var(--text-sec)" tickFormatter={v => v.toFixed(3)} />
                <YAxis type="number" dataKey="return" name="Log Return" stroke="var(--text-sec)" tickFormatter={v => v.toFixed(4)} />
                <ZAxis type="number" dataKey="size" range={[50, 400]} />
                <RechartsTooltip cursor={{strokeDasharray: '3 3'}} formatter={(val) => val.toFixed(4)} />
                <Scatter name="Positions" data={scatterData} fill="#b388ff">
                  {scatterData.map((entry, index) => <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />)}
                </Scatter>
              </ScatterChart>
            </ResponsiveContainer>
          }
        </ChartCard>
      </div>

      {/* 4. Global Score Leaderboard */}
      <ChartCard title="Risk-Adjusted Score Global Leaderboard" minHeight={300}>
         {isLoading ? <ChartPlaceholder loading /> :
           <ResponsiveContainer width="100%" height="100%">
             <BarChart data={scoreLeaders} layout="vertical" margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
               <CartesianGrid strokeDasharray="3 3" opacity={0.1} />
               <XAxis type="number" stroke="var(--text-sec)" />
               <YAxis dataKey="name" type="category" stroke="var(--text-sec)" />
               <RechartsTooltip cursor={<BarCrosshair background="rgba(255,255,255,0.05)" />} />
               <Bar dataKey="score" layout="vertical" radius={[0,4,4,0]}>
                 {scoreLeaders.map((entry, index) => (
                   <Cell key={`cell-${index}`} fill={entry.in_portfolio ? '#1de9b6' : '#00e5ff'} opacity={entry.in_portfolio ? 1 : 0.6} />
                 ))}
               </Bar>
             </BarChart>
           </ResponsiveContainer>
         }
      </ChartCard>
    </div>
  );
}

import { Routes, Route, Navigate } from 'react-router-dom'
import { DashboardProvider } from './context/DashboardContext'
import { SideNav } from './components/SideNav'
import { SymbolSelector } from './components/SymbolSelector'
import DashboardPage    from './pages/DashboardPage'
import PortfolioPage    from './pages/PortfolioPage'

export default function App() {
  return (
    <DashboardProvider>
      <div className="layout">
        <SideNav />
        <div className="main-content">
          <header className="topbar">
            <span className="topbar-title">Analytics Dashboard</span>
            <span className="live-dot">LIVE</span>
            <SymbolSelector />
          </header>
          <main className="page-body">
            <Routes>
              <Route path="/" element={<Navigate to="/dashboard" replace />} />
              <Route path="/dashboard"  element={<DashboardPage />} />
              <Route path="/portfolio"  element={<PortfolioPage />} />
            </Routes>
          </main>
        </div>
      </div>
    </DashboardProvider>
  )
}

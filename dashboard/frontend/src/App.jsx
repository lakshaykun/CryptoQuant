import { Routes, Route, Navigate } from 'react-router-dom'
import { DashboardProvider } from './context/DashboardContext'
import { SideNav } from './components/SideNav'
import { SymbolSelector } from './components/SymbolSelector'
import DescriptivePage  from './pages/DescriptivePage'
import DiagnosticPage   from './pages/DiagnosticPage'
import PredictivePage   from './pages/PredictivePage'
import PrescriptivePage from './pages/PrescriptivePage'

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
              <Route path="/" element={<Navigate to="/descriptive" replace />} />
              <Route path="/descriptive"  element={<DescriptivePage />} />
              <Route path="/diagnostic"   element={<DiagnosticPage />} />
              <Route path="/predictive"   element={<PredictivePage />} />
              <Route path="/prescriptive" element={<PrescriptivePage />} />
            </Routes>
          </main>
        </div>
      </div>
    </DashboardProvider>
  )
}

import { NavLink } from 'react-router-dom'

const links = [
  { to: '/dashboard', icon: '📊', label: 'Market Overview' },
  { to: '/portfolio', icon: '💼', label: 'Portfolio' },
]

export function SideNav() {
  return (
    <nav className="sidenav">
      <div className="sidenav-logo" style={{ flexDirection: 'column', alignItems: 'flex-start', gap: '2px', padding: '24px 20px', borderBottom: '1px solid var(--border-dim)' }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
          <div className="logo-icon">₿</div>
          <div style={{ fontSize: '18px', fontWeight: 800, letterSpacing: '0.05em', textTransform: 'uppercase' }}>CryptoQuant</div>
        </div>
        <div style={{ fontSize: '10px', color: 'var(--accent)', fontWeight: 600, letterSpacing: '0.1em', marginTop: '2px' }}>MARKET INTEL</div>
      </div>
      <div className="sidenav-links">
        {links.map(({ to, icon, label, badge }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) => `sidenav-link${isActive ? ' active' : ''}`}
          >
            <span className="link-icon">{icon}</span>
            {label}
          </NavLink>
        ))}
      </div>
    </nav>
  )
}

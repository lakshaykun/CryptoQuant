import { NavLink } from 'react-router-dom'

const links = [
  { to: '/dashboard', icon: '📊', label: 'Market Overview' },
  { to: '/portfolio', icon: '💼', label: 'Portfolio', badge: 'Soon' },
]

export function SideNav() {
  return (
    <nav className="sidenav">
      <div className="sidenav-logo">
        <div className="logo-icon">₿</div>
        CryptoQuant
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
            {badge && <span className="sidenav-badge">{badge}</span>}
          </NavLink>
        ))}
      </div>
    </nav>
  )
}

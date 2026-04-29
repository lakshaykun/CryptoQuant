import { NavLink } from 'react-router-dom'

const links = [
  { to: '/descriptive',  icon: '📊', label: 'Descriptive'  },
  { to: '/diagnostic',   icon: '🔍', label: 'Diagnostic'   },
  { to: '/predictive',   icon: '🔮', label: 'Predictive'   },
  { to: '/prescriptive', icon: '💡', label: 'Prescriptive', badge: 'Soon' },
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

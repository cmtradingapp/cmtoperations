import { NavLink } from 'react-router-dom';

/**
 * Mobile-only bottom navigation bar with 4 primary destinations.
 * Visible only on screens < 768px (hidden via Tailwind `md:hidden`).
 * Matches the sidebar's dark navy aesthetic.
 */

const NAV_ITEMS = [
  {
    to: '/retention/performance',
    label: 'Dashboard',
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 12l2-2m0 0l7-7 7 7M5 10v10a1 1 0 001 1h3m10-11l2 2m-2-2v10a1 1 0 01-1 1h-3m-6 0a1 1 0 001-1v-4a1 1 0 011-1h2a1 1 0 011 1v4a1 1 0 001 1m-6 0h6" />
      </svg>
    ),
  },
  {
    to: '/retention',
    label: 'Grid',
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M4 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2V6zM14 6a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2V6zM4 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2H6a2 2 0 01-2-2v-2zM14 16a2 2 0 012-2h2a2 2 0 012 2v2a2 2 0 01-2 2h-2a2 2 0 01-2-2v-2z" />
      </svg>
    ),
  },
  {
    to: '/retention/dial',
    label: 'Calls',
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M3 5a2 2 0 012-2h3.28a1 1 0 01.948.684l1.498 4.493a1 1 0 01-.502 1.21l-2.257 1.13a11.042 11.042 0 005.516 5.516l1.13-2.257a1 1 0 011.21-.502l4.493 1.498a1 1 0 01.684.949V19a2 2 0 01-2 2h-1C9.716 21 3 14.284 3 6V5z" />
      </svg>
    ),
  },
  {
    to: '/retention/performance',
    label: 'Performance',
    icon: (
      <svg className="w-5 h-5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={1.8}>
        <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
      </svg>
    ),
  },
] as const;

export function BottomNav() {
  return (
    <nav className="block md:hidden fixed bottom-0 left-0 right-0 z-40 border-t border-[#1C2535]" style={{ background: '#080B12' }}>
      <div className="flex items-stretch justify-around h-14">
        {NAV_ITEMS.map((item) => (
          <NavLink
            key={item.to + item.label}
            to={item.to}
            className={({ isActive }) =>
              `flex flex-col items-center justify-center flex-1 min-h-[44px] min-w-[44px] transition-colors ${
                isActive
                  ? 'text-[#F2C94C]'
                  : 'text-[#4a6080] hover:text-[#6b7fa0]'
              }`
            }
          >
            {item.icon}
            <span className="text-[10px] mt-0.5 font-medium">{item.label}</span>
          </NavLink>
        ))}
      </div>
      {/* Safe area padding for devices with bottom bars (notch phones) */}
      <div className="h-[env(safe-area-inset-bottom)]" style={{ background: '#080B12' }} />
    </nav>
  );
}

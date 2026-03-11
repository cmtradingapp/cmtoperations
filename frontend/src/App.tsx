import type { ReactNode } from 'react';
import { useEffect, useRef, useState } from 'react';
import { NavLink, Navigate, Route, Routes, useLocation, useNavigate } from 'react-router-dom';

import { useAuthStore } from './store/useAuthStore';
import { useThemeStore } from './store/useThemeStore';
import { SidebarProvider, useSidebar } from './store/SidebarContext';
import { fetchMe } from './api/auth';
import { BottomNav } from './components/BottomNav';

import { LoginPage } from './pages/LoginPage';
import { ForgotPasswordPage } from './pages/ForgotPasswordPage';
import { ResetPasswordPage } from './pages/ResetPasswordPage';
import { UsersPage } from './pages/admin/UsersPage';
import { RolesPage } from './pages/admin/RolesPage';
import { PermissionsPage } from './pages/admin/PermissionsPage';
import { IntegrationsPage } from './pages/admin/IntegrationsPage';
import { CallManagerPage } from './pages/CallManagerPage';
import { CallHistoryPage } from './pages/CallHistoryPage';
import { AiCallDashboardPage } from './pages/AiCallDashboardPage';
import { BatchCallPage } from './pages/BatchCallPage';
import { ElenaAiUploadPage } from './pages/ElenaAiUploadPage';
import { ChallengesPage } from './pages/ChallengesPage';
import { ActionBonusesPage } from './pages/admin/ActionBonusesPage';

// ---------------------------------------------------------------------------
// Nav config
// ---------------------------------------------------------------------------

interface NavSection {
  title: string;
  items: { to: string; label: string; permission?: string }[];
}

const NAV_SECTIONS: NavSection[] = [
  {
    title: 'Admin',
    items: [
      { to: '/admin/users', label: 'Users', permission: 'users' },
      { to: '/admin/roles', label: 'Roles', permission: 'roles' },
      { to: '/admin/permissions', label: 'Permissions', permission: 'permissions' },
      { to: '/admin/integrations', label: 'Integrations & Config', permission: 'integrations' },
    ],
  },
  {
    title: 'Marketing',
    items: [
      { to: '/admin/challenges', label: 'Challenges', permission: 'challenges' },
      { to: '/admin/action-bonuses', label: 'Automatic Bonus', permission: 'action-bonuses' },
    ],
  },
  {
    title: 'AI Calls',
    items: [
      { to: '/call-manager', label: 'Call Manager', permission: 'call-manager' },
      { to: '/call-history', label: 'Call History', permission: 'call-history' },
      { to: '/call-dashboard', label: 'AI Call Dashboard', permission: 'call-dashboard' },
      { to: '/batch-call', label: 'Batch Call from File', permission: 'batch-call' },
      { to: '/elena-ai/upload-campaign', label: 'Upload to Campaign', permission: 'elena-ai-upload' },
    ],
  },
];

const ROUTES: { path: string; title: string; element: ReactNode; permission?: string }[] = [
  { path: '/admin/users', title: 'Users', element: <UsersPage /> },
  { path: '/admin/roles', title: 'Roles', element: <RolesPage /> },
  { path: '/admin/permissions', title: 'Permissions', element: <PermissionsPage /> },
  { path: '/admin/integrations', title: 'Integrations & Config', element: <IntegrationsPage /> },
  { path: '/admin/challenges', title: 'Challenges', element: <ChallengesPage /> },
  { path: '/admin/action-bonuses', title: 'Automatic Bonus', element: <ActionBonusesPage /> },
  { path: '/call-manager', title: 'Call Manager', element: <CallManagerPage />, permission: 'call-manager' },
  { path: '/call-history', title: 'Call History', element: <CallHistoryPage />, permission: 'call-history' },
  { path: '/call-dashboard', title: 'AI Call Dashboard', element: <AiCallDashboardPage />, permission: 'call-dashboard' },
  { path: '/batch-call', title: 'Batch Call from File', element: <BatchCallPage />, permission: 'batch-call' },
  { path: '/elena-ai/upload-campaign', title: 'Upload to Campaign', element: <ElenaAiUploadPage />, permission: 'elena-ai-upload' },
];

// ---------------------------------------------------------------------------
// Section collapse helpers
// ---------------------------------------------------------------------------

const SECTIONS_STORAGE_KEY = 'sidebar_sections_collapsed';

function loadSectionCollapsedState(): Record<string, boolean> {
  try {
    const raw = localStorage.getItem(SECTIONS_STORAGE_KEY);
    if (raw) return JSON.parse(raw) as Record<string, boolean>;
  } catch { /* ignore */ }
  return {};
}

function saveSectionCollapsedState(state: Record<string, boolean>): void {
  localStorage.setItem(SECTIONS_STORAGE_KEY, JSON.stringify(state));
}

// ---------------------------------------------------------------------------
// Brand palette
// ---------------------------------------------------------------------------

const C_DARK = {
  base: '#080B12',
  panel: '#0D1118',
  border: '#1C2535',
  borderBright: '#253045',
  yellow: '#F2C94C',
  blue: '#2D7EFF',
  green: '#27AE60',
  red: '#EB5757',
  textMuted: '#4a6080',
  textDim: '#6b7fa0',
  textLight: '#c8d4e8',
  white: '#F0F4FF',
};

const C_LIGHT = {
  base: '#FFFFFF',
  panel: '#F9FAFB',
  border: '#E5E7EB',
  borderBright: '#D1D5DB',
  yellow: '#B45309',
  blue: '#2563EB',
  green: '#15803D',
  red: '#DC2626',
  textMuted: '#9CA3AF',
  textDim: '#4B5563',
  textLight: '#111827',
  white: '#111827',
};

// ---------------------------------------------------------------------------
// Logo icon
// ---------------------------------------------------------------------------

function LogoIcon({ size = 32 }: { size?: number }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <defs>
        <linearGradient id="chevron-grad-1" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#4a9fff" />
          <stop offset="100%" stopColor="#2D7EFF" />
        </linearGradient>
        <linearGradient id="chevron-grad-2" x1="0" y1="0" x2="1" y2="1">
          <stop offset="0%" stopColor="#2D7EFF" />
          <stop offset="100%" stopColor="#1a4fa0" />
        </linearGradient>
      </defs>
      <path d="M6 8 L16 16 L6 24" stroke="url(#chevron-grad-2)" strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round" opacity="0.55" />
      <path d="M13 8 L23 16 L13 24" stroke="url(#chevron-grad-1)" strokeWidth="3.5" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

// ---------------------------------------------------------------------------
// Global sidebar styles
// ---------------------------------------------------------------------------

const SIDEBAR_STYLE_BASE = `
@keyframes pulse-live {
  0%, 100% { opacity: 1; transform: scale(1); }
  50% { opacity: 0.4; transform: scale(0.85); }
}
.sidebar-live-dot {
  animation: pulse-live 2s ease-in-out infinite;
}
.sidebar-scroll::-webkit-scrollbar {
  width: 4px;
}
.sidebar-scroll::-webkit-scrollbar-track {
  background: transparent;
}
.sidebar-scroll::-webkit-scrollbar-thumb {
  background: var(--sidebar-scrollbar);
  border-radius: 2px;
}
.sidebar-scroll::-webkit-scrollbar-thumb:hover {
  background: var(--sidebar-scrollbar-hover);
}
`;

// ---------------------------------------------------------------------------
// Protected layout inner
// ---------------------------------------------------------------------------

function ProtectedLayoutInner() {
  const { token, role, permissions, logout, setAuth } = useAuthStore();
  const username = useAuthStore((s) => s.username);
  const { isDark, toggle: toggleTheme } = useThemeStore();
  const C = isDark ? C_DARK : C_LIGHT;
  const navigate = useNavigate();
  const location = useLocation();
  const sidebar = useSidebar();
  const collapsed = sidebar.isCollapsed;
  const [sectionCollapsed, setSectionCollapsed] = useState<Record<string, boolean>>(loadSectionCollapsedState);
  const refreshedRef = useRef(false);

  useEffect(() => {
    if (!token || refreshedRef.current) return;
    refreshedRef.current = true;
    fetchMe(token)
      .then((me) => {
        setAuth(token, me.username, me.role, me.permissions);
      })
      .catch(() => {
        logout();
        navigate('/login', { replace: true });
      });
  }, [token]); // eslint-disable-line react-hooks/exhaustive-deps

  // Periodically refresh permissions
  useEffect(() => {
    if (!token) return;
    const id = setInterval(() => {
      fetchMe(token)
        .then((me) => { setAuth(token, me.username, me.role, me.permissions); })
        .catch(() => { /* non-fatal */ });
    }, 5 * 60 * 1000);
    return () => clearInterval(id);
  }, [token]); // eslint-disable-line react-hooks/exhaustive-deps

  // Close mobile sidebar on navigation
  useEffect(() => {
    if (sidebar.isMobile) sidebar.closeSidebar();
  }, [location.pathname]); // eslint-disable-line react-hooks/exhaustive-deps

  // Auto-expand active section
  useEffect(() => {
    const currentPath = location.pathname;
    setSectionCollapsed((prev) => {
      const updated = { ...prev };
      let changed = false;
      NAV_SECTIONS.forEach((section) => {
        const hasActive = section.items.some(
          (item) => currentPath === item.to || currentPath.startsWith(item.to + '/')
        );
        if (hasActive && updated[section.title]) {
          updated[section.title] = false;
          changed = true;
        }
      });
      if (changed) {
        saveSectionCollapsedState(updated);
        return updated;
      }
      return prev;
    });
  }, [location.pathname]);

  const toggleSidebar = () => sidebar.toggleSidebar();

  const toggleSection = (title: string) => {
    setSectionCollapsed((prev) => {
      const updated = { ...prev, [title]: !prev[title] };
      saveSectionCollapsedState(updated);
      return updated;
    });
  };

  if (!token) return <Navigate to="/login" replace />;

  const isAdmin = role === 'admin';

  const visibleSections = NAV_SECTIONS.map((section) => ({
    ...section,
    items: section.items.filter((item) =>
      isAdmin || !item.permission || permissions.includes(item.permission)
    ),
  })).filter((section) => section.items.length > 0);

  const collapsibleSections = visibleSections.filter((s) => s.items.length >= 2);
  const showExpandCollapseButtons = collapsibleSections.length >= 2;

  const expandAll = () => {
    const updated: Record<string, boolean> = { ...sectionCollapsed };
    collapsibleSections.forEach((s) => { updated[s.title] = false; });
    saveSectionCollapsedState(updated);
    setSectionCollapsed(updated);
  };

  const collapseAll = () => {
    const updated: Record<string, boolean> = { ...sectionCollapsed };
    collapsibleSections.forEach((s) => { updated[s.title] = true; });
    saveSectionCollapsedState(updated);
    setSectionCollapsed(updated);
  };

  const handleLogout = () => {
    logout();
    navigate('/login', { replace: true });
  };

  const initials = username
    ? username.split(/[\s._-]+/).slice(0, 2).map((p) => p[0]?.toUpperCase() ?? '').join('')
    : '?';

  const roleBadgeLabel = role ? role.toUpperCase() : '';
  const sidebarWidth = sidebar.sidebarWidth;

  return (
    <div className="flex flex-col h-screen overflow-hidden" style={{ background: isDark ? '#0a0d14' : '#f1f5f9' }}>
      <style>{SIDEBAR_STYLE_BASE}</style>

      <div className="flex flex-1 overflow-hidden">
        {/* Mobile backdrop */}
        {sidebar.isMobile && sidebar.isOpen && (
          <div
            className="fixed inset-0 z-40 bg-black/50 transition-opacity"
            onClick={() => sidebar.closeSidebar()}
            aria-hidden="true"
          />
        )}

        {/* Sidebar */}
        <aside
          style={{
            width: sidebar.isMobile ? 280 : sidebarWidth,
            minWidth: sidebar.isMobile ? 280 : sidebarWidth,
            background: C.base,
            borderRight: `1px solid ${C.border}`,
            transition: 'width 0.25s cubic-bezier(0.4,0,0.2,1), min-width 0.25s cubic-bezier(0.4,0,0.2,1), transform 0.25s cubic-bezier(0.4,0,0.2,1)',
            display: 'flex',
            flexDirection: 'column',
            flexShrink: 0,
            overflow: 'hidden',
            ...(sidebar.isMobile ? {
              position: 'fixed',
              top: 0,
              left: 0,
              bottom: 0,
              zIndex: 50,
              transform: sidebar.isOpen ? 'translateX(0)' : 'translateX(-100%)',
            } : {
              position: 'relative',
            }),
            ['--sidebar-scrollbar' as string]: C.border,
            ['--sidebar-scrollbar-hover' as string]: C.borderBright,
            ['--sidebar-blue' as string]: C.blue,
          }}
        >
          {/* Grid texture overlay */}
          <div
            aria-hidden="true"
            style={{
              position: 'absolute',
              inset: 0,
              pointerEvents: 'none',
              backgroundImage: isDark
                ? 'linear-gradient(rgba(45,126,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(45,126,255,0.03) 1px, transparent 1px)'
                : 'linear-gradient(rgba(0,0,0,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(0,0,0,0.03) 1px, transparent 1px)',
              backgroundSize: '24px 24px',
              zIndex: 0,
            }}
          />
          {/* Top gradient fade */}
          <div
            aria-hidden="true"
            style={{
              position: 'absolute',
              top: 0, left: 0, right: 0,
              height: 120,
              background: 'linear-gradient(180deg, rgba(45,126,255,0.04) 0%, transparent 60%)',
              pointerEvents: 'none',
              zIndex: 0,
            }}
          />

          <div style={{ position: 'relative', zIndex: 1, display: 'flex', flexDirection: 'column', flex: 1, overflow: 'hidden' }}>

            {/* Logo bar */}
            <div
              style={{
                padding: collapsed ? '14px 0' : '14px 14px 14px 16px',
                borderBottom: `1px solid ${C.border}`,
                display: 'flex',
                alignItems: 'center',
                gap: 10,
                flexShrink: 0,
                justifyContent: collapsed ? 'center' : 'space-between',
              }}
            >
              <div style={{ display: 'flex', alignItems: 'center', gap: 10, minWidth: 0 }}>
                <div style={{ flexShrink: 0 }}>
                  <LogoIcon size={28} />
                </div>
                {!collapsed && (
                  <div style={{ minWidth: 0 }}>
                    <div style={{ fontFamily: 'Syne, sans-serif', fontWeight: 700, fontSize: 15, color: C.white, whiteSpace: 'nowrap', letterSpacing: '-0.01em' }}>
                      CMTrading
                    </div>
                    <div style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.textMuted, whiteSpace: 'nowrap', letterSpacing: '0.04em' }}>
                      Operations Portal
                    </div>
                  </div>
                )}
              </div>

              {/* Mobile close */}
              {sidebar.isMobile && (
                <button
                  onClick={() => sidebar.closeSidebar()}
                  title="Close menu"
                  style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.textMuted, padding: '4px 6px', borderRadius: 4, lineHeight: 1, fontSize: 18, display: 'flex', alignItems: 'center', transition: 'color 0.15s', flexShrink: 0 }}
                  onMouseEnter={(e) => (e.currentTarget.style.color = C.white)}
                  onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                  aria-label="Close navigation menu"
                >
                  &#10005;
                </button>
              )}

              {/* LIVE + collapse (desktop expanded) */}
              {!sidebar.isMobile && !collapsed && (
                <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexShrink: 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
                    <span className="sidebar-live-dot" style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', background: C.green, boxShadow: `0 0 6px ${C.green}` }} />
                    <span style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.green, letterSpacing: '0.08em' }}>LIVE</span>
                  </div>
                  <button
                    onClick={toggleSidebar}
                    title="Collapse menu"
                    style={{ background: 'none', border: 'none', cursor: 'pointer', color: C.textMuted, padding: '2px 4px', borderRadius: 4, lineHeight: 1, fontSize: 16, display: 'flex', alignItems: 'center', transition: 'color 0.15s' }}
                    onMouseEnter={(e) => (e.currentTarget.style.color = C.white)}
                    onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                  >
                    &#8249;
                  </button>
                </div>
              )}

              {/* Expand (desktop collapsed) */}
              {!sidebar.isMobile && collapsed && (
                <button
                  onClick={toggleSidebar}
                  title="Expand menu"
                  style={{ position: 'absolute', top: 14, right: 6, background: 'none', border: 'none', cursor: 'pointer', color: C.textMuted, padding: '2px 4px', borderRadius: 4, lineHeight: 1, fontSize: 16, display: 'flex', alignItems: 'center', transition: 'color 0.15s' }}
                  onMouseEnter={(e) => (e.currentTarget.style.color = C.white)}
                  onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                >
                  &#8250;
                </button>
              )}
            </div>

            {/* Expand/Collapse all */}
            {!collapsed && !sidebar.isMobile && showExpandCollapseButtons && (
              <div style={{ padding: '10px 14px 0', display: 'flex', gap: 8, flexShrink: 0 }}>
                {[['Expand All', expandAll], ['Collapse All', collapseAll]].map(([label, fn]) => (
                  <button
                    key={label as string}
                    onClick={fn as () => void}
                    style={{ background: 'none', border: 'none', cursor: 'pointer', fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.textMuted, padding: 0, letterSpacing: '0.04em', transition: 'color 0.15s' }}
                    onMouseEnter={(e) => (e.currentTarget.style.color = C.textLight)}
                    onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                  >
                    {label as string}
                  </button>
                ))}
              </div>
            )}

            {/* Navigation */}
            <nav
              className="sidebar-scroll"
              style={{ flex: 1, overflowY: 'auto', overflowX: 'hidden', padding: collapsed ? '12px 0' : '12px 10px' }}
            >
              {visibleSections.map((section) => {
                const isCollapsible = section.items.length >= 2;
                const isSectionCollapsed = isCollapsible && sectionCollapsed[section.title] === true;

                return (
                  <div key={section.title} style={{ marginBottom: collapsed ? 4 : 16 }}>
                    {!collapsed && (
                      isCollapsible ? (
                        <button
                          onClick={() => toggleSection(section.title)}
                          aria-expanded={!isSectionCollapsed}
                          style={{ width: '100%', background: 'none', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '3px 6px', marginBottom: 4, borderRadius: 4 }}
                        >
                          <span style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 500 }}>
                            {section.title}
                          </span>
                          <span style={{ color: C.textMuted, fontSize: 14, lineHeight: 1, display: 'inline-block', transform: isSectionCollapsed ? 'rotate(0deg)' : 'rotate(90deg)', transition: 'transform 0.2s ease' }} aria-hidden="true">
                            ›
                          </span>
                        </button>
                      ) : (
                        <div style={{ padding: '3px 6px', marginBottom: 4, fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.textMuted, textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 500 }}>
                          {section.title}
                        </div>
                      )
                    )}

                    <div style={{ overflow: 'hidden', maxHeight: isSectionCollapsed && !collapsed ? '0px' : '600px', opacity: isSectionCollapsed && !collapsed ? 0 : 1, transition: 'max-height 0.3s cubic-bezier(0.4,0,0.2,1), opacity 0.3s cubic-bezier(0.4,0,0.2,1)' }}>
                      <ul style={{ listStyle: 'none', margin: 0, padding: 0 }}>
                        {section.items.map((item) => (
                          <li key={item.to}>
                            <NavLink
                              to={item.to}
                              title={collapsed ? item.label : undefined}
                              style={({ isActive }) => ({
                                display: 'flex',
                                alignItems: 'center',
                                gap: 8,
                                padding: collapsed ? '9px 0' : '7px 10px 7px 12px',
                                justifyContent: collapsed ? 'center' : 'flex-start',
                                borderRadius: collapsed ? 0 : 5,
                                marginBottom: 1,
                                textDecoration: 'none',
                                fontFamily: 'Syne, sans-serif',
                                fontSize: 13,
                                fontWeight: isActive ? 600 : 400,
                                color: isActive ? C.white : C.textDim,
                                background: isActive ? 'rgba(242,201,76,0.06)' : 'transparent',
                                borderLeft: collapsed ? 'none' : isActive ? `2px solid ${C.yellow}` : `2px solid transparent`,
                                transition: 'background 0.15s, color 0.15s, border-color 0.15s',
                                whiteSpace: 'nowrap',
                                overflow: 'hidden',
                              })}
                              onMouseEnter={(e) => {
                                const el = e.currentTarget;
                                if (el.getAttribute('aria-current') !== 'page') {
                                  el.style.background = C.panel;
                                  if (!collapsed) el.style.borderLeftColor = C.borderBright;
                                  el.style.color = C.textLight;
                                }
                              }}
                              onMouseLeave={(e) => {
                                const el = e.currentTarget;
                                if (el.getAttribute('aria-current') !== 'page') {
                                  el.style.background = 'transparent';
                                  if (!collapsed) el.style.borderLeftColor = 'transparent';
                                  el.style.color = C.textDim;
                                }
                              }}
                            >
                              {collapsed ? (
                                <span style={{ display: 'inline-block', width: 6, height: 6, borderRadius: '50%', background: C.textMuted, flexShrink: 0 }} />
                              ) : (
                                <span style={{ overflow: 'hidden', textOverflow: 'ellipsis' }}>{item.label}</span>
                              )}
                            </NavLink>
                          </li>
                        ))}
                      </ul>
                    </div>
                  </div>
                );
              })}
            </nav>

            {/* User footer */}
            <div
              style={{
                borderTop: `1px solid ${C.border}`,
                padding: collapsed ? '12px 0' : '12px 14px',
                flexShrink: 0,
                display: 'flex',
                alignItems: 'center',
                gap: collapsed ? 0 : 10,
                justifyContent: collapsed ? 'center' : 'flex-start',
                flexDirection: collapsed ? 'column' : 'row',
              }}
            >
              <div
                style={{ width: 32, height: 32, borderRadius: '50%', background: '#0D1421', border: `1px solid ${C.border}`, display: 'flex', alignItems: 'center', justifyContent: 'center', flexShrink: 0, fontFamily: 'Syne, sans-serif', fontWeight: 700, fontSize: 12, color: C.textLight, letterSpacing: '0.05em' }}
                title={username ?? undefined}
              >
                {initials}
              </div>

              {!collapsed && (
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 4 }}>
                    <span style={{ fontFamily: 'Syne, sans-serif', fontSize: 12, fontWeight: 600, color: C.textLight, whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: 100 }}>
                      {username}
                    </span>
                    {roleBadgeLabel && (
                      <span style={{ fontFamily: "'IBM Plex Mono', monospace", fontSize: 9, fontWeight: 700, color: C.yellow, background: 'rgba(242,201,76,0.15)', borderRadius: 4, padding: '1px 5px', textTransform: 'uppercase', letterSpacing: '0.08em', flexShrink: 0 }}>
                        {roleBadgeLabel}
                      </span>
                    )}
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                    <button
                      onClick={handleLogout}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', fontFamily: "'IBM Plex Mono', monospace", fontSize: 10, color: C.textMuted, padding: 0, letterSpacing: '0.04em', transition: 'color 0.15s' }}
                      onMouseEnter={(e) => (e.currentTarget.style.color = C.red)}
                      onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                    >
                      Sign Out
                    </button>
                    <button
                      onClick={toggleTheme}
                      title={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
                      style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, lineHeight: 1, padding: '2px 3px', borderRadius: 4, color: C.textMuted, transition: 'color 0.15s', flexShrink: 0 }}
                      onMouseEnter={(e) => (e.currentTarget.style.color = C.textLight)}
                      onMouseLeave={(e) => (e.currentTarget.style.color = C.textMuted)}
                      aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
                    >
                      {isDark ? '☀️' : '🌙'}
                    </button>
                  </div>
                </div>
              )}

              {collapsed && (
                <button
                  onClick={toggleTheme}
                  title={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
                  style={{ background: 'none', border: 'none', cursor: 'pointer', fontSize: 14, lineHeight: 1, padding: '4px', borderRadius: 4, color: C.textMuted, transition: 'color 0.15s', marginTop: 4 }}
                  aria-label={isDark ? 'Switch to light mode' : 'Switch to dark mode'}
                >
                  {isDark ? '☀️' : '🌙'}
                </button>
              )}
            </div>

          </div>
        </aside>

        {/* Main content */}
        <div className="flex-1 flex flex-col overflow-hidden bg-gray-100 dark:bg-gray-900">
          <Routes>
            {ROUTES.map(({ path, title, element, permission }) => {
              const hasAccess = isAdmin || !permission || permissions.includes(permission);
              return (
                <Route
                  key={path}
                  path={path}
                  element={
                    hasAccess ? (
                      <>
                        <header className="bg-white dark:bg-gray-800 shadow-sm px-3 md:px-6 py-3 md:py-4 flex-shrink-0 border-b border-gray-200 dark:border-gray-700 flex items-center gap-3">
                          {sidebar.isMobile && (
                            <button
                              onClick={() => sidebar.openSidebar()}
                              className="flex items-center justify-center min-h-[44px] min-w-[44px] -ml-1 text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-gray-100 transition-colors"
                              aria-label="Open navigation menu"
                            >
                              <svg className="w-6 h-6" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                                <path strokeLinecap="round" strokeLinejoin="round" d="M4 6h16M4 12h16M4 18h16" />
                              </svg>
                            </button>
                          )}
                          <h1 className="text-base md:text-lg font-semibold text-gray-800 dark:text-gray-100 truncate">{title}</h1>
                        </header>
                        <main className="flex-1 overflow-y-auto px-3 md:px-5 py-4 md:py-6 space-y-4 md:space-y-6 pb-20 md:pb-6">
                          {element}
                        </main>
                      </>
                    ) : (
                      <Navigate to="/admin/users" replace />
                    )
                  }
                />
              );
            })}
            <Route path="/" element={<Navigate to="/admin/users" replace />} />
            <Route path="*" element={<Navigate to="/admin/users" replace />} />
          </Routes>
        </div>
      </div>

      <BottomNav />
    </div>
  );
}

function ProtectedLayout() {
  return (
    <SidebarProvider>
      <ProtectedLayoutInner />
    </SidebarProvider>
  );
}

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------
export default function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/forgot-password" element={<ForgotPasswordPage />} />
      <Route path="/reset-password" element={<ResetPasswordPage />} />
      <Route path="/*" element={<ProtectedLayout />} />
    </Routes>
  );
}

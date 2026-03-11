import { createContext, useContext, useEffect, useState, useCallback, type ReactNode } from 'react';
import { useBreakpoint } from '../hooks/useMediaQuery';

interface SidebarState {
  /** Whether the sidebar is currently visible (mobile overlay open, or expanded on tablet/desktop) */
  isOpen: boolean;
  /** Whether the sidebar is in collapsed icon-rail mode (tablet/desktop only) */
  isCollapsed: boolean;
  /** True when viewing on mobile (<768px) */
  isMobile: boolean;
  /** True when viewing on tablet (768-1024px) */
  isTablet: boolean;
  /** True when viewing on desktop (>1024px) */
  isDesktop: boolean;
  /** Open the sidebar (mobile: show overlay; tablet: expand from icon-rail) */
  openSidebar: () => void;
  /** Close the sidebar (mobile: hide overlay; tablet: collapse to icon-rail) */
  closeSidebar: () => void;
  /** Toggle sidebar open/closed */
  toggleSidebar: () => void;
  /** Current sidebar pixel width for layout calculations */
  sidebarWidth: number;
}

const SidebarContext = createContext<SidebarState | null>(null);

export function useSidebar(): SidebarState {
  const ctx = useContext(SidebarContext);
  if (!ctx) throw new Error('useSidebar must be used within <SidebarProvider>');
  return ctx;
}

export function SidebarProvider({ children }: { children: ReactNode }) {
  const { isMobile, isTablet, isDesktop } = useBreakpoint();

  // Mobile: sidebar hidden by default (overlay behavior)
  // Tablet: sidebar collapsed (icon-rail) by default
  // Desktop: respect persisted preference
  const [mobileOpen, setMobileOpen] = useState(false);
  const [tabletExpanded, setTabletExpanded] = useState(false);
  const [desktopCollapsed, setDesktopCollapsed] = useState(
    () => localStorage.getItem('sidebar_collapsed') === 'true'
  );

  // Close mobile sidebar on breakpoint change away from mobile
  useEffect(() => {
    if (!isMobile) setMobileOpen(false);
  }, [isMobile]);

  // Reset tablet expansion on breakpoint change away from tablet
  useEffect(() => {
    if (!isTablet) setTabletExpanded(false);
  }, [isTablet]);

  const openSidebar = useCallback(() => {
    if (isMobile) setMobileOpen(true);
    else if (isTablet) setTabletExpanded(true);
    else {
      setDesktopCollapsed(false);
      localStorage.setItem('sidebar_collapsed', 'false');
    }
  }, [isMobile, isTablet]);

  const closeSidebar = useCallback(() => {
    if (isMobile) setMobileOpen(false);
    else if (isTablet) setTabletExpanded(false);
    else {
      setDesktopCollapsed(true);
      localStorage.setItem('sidebar_collapsed', 'true');
    }
  }, [isMobile, isTablet]);

  const toggleSidebar = useCallback(() => {
    if (isMobile) {
      setMobileOpen((v) => !v);
    } else if (isTablet) {
      setTabletExpanded((v) => !v);
    } else {
      setDesktopCollapsed((v) => {
        const next = !v;
        localStorage.setItem('sidebar_collapsed', String(next));
        return next;
      });
    }
  }, [isMobile, isTablet]);

  // Derived values
  let isOpen: boolean;
  let isCollapsed: boolean;
  let sidebarWidth: number;

  if (isMobile) {
    isOpen = mobileOpen;
    isCollapsed = false;
    sidebarWidth = 0; // Mobile: sidebar is overlay, doesn't push content
  } else if (isTablet) {
    isOpen = tabletExpanded;
    isCollapsed = !tabletExpanded;
    sidebarWidth = tabletExpanded ? 260 : 68;
  } else {
    isOpen = !desktopCollapsed;
    isCollapsed = desktopCollapsed;
    sidebarWidth = desktopCollapsed ? 56 : 248;
  }

  return (
    <SidebarContext.Provider
      value={{
        isOpen,
        isCollapsed,
        isMobile,
        isTablet,
        isDesktop,
        openSidebar,
        closeSidebar,
        toggleSidebar,
        sidebarWidth,
      }}
    >
      {children}
    </SidebarContext.Provider>
  );
}

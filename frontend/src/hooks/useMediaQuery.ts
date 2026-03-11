import { useEffect, useState } from 'react';

/**
 * React hook that tracks a CSS media query match.
 * Returns `true` when the media query matches, `false` otherwise.
 */
export function useMediaQuery(query: string): boolean {
  const [matches, setMatches] = useState(() => {
    if (typeof window === 'undefined') return false;
    return window.matchMedia(query).matches;
  });

  useEffect(() => {
    const mql = window.matchMedia(query);
    const handler = (e: MediaQueryListEvent) => setMatches(e.matches);
    mql.addEventListener('change', handler);
    // Sync initial state
    setMatches(mql.matches);
    return () => mql.removeEventListener('change', handler);
  }, [query]);

  return matches;
}

/**
 * Returns responsive breakpoint state using Tailwind-aligned breakpoints:
 * - isMobile: < 768px (default / no prefix)
 * - isTablet: 768px - 1024px (md: prefix)
 * - isDesktop: >= 1025px (lg: prefix)
 */
export function useBreakpoint() {
  const isMobile = useMediaQuery('(max-width: 767px)');
  const isTablet = useMediaQuery('(min-width: 768px) and (max-width: 1024px)');
  const isDesktop = useMediaQuery('(min-width: 1025px)');

  return { isMobile, isTablet, isDesktop };
}

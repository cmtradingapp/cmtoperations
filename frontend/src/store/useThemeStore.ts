import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface ThemeState {
  isDark: boolean;
  toggle: () => void;
}

function applyTheme(isDark: boolean) {
  if (isDark) {
    document.documentElement.classList.add('dark');
  } else {
    document.documentElement.classList.remove('dark');
  }
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set, get) => ({
      isDark: false,
      toggle: () => {
        const next = !get().isDark;
        applyTheme(next);
        set({ isDark: next });
      },
    }),
    {
      name: 'theme',
      onRehydrateStorage: () => (state) => {
        if (state) {
          applyTheme(state.isDark);
        }
      },
    }
  )
);

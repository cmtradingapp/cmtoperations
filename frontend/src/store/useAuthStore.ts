import { create } from 'zustand';
import { persist } from 'zustand/middleware';

/** Action-level CRM permissions map (CLAUD-16 RBAC). */
export type CrmPermissions = Record<string, boolean>;

interface AuthState {
  token: string | null;
  username: string | null;
  role: string | null;
  permissions: string[];
  crmPermissions: CrmPermissions;
  // Impersonation state
  originalAdminToken: string | null;
  originalAdminUsername: string | null;
  originalAdminRole: string | null;
  originalAdminPermissions: string[];
  isImpersonating: boolean;
  impersonatingUsername: string | null;
  impersonatingRole: string | null;
  // Actions
  setAuth: (token: string, username: string, role: string, permissions: string[]) => void;
  setCrmPermissions: (perms: CrmPermissions) => void;
  setImpersonation: (token: string, username: string, role: string, permissions: string[]) => void;
  clearImpersonation: () => void;
  logout: () => void;
  isAuthenticated: () => boolean;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set, get) => ({
      token: null,
      username: null,
      role: null,
      permissions: [],
      crmPermissions: {},
      originalAdminToken: null,
      originalAdminUsername: null,
      originalAdminRole: null,
      originalAdminPermissions: [],
      isImpersonating: false,
      impersonatingUsername: null,
      impersonatingRole: null,
      setAuth: (token, username, role, permissions) =>
        set({ token, username, role, permissions }),
      setCrmPermissions: (perms) => set({ crmPermissions: perms }),
      setImpersonation: (token, username, role, permissions) => {
        const state = get();
        set({
          originalAdminToken: state.token,
          originalAdminUsername: state.username,
          originalAdminRole: state.role,
          originalAdminPermissions: state.permissions,
          token,
          username,
          role,
          permissions,
          isImpersonating: true,
          impersonatingUsername: username,
          impersonatingRole: role,
        });
      },
      clearImpersonation: () => {
        const state = get();
        set({
          token: state.originalAdminToken,
          username: state.originalAdminUsername,
          role: state.originalAdminRole,
          permissions: state.originalAdminPermissions,
          originalAdminToken: null,
          originalAdminUsername: null,
          originalAdminRole: null,
          originalAdminPermissions: [],
          isImpersonating: false,
          impersonatingUsername: null,
          impersonatingRole: null,
        });
      },
      logout: () =>
        set({
          token: null,
          username: null,
          role: null,
          permissions: [],
          crmPermissions: {},
          originalAdminToken: null,
          originalAdminUsername: null,
          originalAdminRole: null,
          originalAdminPermissions: [],
          isImpersonating: false,
          impersonatingUsername: null,
          impersonatingRole: null,
        }),
      isAuthenticated: () => !!get().token,
    }),
    { name: 'auth' }
  )
);

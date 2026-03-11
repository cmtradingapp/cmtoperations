import axios from 'axios';
import type { CrmPermissions } from '../store/useAuthStore';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
});

/** Fetch the current user's action-level CRM permissions. */
export async function fetchMyCrmPermissions(token: string): Promise<CrmPermissions> {
  const res = await api.get('/permissions/my', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data as CrmPermissions;
}

export interface PermissionRow {
  id: number;
  role: string;
  action: string;
  enabled: boolean;
}

/** Admin: fetch all permission rows. */
export async function fetchAllPermissions(token: string): Promise<PermissionRow[]> {
  const res = await api.get('/permissions/', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data as PermissionRow[];
}

/** Admin: toggle a permission's enabled state. */
export async function togglePermission(
  token: string,
  permissionId: number,
  enabled: boolean,
): Promise<PermissionRow> {
  const res = await api.put(
    `/permissions/${permissionId}`,
    { enabled },
    { headers: { Authorization: `Bearer ${token}` } },
  );
  return res.data as PermissionRow;
}

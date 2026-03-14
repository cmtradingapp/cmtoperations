const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export interface AddProtectedResult {
  status: 'success' | 'already_protected' | 'client_not_found';
  action?: 'added' | 'updated' | 'reactivated';
  accountid?: string;
  group?: number;
  mt4login?: string;
  trading_account_id?: string;
  message?: string;
  current_group?: number;
}

export async function addProtectedClient(
  accountid: string,
  group: number
): Promise<AddProtectedResult> {
  const res = await fetch(`${API_BASE}/protected-clients/add`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify({ accountid, group }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.detail || 'Request failed');
  return json;
}

export async function fetchProtectedClients(active?: number): Promise<Record<string, unknown>[]> {
  const url = active !== undefined
    ? `${API_BASE}/protected-clients/list?active=${active}`
    : `${API_BASE}/protected-clients/list`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch protected clients');
  return res.json();
}

export async function reactivateAll(): Promise<{ reactivated: number }> {
  const res = await fetch(`${API_BASE}/protected-clients/reactivate-all`, {
    method: 'POST',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error('Failed to reactivate');
  return res.json();
}

export async function fetchLegacyProtectedClients(active?: number): Promise<Record<string, unknown>[]> {
  const url = active !== undefined
    ? `${API_BASE}/protected-clients/list-legacy?active=${active}`
    : `${API_BASE}/protected-clients/list-legacy`;
  const res = await fetch(url, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch clients in protected');
  return res.json();
}

export async function fetchProtectionGroups(): Promise<Record<string, unknown>[]> {
  const res = await fetch(`${API_BASE}/protected-clients/groups`, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch protection groups');
  return res.json();
}

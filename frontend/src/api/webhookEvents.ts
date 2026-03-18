const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export const KNOWN_EVENTS = ['open_trade'] as const;

export const ACTION_TYPES = ['log_only', 'optimove', 'chrome_plugin', 'challenge', 'bonus'] as const;
export type ActionType = (typeof ACTION_TYPES)[number];

export interface ActionRule {
  id: number;
  event_name: string;
  action_type: ActionType;
  label: string | null;
  config: Record<string, unknown>;
  is_active: boolean;
  created_at: string;
  updated_at: string;
}

export interface EventLogRow {
  id: number;
  event_name: string;
  customer: string | null;
  payload: Record<string, unknown>;
  actions_applied: Array<{ rule_id: number; action: string; result: string }>;
  created_at: string;
}

export interface EventLogResponse {
  total: number;
  page: number;
  page_size: number;
  rows: EventLogRow[];
}

export interface EventStat {
  event_name: string;
  total: number;
  last_received: string | null;
}

// ── Log ─────────────────────────────────────────────────────────────────────

export async function fetchEventLog(params: {
  event_name?: string;
  customer?: string;
  page?: number;
  page_size?: number;
}): Promise<EventLogResponse> {
  const q = new URLSearchParams();
  if (params.event_name) q.set('event_name', params.event_name);
  if (params.customer) q.set('customer', params.customer);
  if (params.page) q.set('page', String(params.page));
  if (params.page_size) q.set('page_size', String(params.page_size));
  const res = await fetch(`${API_BASE}/webhook-events/log?${q}`, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch event log');
  return res.json();
}

export async function fetchEventStats(): Promise<EventStat[]> {
  const res = await fetch(`${API_BASE}/webhook-events/stats`, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch event stats');
  return res.json();
}

// ── Action rules ─────────────────────────────────────────────────────────────

export async function fetchActionRules(): Promise<ActionRule[]> {
  const res = await fetch(`${API_BASE}/webhook-events/actions`, { headers: authHeaders() });
  if (!res.ok) throw new Error('Failed to fetch action rules');
  return res.json();
}

export async function createActionRule(body: {
  event_name: string;
  action_type: ActionType;
  label?: string;
  config?: Record<string, unknown>;
  is_active?: boolean;
}): Promise<{ id: number }> {
  const res = await fetch(`${API_BASE}/webhook-events/actions`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(body),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(json.detail || 'Failed to create rule');
  return json;
}

export async function updateActionRule(
  id: number,
  body: Partial<{ action_type: ActionType; label: string; config: Record<string, unknown>; is_active: boolean }>
): Promise<void> {
  const res = await fetch(`${API_BASE}/webhook-events/actions/${id}`, {
    method: 'PATCH',
    headers: { 'Content-Type': 'application/json', ...authHeaders() },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error('Failed to update rule');
}

export async function deleteActionRule(id: number): Promise<void> {
  const res = await fetch(`${API_BASE}/webhook-events/actions/${id}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error('Failed to delete rule');
}

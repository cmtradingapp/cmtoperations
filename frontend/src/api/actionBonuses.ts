/**
 * CLAUD-96: Action Bonuses — API helpers.
 */

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token
    ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
    : { 'Content-Type': 'application/json' };
}

export type ActionType = 'live_details' | 'submit_documents';

export interface ActionBonusRule {
  id: number;
  action: ActionType;
  countries: string[] | null;
  affiliates: string[] | null;
  reward_amount: number;
  reward_type: string;
  priority: number;
  isactive: boolean;
  created_at: string | null;
}

export interface ActionBonusRulePayload {
  action: ActionType;
  countries: string[] | null;
  affiliates: string[] | null;
  reward_amount: number;
  isactive: boolean;
}

export interface BonusLogItem {
  id: number;
  rule_id: number;
  accountid: string;
  trading_account_id: string;
  action: ActionType;
  reward_amount: number;
  country: string | null;
  affiliate: string | null;
  success: boolean;
  created_at: string | null;
}

export interface BonusLogResponse {
  total: number;
  items: BonusLogItem[];
}

export interface Campaign {
  id: string;
  name: string | null;
}

export async function getRules(action?: ActionType): Promise<ActionBonusRule[]> {
  const qs = action ? `?action=${action}` : '';
  const res = await fetch(`${API_BASE}/action-bonuses/rules${qs}`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch rules: ${res.status}`);
  return res.json();
}

export async function createRule(data: ActionBonusRulePayload): Promise<{ status: string; id: number }> {
  const res = await fetch(`${API_BASE}/action-bonuses/rules`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to create rule: ${res.status}`);
  }
  return res.json();
}

export async function updateRule(id: number, data: ActionBonusRulePayload): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/action-bonuses/rules/${id}`, {
    method: 'PUT',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) throw new Error(`Failed to update rule: ${res.status}`);
  return res.json();
}

export async function toggleRule(id: number): Promise<{ status: string; isactive: boolean }> {
  const res = await fetch(`${API_BASE}/action-bonuses/rules/${id}/toggle`, {
    method: 'PATCH',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to toggle rule: ${res.status}`);
  return res.json();
}

export async function deleteRule(id: number): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/action-bonuses/rules/${id}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to delete rule: ${res.status}`);
  return res.json();
}

export async function reorderRules(action: ActionType, rule_ids: number[]): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/action-bonuses/rules/reorder`, {
    method: 'PATCH',
    headers: authHeaders(),
    body: JSON.stringify({ action, rule_ids }),
  });
  if (!res.ok) throw new Error(`Failed to reorder rules: ${res.status}`);
  return res.json();
}

export async function getBonusLog(params: {
  action?: ActionType;
  country?: string;
  success?: boolean;
  date_from?: string;
  date_to?: string;
  page?: number;
  page_size?: number;
}): Promise<BonusLogResponse> {
  const sp = new URLSearchParams();
  if (params.action) sp.set('action', params.action);
  if (params.country) sp.set('country', params.country);
  if (params.success !== undefined) sp.set('success', String(params.success));
  if (params.date_from) sp.set('date_from', params.date_from);
  if (params.date_to) sp.set('date_to', params.date_to);
  if (params.page) sp.set('page', String(params.page));
  if (params.page_size) sp.set('page_size', String(params.page_size));
  const qs = sp.toString();
  const res = await fetch(`${API_BASE}/action-bonuses/log${qs ? `?${qs}` : ''}`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch bonus log: ${res.status}`);
  return res.json();
}

export async function getCampaigns(): Promise<Campaign[]> {
  const res = await fetch(`${API_BASE}/action-bonuses/campaigns`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch campaigns: ${res.status}`);
  return res.json();
}

export async function getCountries(): Promise<string[]> {
  const res = await fetch(`${API_BASE}/action-bonuses/countries`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch countries: ${res.status}`);
  return res.json();
}

/**
 * CLAUD-89/90: Challenges Module — API helpers for challenge CRUD and progress.
 */

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' } : { 'Content-Type': 'application/json' };
}

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ChallengeTier {
  challengeId?: number;
  name: string;
  targetvalue: number;
  rewardamount: number;
  tier_rank?: number;
  symbol?: string | null;
}

export interface AudienceCriteria {
  all_clients?: boolean;
  countries?: string[];
  languages?: string[];
  balance_min?: number;
  balance_max?: number;
  last_trade_before?: string;
  qualified_before?: string;
  account_ids?: string[];
}

export interface ChallengeGroup {
  group_name: string;
  type: 'trade' | 'volume' | 'streak' | 'pnl' | 'diversity' | 'instrument';
  isactive: number;
  tiers: ChallengeTier[];
  InsertDate: string;
  audience_criteria?: AudienceCriteria | null;
  timeperiod?: 'daily' | 'weekly';
  valid_until?: string | null;       // ISO datetime or null
  reward_multiplier?: number;        // default 1.0
  expires_on?: string | null;        // YYYY-MM-DD
}

export interface CreateChallengePayload {
  group_name: string;
  type: 'trade' | 'volume' | 'streak' | 'pnl' | 'diversity' | 'instrument';
  tiers: { name: string; targetvalue: number; rewardamount: number; symbol?: string }[];
  audience_criteria?: AudienceCriteria | null;
  timeperiod?: 'daily' | 'weekly';
  valid_until?: string | null;       // ISO datetime string
  reward_multiplier?: number;        // default 1.0
  expires_on?: string | null;        // YYYY-MM-DD
}

/** CLAUD-90: Client progress record */
export interface ChallengeProgressItem {
  accountid: string;
  group_name: string;
  challenge_type: string;
  progress_value: number;
  total_tiers: number;
  last_rewarded_tier: number;
  total_reward: number;
  status: 'Open' | 'In Progress' | 'Completed' | 'Cancelled';
  date: string;
}

export interface ChallengeProgressResponse {
  total: number;
  items: ChallengeProgressItem[];
}

export interface ChallengeProgressParams {
  date?: string;
  group_name?: string;
  accountid?: string;
  page?: number;
  page_size?: number;
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

export async function getChallenges(): Promise<ChallengeGroup[]> {
  const res = await fetch(`${API_BASE}/challenges`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch challenges: ${res.status}`);
  return res.json();
}

export async function createChallenge(data: CreateChallengePayload): Promise<{ status: string; group_name: string; tiers: number }> {
  const res = await fetch(`${API_BASE}/challenges`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to create challenge: ${res.status}`);
  }
  return res.json();
}

export async function toggleChallenge(groupName: string): Promise<{ status: string; group_name: string; isactive: number }> {
  const res = await fetch(`${API_BASE}/challenges/${encodeURIComponent(groupName)}/toggle`, {
    method: 'PATCH',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to toggle challenge: ${res.status}`);
  return res.json();
}

export async function deleteChallenge(groupName: string): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/challenges/${encodeURIComponent(groupName)}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to delete challenge: ${res.status}`);
  return res.json();
}

/** CLAUD-91: Optimove event log item */
export interface OptimoveEventItem {
  id: number;
  challengeId: number | null;
  group_name: string;
  accountid: string;
  event_name: string;
  payload: Record<string, unknown> | null;
  response: string;
  success: boolean;
  created_at: string | null;
}

export interface OptimoveEventsResponse {
  total: number;
  items: OptimoveEventItem[];
}

export interface OptimoveEventsParams {
  date?: string;
  event_name?: string;
  page?: number;
  page_size?: number;
}

/** CLAUD-91: Fetch paginated Optimove event log */
export async function getOptimoveEvents(params: OptimoveEventsParams): Promise<OptimoveEventsResponse> {
  const searchParams = new URLSearchParams();
  if (params.date) searchParams.set('date', params.date);
  if (params.event_name) searchParams.set('event_name', params.event_name);
  if (params.page) searchParams.set('page', String(params.page));
  if (params.page_size) searchParams.set('page_size', String(params.page_size));

  const qs = searchParams.toString();
  const res = await fetch(`${API_BASE}/challenges/events${qs ? `?${qs}` : ''}`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch Optimove events: ${res.status}`);
  return res.json();
}

export interface SymbolMapping {
  symbol: string;
  asset_class: 'forex' | 'commodity' | 'index' | 'crypto' | 'stock';
}

/** CLAUD-90: Fetch paginated client progress */
export async function getChallengeProgress(params: ChallengeProgressParams): Promise<ChallengeProgressResponse> {
  const searchParams = new URLSearchParams();
  if (params.date) searchParams.set('date', params.date);
  if (params.group_name) searchParams.set('group_name', params.group_name);
  if (params.accountid) searchParams.set('accountid', params.accountid);
  if (params.page) searchParams.set('page', String(params.page));
  if (params.page_size) searchParams.set('page_size', String(params.page_size));

  const qs = searchParams.toString();
  const res = await fetch(`${API_BASE}/challenges/progress${qs ? `?${qs}` : ''}`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch challenge progress: ${res.status}`);
  return res.json();
}

/** CLAUD-94: Fetch all symbol → asset class mappings */
export async function getSymbolMappings(): Promise<SymbolMapping[]> {
  const res = await fetch(`${API_BASE}/challenges/symbols`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch symbol mappings: ${res.status}`);
  return res.json();
}

/** CLAUD-94: Create or update a symbol mapping */
export async function upsertSymbolMapping(data: SymbolMapping): Promise<{ status: string; symbol: string; asset_class: string }> {
  const res = await fetch(`${API_BASE}/challenges/symbols`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to save symbol mapping: ${res.status}`);
  }
  return res.json();
}

/** CLAUD-94: Delete a symbol mapping */
export async function deleteSymbolMapping(symbol: string): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/challenges/symbols/${encodeURIComponent(symbol)}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to delete symbol mapping: ${res.status}`);
  return res.json();
}

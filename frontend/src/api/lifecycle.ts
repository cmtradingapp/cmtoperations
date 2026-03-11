/**
 * CLAUD-97: Lifecycle Stages -- API helpers.
 */

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

function authHeaders(): Record<string, string> {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  return token
    ? { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }
    : { 'Content-Type': 'application/json' };
}

export type MetricType = 'ftd' | 'deposit' | 'position' | 'volume' | 'custom';

export interface LifecycleStage {
  id: number;
  name: string;
  key: string;
  metric_type: MetricType;
  threshold: number;
  display_order: number;
  is_active: boolean;
  created_at: string | null;
  updated_at: string | null;
}

export interface StagePayload {
  name: string;
  metric_type: MetricType;
  threshold: number;
}

export async function getStages(): Promise<LifecycleStage[]> {
  const res = await fetch(`${API_BASE}/lifecycle/stages`, { headers: authHeaders() });
  if (!res.ok) throw new Error(`Failed to fetch stages: ${res.status}`);
  return res.json();
}

export async function createStage(data: StagePayload): Promise<LifecycleStage> {
  const res = await fetch(`${API_BASE}/lifecycle/stages`, {
    method: 'POST',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to create stage: ${res.status}`);
  }
  return res.json();
}

export async function updateStage(id: number, data: StagePayload): Promise<LifecycleStage> {
  const res = await fetch(`${API_BASE}/lifecycle/stages/${id}`, {
    method: 'PUT',
    headers: authHeaders(),
    body: JSON.stringify(data),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to update stage: ${res.status}`);
  }
  return res.json();
}

export async function deleteStage(id: number): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/lifecycle/stages/${id}`, {
    method: 'DELETE',
    headers: authHeaders(),
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error((err as { detail?: string }).detail || `Failed to delete stage: ${res.status}`);
  }
  return res.json();
}

export async function toggleStage(id: number): Promise<{ status: string; id: number; is_active: boolean }> {
  const res = await fetch(`${API_BASE}/lifecycle/stages/${id}/toggle`, {
    method: 'PATCH',
    headers: authHeaders(),
  });
  if (!res.ok) throw new Error(`Failed to toggle stage: ${res.status}`);
  return res.json();
}

export async function reorderStages(order: number[]): Promise<{ status: string }> {
  const res = await fetch(`${API_BASE}/lifecycle/stages/reorder`, {
    method: 'PATCH',
    headers: authHeaders(),
    body: JSON.stringify({ order }),
  });
  if (!res.ok) throw new Error(`Failed to reorder stages: ${res.status}`);
  return res.json();
}

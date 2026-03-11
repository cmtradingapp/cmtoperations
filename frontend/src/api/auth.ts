import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
});

export async function login(username: string, password: string) {
  const res = await api.post('/auth/login', { username, password });
  return res.data as {
    access_token: string;
    username: string;
    role: string;
    permissions: string[];
  };
}

/** Fetch current user info including fresh permissions from the DB (CLAUD-67). */
export async function fetchMe(token: string) {
  const res = await api.get('/auth/me', {
    headers: { Authorization: `Bearer ${token}` },
  });
  return res.data as {
    id: number;
    username: string;
    email: string | null;
    role: string;
    is_active: boolean;
    permissions: string[];
    vtiger_user_id: number | null;
    vtiger_office: string | null;
    vtiger_department: string | null;
  };
}

// ---------------------------------------------------------------------------
// Password reset helpers (CLAUD-79)
// ---------------------------------------------------------------------------

const API_BASE = import.meta.env.VITE_API_BASE_URL || '/api';

/** Request a password reset email. Always resolves — do not reveal whether
 *  the email exists (the server follows the same rule). */
export async function requestPasswordReset(email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/auth/forgot-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email }),
  });
  if (!res.ok) throw new Error('Request failed');
}

/** Complete a password reset using the one-time token from the email link.
 *  Throws an error with `status` property set to the HTTP status code on
 *  failure (400 = expired/invalid token). */
export async function resetPassword(token: string, newPassword: string): Promise<void> {
  const res = await fetch(`${API_BASE}/auth/reset-password`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, new_password: newPassword }),
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({}));
    throw Object.assign(new Error((data as { detail?: string }).detail || 'Request failed'), {
      status: res.status,
    });
  }
}

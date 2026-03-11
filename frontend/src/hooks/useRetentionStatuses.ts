import { useState, useEffect } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export interface RetentionStatus {
  key: number;
  label: string;
}

let _cache: RetentionStatus[] | null = null;
let _cachePromise: Promise<RetentionStatus[]> | null = null;

export function useRetentionStatuses() {
  const [statuses, setStatuses] = useState<RetentionStatus[]>(_cache ?? []);
  const [loading, setLoading] = useState(_cache === null);

  useEffect(() => {
    if (_cache !== null) {
      setStatuses(_cache);
      setLoading(false);
      return;
    }
    if (!_cachePromise) {
      _cachePromise = api.get<RetentionStatus[]>('/retention/statuses').then((r) => {
        _cache = r.data;
        return r.data;
      });
    }
    _cachePromise
      .then((data) => {
        setStatuses(data);
        setLoading(false);
      })
      .catch(() => {
        setLoading(false);
      });
  }, []);

  return { statuses, loading };
}

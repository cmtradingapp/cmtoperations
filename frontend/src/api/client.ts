import axios from 'axios';

import type { CallResponse, ClientDetail, ConversationsResponse, Country, FilterParams, SalesStatus } from '../types';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_BASE_URL || '/api',
});

api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

export async function getClients(filters: FilterParams): Promise<ClientDetail[]> {
  // Strip undefined / empty-string values so they don't appear as query params
  const params = Object.fromEntries(
    Object.entries(filters).filter(([, v]) => v !== undefined && v !== '')
  );
  const response = await api.get<ClientDetail[]>('/clients', { params });
  return response.data;
}

export async function getCountries(): Promise<Country[]> {
  const response = await api.get<Country[]>('/filters/countries');
  return response.data;
}

export async function getStatuses(): Promise<SalesStatus[]> {
  const response = await api.get<SalesStatus[]>('/filters/statuses');
  return response.data;
}

export async function getCallHistory(params?: {
  agent_id?: string;
  call_successful?: string;
  page_size?: number;
  cursor?: string;
}): Promise<ConversationsResponse> {
  const response = await api.get<ConversationsResponse>('/calls/history', { params });
  return response.data;
}

export async function initiateCalls(
  clientIds: string[],
  agentId: string,
  agentPhoneNumberId: string,
  callProvider: string = 'twilio',
): Promise<CallResponse> {
  const response = await api.post<CallResponse>('/calls/initiate', {
    client_ids: clientIds,
    agent_id: agentId || undefined,
    agent_phone_number_id: agentPhoneNumberId || undefined,
    call_provider: callProvider,
  });
  return response.data;
}

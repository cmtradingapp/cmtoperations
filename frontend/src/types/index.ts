export type CallStatusType = 'idle' | 'calling' | 'initiated' | 'failed';

export interface Country {
  name: string;
  iso2code: string;
}

export interface SalesStatus {
  id: number;
  value: string;
}

export interface FilterParams {
  date_from?: string;
  date_to?: string;
  sales_status?: number;
  region?: string;
  custom_field?: string;
  sales_client_potential?: number;
  sales_client_potential_op?: string;
  language?: string;
  live?: string;
  ftd?: string;
}

export interface ClientDetail {
  client_id: string;
  name: string;
  status: string;
  region?: string;
  created_at?: string;
  phone_number?: string;
  email?: string;
  account_manager?: string;
  sales_client_potential?: number;
  language?: string;
}

export interface ClientCallResult {
  client_id: string;
  status: 'initiated' | 'failed';
  conversation_id?: string;
  error?: string;
}

export interface CallResponse {
  results: ClientCallResult[];
}

export interface ElevenLabsConversation {
  conversation_id: string;
  agent_id: string;
  agent_name?: string;
  start_time_unix_secs?: number;
  call_duration_secs?: number;
  call_successful?: 'success' | 'failure' | 'unknown';
}

export interface ConversationsResponse {
  conversations: ElevenLabsConversation[];
  has_more: boolean;
  next_cursor?: string;
}

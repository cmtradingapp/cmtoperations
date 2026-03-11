import { useEffect, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

interface Agent {
  agent_id: string;
  name: string;
}

interface AgentSelectProps {
  value: string;
  onChange: (agentId: string) => void;
  placeholder?: string;
  className?: string;
}

export function AgentSelect({ value, onChange, placeholder = 'Select agent…', className = '' }: AgentSelectProps) {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get('/elevenlabs/agents')
      .then((res) => setAgents(res.data.agents || []))
      .catch(() => setAgents([]))
      .finally(() => setLoading(false));
  }, []);

  const baseClass = `w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm
    focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white dark:bg-gray-800
    text-gray-900 dark:text-gray-100 disabled:opacity-50 ${className}`;

  return (
    <select
      value={value}
      onChange={(e) => onChange(e.target.value)}
      disabled={loading}
      className={baseClass}
    >
      <option value="">{loading ? 'Loading agents…' : placeholder}</option>
      {agents.map((a) => (
        <option key={a.agent_id} value={a.agent_id}>{a.name}</option>
      ))}
    </select>
  );
}

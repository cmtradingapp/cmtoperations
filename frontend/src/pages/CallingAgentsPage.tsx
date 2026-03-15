import { useEffect, useRef, useState } from 'react';
import axios from 'axios';

const api = axios.create({ baseURL: import.meta.env.VITE_API_BASE_URL || '/api' });
api.interceptors.request.use((config) => {
  const auth = JSON.parse(localStorage.getItem('auth') || '{}');
  const token = auth?.state?.token;
  if (token) config.headers.Authorization = `Bearer ${token}`;
  return config;
});

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface Voice {
  voice_id: string;
  name: string;
  category?: string;
}

interface CallingAgent {
  id: number;
  name: string;
  opportunity_type: string;
  description?: string;
  system_prompt: string;
  first_message?: string;
  voice_id?: string;
  voice_name?: string;
  elevenlabs_agent_id?: string;
  status: string;
  created_at?: string;
  updated_at?: string;
}

interface GenerateScriptResult {
  system_prompt: string;
  first_message: string;
  evaluation_criteria: string[];
}

const OPPORTUNITY_TYPES = [
  { value: 'Margin Call', label: 'Margin Call' },
  { value: 'Deposit Reminder', label: 'Deposit Reminder' },
  { value: 'Inactive Client Re-engagement', label: 'Inactive Client Re-engagement' },
  { value: 'First Deposit Encouragement', label: 'First Deposit Encouragement' },
  { value: 'Account Upgrade', label: 'Account Upgrade' },
  { value: 'Custom', label: 'Custom' },
];

// ---------------------------------------------------------------------------
// Status badge
// ---------------------------------------------------------------------------

function StatusBadge({ status }: { status: string }) {
  const styles: Record<string, string> = {
    active: 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300',
    draft: 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/40 dark:text-yellow-300',
    inactive: 'bg-gray-100 text-gray-600 dark:bg-gray-700 dark:text-gray-400',
  };
  return (
    <span className={`px-2 py-0.5 rounded-full text-xs font-medium ${styles[status] ?? styles.inactive}`}>
      {status}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Main page
// ---------------------------------------------------------------------------

export function CallingAgentsPage() {
  // Agents list
  const [agents, setAgents] = useState<CallingAgent[]>([]);
  const [loadingAgents, setLoadingAgents] = useState(false);
  const [agentsError, setAgentsError] = useState<string | null>(null);

  // Voices
  const [voices, setVoices] = useState<Voice[]>([]);
  const [loadingVoices, setLoadingVoices] = useState(false);

  // Form
  const [showForm, setShowForm] = useState(false);
  const [agentName, setAgentName] = useState('');
  const [opportunityType, setOpportunityType] = useState('Margin Call');
  const [description, setDescription] = useState('');
  const [voiceId, setVoiceId] = useState('');
  const [voiceName, setVoiceName] = useState('');
  const [systemPrompt, setSystemPrompt] = useState('');
  const [firstMessage, setFirstMessage] = useState('');
  const [evalCriteria, setEvalCriteria] = useState<string[]>([]);

  // Script generation
  const [generating, setGenerating] = useState(false);
  const [genError, setGenError] = useState<string | null>(null);

  // Creating agent
  const [creating, setCreating] = useState(false);
  const [createError, setCreateError] = useState<string | null>(null);

  // Expanded agent row
  const [expandedId, setExpandedId] = useState<number | null>(null);

  // Delete
  const [deletingId, setDeletingId] = useState<number | null>(null);

  // Edit modal
  const [editAgent, setEditAgent] = useState<CallingAgent | null>(null);
  const [editSystemPrompt, setEditSystemPrompt] = useState('');
  const [editFirstMessage, setEditFirstMessage] = useState('');
  const [editSaving, setEditSaving] = useState(false);
  const [editError, setEditError] = useState<string | null>(null);

  const formRef = useRef<HTMLDivElement>(null);

  // ---------------------------------------------------------------------------
  // Load agents + voices
  // ---------------------------------------------------------------------------

  const loadAgents = async () => {
    setLoadingAgents(true);
    setAgentsError(null);
    try {
      const res = await api.get<CallingAgent[]>('/calling/agents');
      setAgents(res.data);
    } catch {
      setAgentsError('Failed to load agents');
    } finally {
      setLoadingAgents(false);
    }
  };

  const loadVoices = async () => {
    setLoadingVoices(true);
    try {
      const res = await api.get<Voice[]>('/calling/voices');
      setVoices(res.data);
      if (res.data.length > 0 && !voiceId) {
        setVoiceId(res.data[0].voice_id);
        setVoiceName(res.data[0].name);
      }
    } catch {
      // non-critical
    } finally {
      setLoadingVoices(false);
    }
  };

  useEffect(() => {
    loadAgents();
    loadVoices();
  }, []); // eslint-disable-line react-hooks/exhaustive-deps

  // ---------------------------------------------------------------------------
  // Generate script
  // ---------------------------------------------------------------------------

  const generateScript = async () => {
    setGenerating(true);
    setGenError(null);
    setSystemPrompt('');
    setFirstMessage('');
    setEvalCriteria([]);
    try {
      const res = await api.post<GenerateScriptResult>('/calling/agents/generate-script', {
        opportunity_type: opportunityType,
        description,
      });
      setSystemPrompt(res.data.system_prompt);
      setFirstMessage(res.data.first_message);
      setEvalCriteria(res.data.evaluation_criteria ?? []);
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setGenError(`Script generation failed: ${msg}`);
    } finally {
      setGenerating(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Create agent
  // ---------------------------------------------------------------------------

  const createAgent = async () => {
    if (!agentName.trim()) { setCreateError('Agent name is required'); return; }
    if (!systemPrompt.trim()) { setCreateError('Generate or write a system prompt first'); return; }
    setCreating(true);
    setCreateError(null);
    try {
      const selectedVoice = voices.find((v) => v.voice_id === voiceId);
      await api.post('/calling/agents', {
        name: agentName,
        opportunity_type: opportunityType,
        description,
        system_prompt: systemPrompt,
        first_message: firstMessage,
        voice_id: voiceId,
        voice_name: selectedVoice?.name ?? voiceName,
        create_on_elevenlabs: !!voiceId,
      });
      // Reset form
      setAgentName('');
      setDescription('');
      setSystemPrompt('');
      setFirstMessage('');
      setEvalCriteria([]);
      setShowForm(false);
      await loadAgents();
    } catch (e: unknown) {
      const msg = e instanceof Error ? e.message : String(e);
      setCreateError(`Failed to create agent: ${msg}`);
    } finally {
      setCreating(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Delete agent
  // ---------------------------------------------------------------------------

  const deleteAgent = async (id: number) => {
    if (!confirm('Delete this agent? This will also remove it from ElevenLabs if synced.')) return;
    setDeletingId(id);
    try {
      await api.delete(`/calling/agents/${id}`);
      setAgents((prev) => prev.filter((a) => a.id !== id));
    } catch {
      alert('Failed to delete agent');
    } finally {
      setDeletingId(null);
    }
  };

  // ---------------------------------------------------------------------------
  // Edit agent
  // ---------------------------------------------------------------------------

  const openEdit = (agent: CallingAgent) => {
    setEditAgent(agent);
    setEditSystemPrompt(agent.system_prompt);
    setEditFirstMessage(agent.first_message ?? '');
    setEditError(null);
  };

  const saveEdit = async () => {
    if (!editAgent) return;
    setEditSaving(true);
    setEditError(null);
    try {
      const res = await api.patch<CallingAgent>(`/calling/agents/${editAgent.id}`, {
        system_prompt: editSystemPrompt,
        first_message: editFirstMessage,
      });
      setAgents((prev) => prev.map((a) => (a.id === editAgent.id ? res.data : a)));
      setEditAgent(null);
    } catch {
      setEditError('Failed to save changes');
    } finally {
      setEditSaving(false);
    }
  };

  // ---------------------------------------------------------------------------
  // Voice select handler
  // ---------------------------------------------------------------------------

  const handleVoiceChange = (id: string) => {
    setVoiceId(id);
    const found = voices.find((v) => v.voice_id === id);
    setVoiceName(found?.name ?? '');
  };

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------

  const inputCls = 'w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm bg-white dark:bg-gray-700 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500';
  const labelCls = 'block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1';

  return (
    <div className="space-y-6 max-w-5xl mx-auto">

      {/* Header row */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-gray-800 dark:text-gray-100">AI Calling Agents</h2>
          <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5">
            Create ElevenLabs voice agents with AI-generated scripts for automated outreach
          </p>
        </div>
        <button
          onClick={() => { setShowForm((f) => !f); setTimeout(() => formRef.current?.scrollIntoView({ behavior: 'smooth' }), 50); }}
          className="px-4 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 transition-colors"
        >
          {showForm ? 'Cancel' : '+ New Agent'}
        </button>
      </div>

      {/* Create form */}
      {showForm && (
        <div ref={formRef} className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 space-y-5 border border-gray-200 dark:border-gray-700">
          <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200 uppercase tracking-wider">New AI Calling Agent</h3>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {/* Agent name */}
            <div>
              <label className={labelCls}>Agent Name *</label>
              <input
                type="text"
                value={agentName}
                onChange={(e) => setAgentName(e.target.value)}
                placeholder="e.g. Margin Call Agent v1"
                className={inputCls}
              />
            </div>

            {/* Opportunity type */}
            <div>
              <label className={labelCls}>Opportunity Type *</label>
              <select
                value={opportunityType}
                onChange={(e) => setOpportunityType(e.target.value)}
                className={inputCls}
              >
                {OPPORTUNITY_TYPES.map((o) => (
                  <option key={o.value} value={o.value}>{o.label}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Description */}
          <div>
            <label className={labelCls}>Description / Context for AI Script Generation</label>
            <textarea
              value={description}
              onChange={(e) => setDescription(e.target.value)}
              rows={3}
              placeholder="Describe the situation — e.g. 'Client's margin level is below 100%, they risk automatic position closure. We need to urgently inform them and offer help.'"
              className={inputCls}
            />
          </div>

          {/* Voice picker */}
          <div>
            <label className={labelCls}>Voice {loadingVoices ? '(loading…)' : `(${voices.length} available)`}</label>
            {voices.length > 0 ? (
              <select
                value={voiceId}
                onChange={(e) => handleVoiceChange(e.target.value)}
                className={inputCls}
              >
                <option value="">— Select a voice —</option>
                {voices.map((v) => (
                  <option key={v.voice_id} value={v.voice_id}>{v.name}{v.category ? ` (${v.category})` : ''}</option>
                ))}
              </select>
            ) : (
              <div className="text-sm text-gray-500 dark:text-gray-400 italic py-2">
                {loadingVoices ? 'Loading voices…' : 'No voices found. Check ElevenLabs configuration.'}
              </div>
            )}
          </div>

          {/* Generate button */}
          <div className="flex items-center gap-3">
            <button
              onClick={generateScript}
              disabled={generating || !opportunityType}
              className="px-5 py-2 bg-indigo-600 text-white rounded-md text-sm font-medium hover:bg-indigo-700 disabled:opacity-50 transition-colors"
            >
              {generating ? (
                <span className="flex items-center gap-2">
                  <svg className="animate-spin w-4 h-4" viewBox="0 0 24 24" fill="none">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v8z" />
                  </svg>
                  Generating with Claude AI…
                </span>
              ) : '✦ Generate Script with AI'}
            </button>
            {genError && <span className="text-sm text-red-600 dark:text-red-400">{genError}</span>}
          </div>

          {/* Script preview / editor */}
          {(systemPrompt || generating) && (
            <div className="space-y-4 border-t border-gray-200 dark:border-gray-700 pt-4">
              <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Generated Script — review and edit before creating</p>

              <div>
                <label className={labelCls}>First Message (spoken first by the AI agent)</label>
                <input
                  type="text"
                  value={firstMessage}
                  onChange={(e) => setFirstMessage(e.target.value)}
                  className={inputCls}
                  placeholder="The opening line the agent will say…"
                />
              </div>

              <div>
                <label className={labelCls}>System Prompt (agent instructions)</label>
                <textarea
                  value={systemPrompt}
                  onChange={(e) => setSystemPrompt(e.target.value)}
                  rows={14}
                  className={`${inputCls} font-mono text-xs leading-relaxed`}
                />
              </div>

              {evalCriteria.length > 0 && (
                <div>
                  <label className={labelCls}>Evaluation Criteria (for call success scoring)</label>
                  <ul className="list-disc ml-5 space-y-1">
                    {evalCriteria.map((c, i) => (
                      <li key={i} className="text-sm text-gray-600 dark:text-gray-300">{c}</li>
                    ))}
                  </ul>
                </div>
              )}
            </div>
          )}

          {/* Create button */}
          {systemPrompt && (
            <div className="flex items-center gap-3 pt-2">
              <button
                onClick={createAgent}
                disabled={creating}
                className="px-6 py-2 bg-green-600 text-white rounded-md text-sm font-medium hover:bg-green-700 disabled:opacity-50 transition-colors"
              >
                {creating ? 'Creating…' : voiceId ? 'Create Agent on ElevenLabs' : 'Save Agent (no voice selected)'}
              </button>
              {createError && <span className="text-sm text-red-600 dark:text-red-400">{createError}</span>}
            </div>
          )}
        </div>
      )}

      {/* Agents list */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow overflow-hidden">
        <div className="px-5 py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-700/50 flex items-center justify-between">
          <span className="text-sm font-medium text-gray-700 dark:text-gray-300">
            {agents.length} agent{agents.length !== 1 ? 's' : ''}
          </span>
          <button
            onClick={loadAgents}
            disabled={loadingAgents}
            className="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200 transition-colors disabled:opacity-50"
          >
            {loadingAgents ? 'Loading…' : 'Refresh'}
          </button>
        </div>

        {agentsError ? (
          <div className="p-6 text-red-600 dark:text-red-400 text-sm">{agentsError}</div>
        ) : loadingAgents && agents.length === 0 ? (
          <div className="p-10 text-center text-gray-400 dark:text-gray-500 text-sm">Loading…</div>
        ) : agents.length === 0 ? (
          <div className="p-10 text-center text-gray-400 dark:text-gray-500 text-sm">
            No agents yet. Click <strong>+ New Agent</strong> to create your first AI calling agent.
          </div>
        ) : (
          <div className="divide-y divide-gray-100 dark:divide-gray-700">
            {agents.map((agent) => (
              <div key={agent.id}>
                {/* Agent row */}
                <div className="px-5 py-4 flex items-center gap-4 hover:bg-gray-50 dark:hover:bg-gray-700/40 transition-colors">
                  {/* Toggle */}
                  <button
                    onClick={() => setExpandedId((prev) => (prev === agent.id ? null : agent.id))}
                    className="w-6 h-6 flex-shrink-0 rounded border border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-700 text-xs font-bold flex items-center justify-center"
                  >
                    {expandedId === agent.id ? '−' : '+'}
                  </button>

                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-3 flex-wrap">
                      <span className="text-sm font-semibold text-gray-900 dark:text-gray-100">{agent.name}</span>
                      <StatusBadge status={agent.status} />
                      <span className="text-xs text-gray-500 dark:text-gray-400 bg-gray-100 dark:bg-gray-700 rounded px-2 py-0.5">{agent.opportunity_type}</span>
                      {agent.voice_name && (
                        <span className="text-xs text-purple-600 dark:text-purple-400">🎙 {agent.voice_name}</span>
                      )}
                    </div>
                    {agent.description && (
                      <p className="text-xs text-gray-500 dark:text-gray-400 mt-0.5 truncate">{agent.description}</p>
                    )}
                    {agent.elevenlabs_agent_id && (
                      <p className="text-xs font-mono text-gray-400 dark:text-gray-500 mt-0.5">EL: {agent.elevenlabs_agent_id}</p>
                    )}
                  </div>

                  <div className="flex items-center gap-2 flex-shrink-0">
                    <button
                      onClick={() => openEdit(agent)}
                      className="px-3 py-1 text-xs text-blue-600 dark:text-blue-400 border border-blue-200 dark:border-blue-700 rounded hover:bg-blue-50 dark:hover:bg-blue-900/30 transition-colors"
                    >
                      Edit Script
                    </button>
                    <button
                      onClick={() => deleteAgent(agent.id)}
                      disabled={deletingId === agent.id}
                      className="px-3 py-1 text-xs text-red-600 dark:text-red-400 border border-red-200 dark:border-red-700 rounded hover:bg-red-50 dark:hover:bg-red-900/30 disabled:opacity-50 transition-colors"
                    >
                      {deletingId === agent.id ? '…' : 'Delete'}
                    </button>
                  </div>
                </div>

                {/* Expanded: first message + system prompt */}
                {expandedId === agent.id && (
                  <div className="px-5 pb-4 bg-gray-50 dark:bg-gray-700/30 border-t border-gray-100 dark:border-gray-700 space-y-3">
                    {agent.first_message && (
                      <div>
                        <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">First Message</p>
                        <p className="text-sm text-gray-700 dark:text-gray-300 italic">"{agent.first_message}"</p>
                      </div>
                    )}
                    <div>
                      <p className="text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider mb-1">System Prompt</p>
                      <pre className="text-xs text-gray-600 dark:text-gray-300 whitespace-pre-wrap font-mono leading-relaxed max-h-64 overflow-y-auto bg-white dark:bg-gray-800 rounded p-3 border border-gray-200 dark:border-gray-700">
                        {agent.system_prompt}
                      </pre>
                    </div>
                  </div>
                )}
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Edit modal */}
      {editAgent && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="bg-white dark:bg-gray-800 rounded-xl shadow-2xl w-full max-w-2xl max-h-[90vh] flex flex-col">
            <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between">
              <h3 className="text-sm font-semibold text-gray-700 dark:text-gray-200">Edit Script — {editAgent.name}</h3>
              <button onClick={() => setEditAgent(null)} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xl leading-none">&times;</button>
            </div>
            <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
              <div>
                <label className={labelCls}>First Message</label>
                <input
                  type="text"
                  value={editFirstMessage}
                  onChange={(e) => setEditFirstMessage(e.target.value)}
                  className={inputCls}
                />
              </div>
              <div>
                <label className={labelCls}>System Prompt</label>
                <textarea
                  value={editSystemPrompt}
                  onChange={(e) => setEditSystemPrompt(e.target.value)}
                  rows={16}
                  className={`${inputCls} font-mono text-xs leading-relaxed`}
                />
              </div>
              {editError && <p className="text-sm text-red-600 dark:text-red-400">{editError}</p>}
            </div>
            <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700 flex items-center gap-3">
              <button
                onClick={saveEdit}
                disabled={editSaving}
                className="px-5 py-2 bg-blue-600 text-white rounded-md text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
              >
                {editSaving ? 'Saving…' : 'Save & Sync to ElevenLabs'}
              </button>
              <button
                onClick={() => setEditAgent(null)}
                className="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-800 dark:hover:text-gray-200 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

import { useEffect, useState, useRef, useCallback } from 'react';
import axios from 'axios';
import { useAuthStore } from '../store/useAuthStore';
import { useRetentionStatuses } from '../hooks/useRetentionStatuses';

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

interface QueueClient {
  accountid: string;
  full_name: string;
  score: number;
  total_balance: number;
  is_favorite: boolean;
  has_callback: boolean;
  callback_time: string | null;
  margin_level_pct: number | null;
}

interface Position {
  symbol: string;
  direction: 'Buy' | 'Sell';
  position_count: number;
  exposure_usd: number;
}

// CLAUD-155: Full per-trade open position detail
interface OpenPosition {
  ticket: number;
  symbol: string;
  side: 'Long' | 'Short';
  net_lot: number;
  contract_size: number | null;
  open_price: number;
  open_time: string | null;
  exposure: number;
  pnl: number;
  sl: number;
  tp: number;
  swap: number;
  commission: number;
}

// CLAUD-161: Live price map (symbol → current bid price)
type LivePriceMap = Record<string, number>;

// CLAUD-142: Dynamic lifecycle stage from lifecycle_stages table
interface LifecycleItem {
  id: number;
  name: string;
  key: string;
  reached: boolean;
}

interface CallbackInfo {
  id: number;
  callback_time: string | null;
  note: string | null;
}

interface ClientDetail {
  accountid: string;
  full_name: string;
  client_qualification_date: string | null;
  trade_count: number;
  total_profit: number;
  last_trade_date: string | null;
  deposit_count: number;
  total_deposit: number;
  balance: number;
  credit: number;
  equity: number;
  open_pnl: number;
  live_equity: number;
  margin: number;
  used_margin: number;
  free_margin: number;
  margin_level_pct: number | null;
  max_open_trade: number | null;
  max_volume: number | null;
  turnover: number | null;
  win_rate: number | null;
  avg_trade_size: number | null;
  assigned_to: string | null;
  agent_name: string | null;
  sales_client_potential: string | null;
  score: number;
  is_favorite: boolean;
  is_active: boolean;
  card_type: string | null;
  exposure_usd: number;
  exposure_pct: number | null;
  last_deposit_date: string | null;
  last_withdrawal_date: string | null;
  net_deposit_ever: number | null;
  country: string | null;
  last_communication_date: string | null;
  retention_status: string | null;
  lifecycle: LifecycleItem[];
  positions: Position[];
  mt_accounts: { login: number; balance: number; equity: number }[];
  callback: CallbackInfo | null;
}

interface PerformanceData {
  net_deposit: number;
  depositors: number;
  traders: number;
  volume: number;
  contacted: number;
  calls_made: number;
  talk_time_secs: number;
  target: number | null;
  callbacks_set: number;
  run_rate: number | null;
  contact_rate: number | null;
  avg_call_secs: number;
  computed_at: string | null;
  period: string;
}

interface ClosedPosition {
  symbol: string;
  direction: string;
  net_lot: number;
  exposure: number;
  pnl: number;
  entry_time: string | null;
  close_time: string | null;
}

interface Transaction {
  id: string;
  type: string;
  amount: number;
  usd_amount: number;
  method: string | null;
  status: string | null;
  date: string | null;
  card: string | null;
}

interface CallHistoryEntry {
  timestamp: string;
  agent: string;
  status_key: string | null;
  status_label: string | null;
  duration_sec: number | null;
  note: string | null;
}

interface CallbackRow {
  id: number;
  accountid: string;
  callback_time: string | null;
  note: string | null;
  created_at: string | null;
  full_name: string;
}

interface RetentionTask {
  id: number;
  task_type: string;
  due_date: string | null;
  status: string;
  note: string | null;
  color?: string;
}

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

// CLAUD-149: Retention statuses are now loaded dynamically from the backend (report.ant_ret_status)

// CLAUD-142: Lifecycle stages are now loaded dynamically from the backend (lifecycle_stages table)

// CLAUD-145: Country ISO => full name
const _ISO_TO_COUNTRY: Record<string, string> = {
  AE:'United Arab Emirates', AO:'Angola', AR:'Argentina', AU:'Australia', AW:'Aruba',
  AZ:'Azerbaijan', BD:'Bangladesh', BF:'Burkina Faso', BG:'Bulgaria', BH:'Bahrain',
  BR:'Brazil', BW:'Botswana', BY:'Belarus', BZ:'Belize', CA:'Canada',
  CD:'Democratic Republic of the Congo', CH:'Switzerland', CI:'Ivory Coast', CM:'Cameroon',
  CN:'China', CO:'Colombia', CY:'Cyprus', DE:'Germany', DK:'Denmark', DZ:'Algeria',
  EG:'Egypt', ET:'Ethiopia', FJ:'Fiji', GB:'United Kingdom', GH:'Ghana', GR:'Greece',
  GT:'Guatemala', GY:'Guyana', HK:'Hong Kong', HN:'Honduras', ID:'Indonesia',
  IE:'Ireland', IL:'Israel', IN:'India', IQ:'Iraq', IT:'Italy', JM:'Jamaica',
  JO:'Jordan', JP:'Japan', KE:'Kenya', KH:'Cambodia', KW:'Kuwait', LB:'Lebanon',
  LK:'Sri Lanka', LR:'Liberia', LS:'Lesotho', MD:'Moldova', MU:'Mauritius',
  MV:'Maldives', MX:'Mexico', MY:'Malaysia', MZ:'Mozambique', NA:'Namibia',
  NG:'Nigeria', NI:'Nicaragua', NL:'Netherlands', NO:'Norway', NZ:'New Zealand',
  OM:'Oman', PA:'Panama', PE:'Peru', PH:'Philippines', PK:'Pakistan', PL:'Poland',
  QA:'Qatar', RO:'Romania', RS:'Serbia', RW:'Rwanda', SA:'Saudi Arabia', SE:'Sweden',
  SG:'Singapore', SL:'Sierra Leone', SN:'Senegal', SS:'South Sudan', SV:'El Salvador',
  SY:'Syria', SZ:'Eswatini', TG:'Togo', TH:'Thailand', TR:'Turkey', TW:'Taiwan',
  TZ:'Tanzania', UA:'Ukraine', UG:'Uganda', US:'United States', UZ:'Uzbekistan',
  VG:'British Virgin Islands', VN:'Vietnam', ZA:'South Africa', ZM:'Zambia', ZW:'Zimbabwe',
};

// CLAUD-145: Country ISO => primary IANA timezone
const _ISO_TO_TZ: Record<string, string> = {
  AE:'Asia/Dubai', AO:'Africa/Luanda', AR:'America/Argentina/Buenos_Aires',
  AU:'Australia/Sydney', AW:'America/Aruba', AZ:'Asia/Baku', BD:'Asia/Dhaka',
  BF:'Africa/Ouagadougou', BG:'Europe/Sofia', BH:'Asia/Bahrain', BR:'America/Sao_Paulo',
  BW:'Africa/Gaborone', BY:'Europe/Minsk', BZ:'America/Belize', CA:'America/Toronto',
  CD:'Africa/Kinshasa', CH:'Europe/Zurich', CI:'Africa/Abidjan', CM:'Africa/Douala',
  CN:'Asia/Shanghai', CO:'America/Bogota', CY:'Asia/Nicosia', DE:'Europe/Berlin',
  DK:'Europe/Copenhagen', DZ:'Africa/Algiers', EG:'Africa/Cairo', ET:'Africa/Addis_Ababa',
  FJ:'Pacific/Fiji', GB:'Europe/London', GH:'Africa/Accra', GR:'Europe/Athens',
  GT:'America/Guatemala', GY:'America/Guyana', HK:'Asia/Hong_Kong', HN:'America/Tegucigalpa',
  ID:'Asia/Jakarta', IE:'Europe/Dublin', IL:'Asia/Jerusalem', IN:'Asia/Kolkata',
  IQ:'Asia/Baghdad', IT:'Europe/Rome', JM:'America/Jamaica', JO:'Asia/Amman',
  JP:'Asia/Tokyo', KE:'Africa/Nairobi', KH:'Asia/Phnom_Penh', KW:'Asia/Kuwait',
  LB:'Asia/Beirut', LK:'Asia/Colombo', LR:'Africa/Monrovia', LS:'Africa/Maseru',
  MD:'Europe/Chisinau', MU:'Indian/Mauritius', MV:'Indian/Maldives', MX:'America/Mexico_City',
  MY:'Asia/Kuala_Lumpur', MZ:'Africa/Maputo', NA:'Africa/Windhoek', NG:'Africa/Lagos',
  NI:'America/Managua', NL:'Europe/Amsterdam', NO:'Europe/Oslo', NZ:'Pacific/Auckland',
  OM:'Asia/Muscat', PA:'America/Panama', PE:'America/Lima', PH:'Asia/Manila',
  PK:'Asia/Karachi', PL:'Europe/Warsaw', QA:'Asia/Qatar', RO:'Europe/Bucharest',
  RS:'Europe/Belgrade', RW:'Africa/Kigali', SA:'Asia/Riyadh', SE:'Europe/Stockholm',
  SG:'Asia/Singapore', SL:'Africa/Freetown', SN:'Africa/Dakar', SS:'Africa/Juba',
  SV:'America/El_Salvador', SY:'Asia/Damascus', SZ:'Africa/Mbabane', TG:'Africa/Lome',
  TH:'Asia/Bangkok', TR:'Europe/Istanbul', TW:'Asia/Taipei', TZ:'Africa/Dar_es_Salaam',
  UA:'Europe/Kiev', UG:'Africa/Kampala', US:'America/New_York', UZ:'Asia/Tashkent',
  VG:'America/Tortola', VN:'Asia/Ho_Chi_Minh', ZA:'Africa/Johannesburg',
  ZM:'Africa/Lusaka', ZW:'Africa/Harare',
};

/** CLAUD-145: Format current time in a given IANA timezone as "03:45 PM (UTC+2)" */
function formatLocalTime(isoCode: string | null | undefined): string {
  if (!isoCode) return '--';
  const tz = _ISO_TO_TZ[isoCode.toUpperCase()];
  if (!tz) return '--';
  try {
    const now = new Date();
    const timeStr = now.toLocaleTimeString('en-US', {
      timeZone: tz, hour: '2-digit', minute: '2-digit', hour12: true,
    });
    // Compute UTC offset in hours
    const utcDate = new Date(now.toLocaleString('en-US', { timeZone: 'UTC' }));
    const tzDate = new Date(now.toLocaleString('en-US', { timeZone: tz }));
    const diffH = Math.round((tzDate.getTime() - utcDate.getTime()) / 3600000);
    const sign = diffH >= 0 ? '+' : '-';
    const offset = `UTC${sign}${Math.abs(diffH)}`;
    return `${timeStr} (${offset})`;
  } catch {
    return '--';
  }
}

/** CLAUD-146: Count working days (Mon-Fri) between two dates */
function workingDaysBetween(from: Date, to: Date): number {
  const a = new Date(from.getFullYear(), from.getMonth(), from.getDate());
  const b = new Date(to.getFullYear(), to.getMonth(), to.getDate());
  if (a >= b) return 0;
  const totalDays = Math.round((b.getTime() - a.getTime()) / 86400000);
  const weeks = Math.floor(totalDays / 7);
  const remaining = totalDays % 7;
  let workDays = weeks * 5;
  const fromDow = a.getDay();
  for (let i = 1; i <= remaining; i++) {
    const dow = (fromDow + i) % 7;
    if (dow !== 0 && dow !== 6) workDays++;
  }
  return workDays;
}


const CALLBACK_PRESETS: { key: string; label: string }[] = [
  { key: '15min', label: '15 minutes' },
  { key: '1hour', label: '1 hour' },
  { key: '2hours', label: '2 hours' },
  { key: 'tomorrow', label: 'Tomorrow same time' },
];

const QUICK_COMMENTS: string[] = [
  'Market update',
  'Account status update',
  'Deposit call',
  'Talk later',
];

const NO_ANSWER_STATUS_KEY = 3;
const NO_ANSWER_NOTE_TEXT = 'No answer';

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

function formatTime(totalSec: number): string {
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  const s = totalSec % 60;
  return `${String(h).padStart(2, '0')}:${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function formatMM_SS(totalSec: number): string {
  const m = Math.floor(totalSec / 60);
  const s = totalSec % 60;
  return `${String(m).padStart(2, '0')}:${String(s).padStart(2, '0')}`;
}

function formatTalkTime(totalSec: number): string {
  const h = Math.floor(totalSec / 3600);
  const m = Math.floor((totalSec % 3600) / 60);
  if (h > 0) return `${h}h ${m}m`;
  return `${m}m`;
}

function formatMoney(val: number | null | undefined): string {
  if (val == null) return '\u2014';
  return '$' + val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function formatMoneyCompact(val: number | null | undefined): string {
  if (val == null) return '\u2014';
  if (Math.abs(val) >= 1_000_000) return '$' + (val / 1_000_000).toFixed(1) + 'M';
  if (Math.abs(val) >= 1_000) return '$' + (val / 1_000).toFixed(1) + 'K';
  return '$' + val.toLocaleString(undefined, { minimumFractionDigits: 0, maximumFractionDigits: 0 });
}

function formatCallbackCountdown(iso: string): { text: string; color: string } {
  const d = new Date(iso);
  const now = new Date();
  const diffMs = d.getTime() - now.getTime();
  if (diffMs < 0) {
    const overMin = Math.abs(Math.floor(diffMs / 60000));
    return { text: `-${overMin}m`, color: 'text-red-600' };
  }
  const diffMin = Math.floor(diffMs / 60000);
  if (diffMin < 60) return { text: `${diffMin}m ${Math.floor((diffMs % 60000) / 1000)}s`, color: diffMin < 5 ? 'text-orange-600' : 'text-green-600' };
  const diffH = Math.floor(diffMin / 60);
  return { text: `${diffH}h ${diffMin % 60}m`, color: 'text-green-600' };
}

function friendlyError(err: any, fallback: string): string {
  const status = err?.response?.status;
  const detail = err?.response?.data?.detail;
  if (detail) return detail;
  if (status === 404) return 'Not found';
  if (status === 400) return 'Bad request';
  if (status === 502) return 'Service temporarily unavailable';
  if (status === 503) return 'Service temporarily unavailable';
  return fallback;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function CallDashboardPage() {
  const username = useAuthStore((s) => s.username);
  const userRole = useAuthStore((s) => s.role);
  const userPermissions = useAuthStore((s) => s.permissions);
  const canManageTasks = userPermissions.includes('retention-tasks');

  // CLAUD-149: Dynamic retention statuses from report.ant_ret_status
  const { statuses: retentionStatuses } = useRetentionStatuses();

  // Queue + client state
  const [queue, setQueue] = useState<QueueClient[]>([]);
  const [currentClient, setCurrentClient] = useState<ClientDetail | null>(null);
  const [loadingQueue, setLoadingQueue] = useState(false);
  const [loadingClient, setLoadingClient] = useState(false);
  const [queueIndex, setQueueIndex] = useState(0);
  // CLAUD-173: selected MT login for account filter chips
  const [selectedLogin, setSelectedLogin] = useState<number | null>(null);

  // Performance bar
  const [perfTab, setPerfTab] = useState<'daily' | 'monthly'>('daily');
  const [performance, setPerformance] = useState<PerformanceData | null>(null);

  // Tabbed data panel
  const [activeTab, setActiveTab] = useState<'open' | 'exposure' | 'closed' | 'transactions' | 'history'>('open');
  // CLAUD-155: Full per-trade open positions
  const [openPositions, setOpenPositions] = useState<OpenPosition[]>([]);
  const [loadingOpen, setLoadingOpen] = useState(false);
  // CLAUD-169: Manual refresh — track last fetch time, no more auto-polling
  const [openLastUpdated, setOpenLastUpdated] = useState<Date | null>(null);
  // CLAUD-161: Live current prices for open position symbols
  const [livePrices, setLivePrices] = useState<LivePriceMap>({});
  const [closedPositions, setClosedPositions] = useState<ClosedPosition[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loadingClosed, setLoadingClosed] = useState(false);
  const [loadingTxns, setLoadingTxns] = useState(false);
  const [closedLoaded, setClosedLoaded] = useState<string | null>(null);
  const [txnsLoaded, setTxnsLoaded] = useState<string | null>(null);
  const [callHistory, setCallHistory] = useState<CallHistoryEntry[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);
  const [historyLoaded, setHistoryLoaded] = useState<string | null>(null);

  // CLAUD-168: Column sort state for tabbed data panel
  const [tabSort, setTabSort] = useState<{ col: string | null; dir: 'asc' | 'desc' | null }>({ col: null, dir: null });

  // Callbacks
  const [callbacks, setCallbacks] = useState<CallbackRow[]>([]);
  const [dueCallbackNotifs, setDueCallbackNotifs] = useState<CallbackRow[]>([]);

  // Call controls state
  const [callStart, setCallStart] = useState<Date | null>(null);
  const [talkSeconds, setTalkSeconds] = useState(0);
  const [selectedStatus, setSelectedStatus] = useState<number | null>(null);
  // CLAUD-145: tick state to refresh the client local time every minute
  const [, setTimeTick] = useState(0);
  const [note, setNote] = useState('');
  const [noteValidationError, setNoteValidationError] = useState(false);
  const noteRef = useRef<HTMLTextAreaElement>(null);
  const noteAutoFilled = useRef(false);
  const [callbackPreset, setCallbackPreset] = useState<string | null>(null);
  const [callbackCustom, setCallbackCustom] = useState('');
  const [showCustomCallback, setShowCustomCallback] = useState(false);
  const [saving, setSaving] = useState(false);
  const [feedback, setFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Online/offline + shift
  const [onlineStatus, setOnlineStatus] = useState<'online' | 'offline'>(() =>
    (sessionStorage.getItem('dashOnline') || 'online') as 'online' | 'offline'
  );
  const [shiftStart] = useState(() => {
    const stored = sessionStorage.getItem('shiftStart');
    if (stored) return stored;
    const t = new Date().toISOString();
    sessionStorage.setItem('shiftStart', t);
    return t;
  });

  // Timers
  const [liveClock, setLiveClock] = useState('');
  const [shiftElapsed, setShiftElapsed] = useState('');

  // Inactivity alert
  const [inactivityAlert, setInactivityAlert] = useState(false);
  const inactivityTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Call action state
  const [callLoading, setCallLoading] = useState(false);
  const [waLoading, setWaLoading] = useState(false);

  // Retention tasks strip
  const [clientTasks, setClientTasks] = useState<RetentionTask[]>([]);
  const [tasksExpanded, setTasksExpanded] = useState(false);

  // ------- Clock & shift timer -------
  useEffect(() => {
    const interval = setInterval(() => {
      const now = new Date();
      setLiveClock(
        `${String(now.getHours()).padStart(2, '0')}:${String(now.getMinutes()).padStart(2, '0')}:${String(now.getSeconds()).padStart(2, '0')}`
      );
      const shiftSec = Math.floor((now.getTime() - new Date(shiftStart).getTime()) / 1000);
      setShiftElapsed(formatTime(Math.max(0, shiftSec)));
    }, 1000);
    return () => clearInterval(interval);
  }, [shiftStart]);

  // CLAUD-145: Tick every 60s to refresh client local time
  useEffect(() => {
    const interval = setInterval(() => setTimeTick((t) => t + 1), 60000);
    return () => clearInterval(interval);
  }, []);

  // ------- Talk timer -------
  useEffect(() => {
    if (!callStart || saving) return;
    const interval = setInterval(() => {
      setTalkSeconds(Math.floor((Date.now() - callStart.getTime()) / 1000));
    }, 1000);
    return () => clearInterval(interval);
  }, [callStart, saving]);

  // ------- Inactivity timer -------
  const resetInactivity = useCallback(() => {
    setInactivityAlert(false);
    if (inactivityTimer.current) clearTimeout(inactivityTimer.current);
    inactivityTimer.current = setTimeout(() => {
      setInactivityAlert(true);
    }, 15000);
  }, []);

  useEffect(() => {
    resetInactivity();
    return () => {
      if (inactivityTimer.current) clearTimeout(inactivityTimer.current);
    };
  }, [resetInactivity, queueIndex]);

  // ------- Load queue -------
  const loadQueue = useCallback(async () => {
    setLoadingQueue(true);
    try {
      const res = await api.get('/call-dashboard/queue');
      setQueue(res.data);
    } catch {
      // silent
    } finally {
      setLoadingQueue(false);
    }
  }, []);

  // ------- Load performance -------
  const [perfLastUpdated, setPerfLastUpdated] = useState<string | null>(null);

  const loadPerformance = useCallback(async (periodOverride?: 'daily' | 'monthly') => {
    const p = periodOverride ?? perfTab;
    try {
      const res = await api.get('/call-dashboard/performance', { params: { period: p } });
      setPerformance(res.data);
      setPerfLastUpdated(new Date().toLocaleTimeString());
    } catch {
      // silent
    }
  }, [perfTab]);

  // ------- Load callbacks -------
  const loadCallbacks = useCallback(async () => {
    try {
      const res = await api.get('/call-dashboard/callbacks');
      const data: CallbackRow[] = res.data;
      setCallbacks(data);
      // surface any callbacks that are now due (within 1 min window)
      const now = new Date();
      const due = data.filter((cb) => {
        if (!cb.callback_time) return false;
        const t = new Date(cb.callback_time);
        return t <= new Date(now.getTime() + 60_000);
      });
      if (due.length > 0) {
        setDueCallbackNotifs(due);
        // browser notification if permission granted
        if (Notification.permission === 'granted') {
          due.forEach((cb) =>
            new Notification('Callback Due', {
              body: `${cb.full_name} \u2014 ${cb.note || 'No note'}`,
              icon: '/favicon.ico',
            })
          );
        }
      }
    } catch {
      // silent
    }
  }, []);

  // Re-fetch when perfTab changes
  useEffect(() => {
    loadPerformance();
  }, [perfTab]); // eslint-disable-line react-hooks/exhaustive-deps

  // Poll callbacks + performance every 60 s.
  // Polling is paused when the tab is hidden to reduce server load for
  // 70 concurrent users (CLAUD-37).
  useEffect(() => {
    // Request browser notification permission once
    if (Notification.permission === 'default') Notification.requestPermission();
    loadQueue();
    loadPerformance();
    loadCallbacks();
    const callbackPoll = { current: setInterval(loadCallbacks, 60_000) };
    const perfPoll = { current: setInterval(() => loadPerformance(), 60_000) };

    const handleVisibility = () => {
      if (document.hidden) {
        clearInterval(callbackPoll.current);
        clearInterval(perfPoll.current);
      } else {
        loadCallbacks();
        loadPerformance();
        callbackPoll.current = setInterval(loadCallbacks, 60_000);
        perfPoll.current = setInterval(() => loadPerformance(), 60_000);
      }
    };
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      clearInterval(callbackPoll.current);
      clearInterval(perfPoll.current);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [loadQueue, loadPerformance, loadCallbacks]);

  // ------- Load client detail -------
  const loadClient = useCallback(async (accountId: string, loginFilter?: number | null) => {
    setLoadingClient(true);
    setFeedback(null);
    setActiveTab('open');
    setClosedLoaded(null);
    setTxnsLoaded(null);
    setHistoryLoaded(null);
    setOpenPositions([]);
    setClosedPositions([]);
    setTransactions([]);
    setCallHistory([]);
    if (!loginFilter) setSelectedLogin(null);
    try {
      const params = loginFilter ? `?login=${loginFilter}` : '';
      const res = await api.get(`/call-dashboard/client/${accountId}${params}`);
      setCurrentClient(res.data);
      // CLAUD-155: Load open positions immediately (default tab is 'open')
      loadOpenPositions(accountId, loginFilter ?? undefined);
    } catch (err: any) {
      setCurrentClient(null);
      setFeedback({ type: 'error', message: friendlyError(err, 'Failed to load client') });
    } finally {
      setLoadingClient(false);
    }
  }, []);

  // Auto-select from URL param or first in queue on load
  const urlClientId = useRef(new URLSearchParams(window.location.search).get('client_id'));
  const urlClientSelected = useRef(false);
  useEffect(() => {
    if (loadingClient) return;
    if (urlClientId.current && !urlClientSelected.current) {
      // Try to find the URL client in the queue first (to set queue position)
      if (queue.length > 0) {
        const idx = queue.findIndex((q) => q.accountid === urlClientId.current);
        if (idx >= 0) {
          urlClientSelected.current = true;
          setQueueIndex(idx);
          loadClient(queue[idx].accountid);
          return;
        }
      }
      // Client not in queue (or queue still loading) -- load directly by ID
      urlClientSelected.current = true;
      loadClient(urlClientId.current);
    } else if (!currentClient && queue.length > 0) {
      setQueueIndex(0);
      loadClient(queue[0].accountid);
    }
  }, [queue, currentClient, loadingClient, loadClient]);

  // Re-fetch lifecycle (and all client data) when the user's role changes so the
  // lifecycle bar reflects any role-gated data served by the backend.
  const prevUserRole = useRef(userRole);
  useEffect(() => {
    if (prevUserRole.current !== userRole) {
      prevUserRole.current = userRole;
      if (currentClient) {
        loadClient(currentClient.accountid);
      }
    }
  }, [userRole, currentClient, loadClient]);

  // Document title -- updates with active client name
  useEffect(() => {
    if (currentClient?.full_name) {
      document.title = `${currentClient.full_name} \u2014 Call Dashboard | CMTrading`;
    } else {
      document.title = 'Call Dashboard | CMTrading';
    }
    return () => { document.title = 'CMTrading'; };
  }, [currentClient]);

  // ------- Load closed positions (lazy) -------
  const loadClosedPositions = useCallback(async (accountId: string, loginFilter?: number) => {
    const cacheKey = loginFilter ? `${accountId}:${loginFilter}` : accountId;
    if (closedLoaded === cacheKey) return;
    setLoadingClosed(true);
    try {
      const params = loginFilter ? `?login=${loginFilter}` : '';
      const res = await api.get(`/call-dashboard/client/${accountId}/closed-positions${params}`);
      setClosedPositions(res.data);
      setClosedLoaded(cacheKey);
    } catch {
      setClosedPositions([]);
    } finally {
      setLoadingClosed(false);
    }
  }, [closedLoaded]);

  // ------- Load transactions (lazy) -------
  const loadTransactions = useCallback(async (accountId: string, loginFilter?: number) => {
    const cacheKey = loginFilter ? `${accountId}:${loginFilter}` : accountId;
    if (txnsLoaded === cacheKey) return;
    setLoadingTxns(true);
    try {
      const params = loginFilter ? `?login=${loginFilter}` : '';
      const res = await api.get(`/call-dashboard/client/${accountId}/transactions${params}`);
      setTransactions(res.data);
      setTxnsLoaded(cacheKey);
    } catch {
      setTransactions([]);
    } finally {
      setLoadingTxns(false);
    }
  }, [txnsLoaded]);

  // ------- CLAUD-155: Load open positions (live from Dealio) -------
  // CLAUD-161: Also fetch live prices for each unique symbol
  // CLAUD-169: No auto-polling — data fetched once on load and on manual refresh
  const loadOpenPositions = useCallback(async (accountId: string, loginFilter?: number) => {
    setLoadingOpen(true);
    try {
      const params = loginFilter ? `?login=${loginFilter}` : '';
      const res = await api.get(`/call-dashboard/client/${accountId}/open-positions${params}`);
      const positions: OpenPosition[] = res.data;
      setOpenPositions(positions);
      setOpenLastUpdated(new Date());
      // Fetch live prices for all unique symbols in one call
      const symbols = [...new Set(positions.map((p) => p.symbol).filter(Boolean))];
      if (symbols.length > 0) {
        try {
          const priceRes = await api.get(`/call-dashboard/live-prices?symbols=${symbols.join(',')}`);
          setLivePrices(priceRes.data ?? {});
        } catch {
          setLivePrices({});
        }
      } else {
        setLivePrices({});
      }
    } catch {
      setOpenPositions([]);
      setLivePrices({});
    } finally {
      setLoadingOpen(false);
    }
  }, []);

  // ------- Load call history (lazy) -------
  const loadCallHistory = useCallback(async (accountId: string) => {
    if (historyLoaded === accountId) return;
    setLoadingHistory(true);
    try {
      const res = await api.get(`/call-dashboard/client/${accountId}/call-history`);
      setCallHistory(res.data);
      setHistoryLoaded(accountId);
    } catch {
      setCallHistory([]);
    } finally {
      setLoadingHistory(false);
    }
  }, [historyLoaded]);

  // Handle tab clicks with lazy loading
  const handleTabClick = (tab: 'open' | 'exposure' | 'closed' | 'transactions' | 'history') => {
    setActiveTab(tab);
    setTabSort({ col: null, dir: null }); // CLAUD-168: reset sort on tab change
    if (tab === 'open' && currentClient) loadOpenPositions(currentClient.accountid, selectedLogin ?? undefined);
    if (tab === 'closed' && currentClient) loadClosedPositions(currentClient.accountid, selectedLogin ?? undefined);
    if (tab === 'transactions' && currentClient) loadTransactions(currentClient.accountid, selectedLogin ?? undefined);
    if (tab === 'history' && currentClient) loadCallHistory(currentClient.accountid);
  };

  // ------- Sync selectedStatus with client's current status on client change -------
  useEffect(() => {
    if (!currentClient) return;
    const match = retentionStatuses.find((s) => s.label === currentClient.sales_client_potential);
    setSelectedStatus(match?.key ?? null);
  }, [currentClient?.accountid, retentionStatuses]);

  // ------- Load retention tasks for current client -------
  useEffect(() => {
    if (!currentClient) {
      setClientTasks([]);
      return;
    }
    setTasksExpanded(false);
    api.get<RetentionTask[]>(`/retention/clients/${currentClient.accountid}/tasks`)
      .then((res) => setClientTasks(res.data))
      .catch(() => setClientTasks([]));
  }, [currentClient?.accountid]);

  // ------- Reset form -------
  const resetForm = useCallback(() => {
    setCallStart(null);
    setTalkSeconds(0);
    setSelectedStatus(null);
    setNote('');
    setNoteValidationError(false);
    noteAutoFilled.current = false;
    setCallbackPreset(null);
    setCallbackCustom('');
    setShowCustomCallback(false);
    setFeedback(null);
    setTabSort({ col: null, dir: null }); // CLAUD-168: reset sort on client change
  }, []);

  // ------- Online toggle -------
  const toggleOnline = () => {
    setOnlineStatus((prev) => {
      const next = prev === 'online' ? 'offline' : 'online';
      sessionStorage.setItem('dashOnline', next);
      return next;
    });
  };

  // ------- Favorite toggle -------
  const toggleFavorite = async () => {
    if (!currentClient) return;
    setCurrentClient((c) => c ? { ...c, is_favorite: !c.is_favorite } : c);
    try {
      await api.post(`/retention/favorites/${currentClient.accountid}`);
    } catch {
      setCurrentClient((c) => c ? { ...c, is_favorite: !c.is_favorite } : c);
    }
  };

  // ------- Call -------
  const handleCall = async () => {
    if (!currentClient) return;
    setCallLoading(true);
    try {
      await api.post(`/clients/${currentClient.accountid}/call`);
      setCallStart(new Date());
      setTalkSeconds(0);
    } catch (err: any) {
      setFeedback({ type: 'error', message: friendlyError(err, 'Failed to initiate call') });
    } finally {
      setCallLoading(false);
    }
  };

  // ------- WhatsApp -------
  const handleWhatsApp = async () => {
    if (!currentClient) return;
    setWaLoading(true);
    try {
      const res = await api.get(`/clients/${currentClient.accountid}/crm-user`, { params: { log_whatsapp: true } });
      const phone =
        res.data?.fullTelephone ||
        res.data?.telephone ||
        res.data?.phone ||
        res.data?.Phone ||
        res.data?.phoneNumber ||
        res.data?.PhoneNumber ||
        res.data?.mobile ||
        res.data?.Mobile;
      if (!phone) {
        setFeedback({ type: 'error', message: 'No phone number found for this client' });
        return;
      }
      const cleanPhone = String(phone).replace(/[^0-9+]/g, '').replace(/^\+/, '');
      window.open(`https://api.whatsapp.com/send?phone=${encodeURIComponent(cleanPhone)}`, '_blank');
    } catch (err: any) {
      setFeedback({ type: 'error', message: friendlyError(err, 'Failed to fetch client phone') });
    } finally {
      setWaLoading(false);
    }
  };

  // ------- Navigation -------
  const goToPrevious = () => {
    if (queueIndex <= 0 || queue.length === 0) return;
    const newIdx = queueIndex - 1;
    setQueueIndex(newIdx);
    resetForm();
    loadClient(queue[newIdx].accountid);
  };

  const goToNext = () => {
    if (queueIndex >= queue.length - 1 || queue.length === 0) return;
    const newIdx = queueIndex + 1;
    setQueueIndex(newIdx);
    resetForm();
    loadClient(queue[newIdx].accountid);
  };

  // ------- Delete callback -------
  const handleDeleteCallback = async (cbId: number) => {
    try {
      await api.patch(`/call-dashboard/callbacks/${cbId}/done`);
      setCallbacks((prev) => prev.filter((cb) => cb.id !== cbId));
    } catch {
      // silent
    }
  };

  // ------- Save & Next -------
  const handleSaveAndNext = async () => {
    if (!currentClient || selectedStatus === null) return;
    if (note.trim() === '') {
      setNoteValidationError(true);
      return;
    }
    setSaving(true);
    setFeedback(null);
    try {
      await api.post('/call-dashboard/save', {
        account_id: currentClient.accountid,
        status_key: selectedStatus,
        note: note.trim(),
        talk_seconds: talkSeconds,
        callback_preset: callbackPreset !== 'custom' ? callbackPreset : null,
        callback_custom_utc: callbackPreset === 'custom' && callbackCustom
          ? new Date(callbackCustom).toISOString()
          : null,
      });

      const savedAccountId = currentClient.accountid;
      resetForm();
      // Refresh queue, performance, callbacks and load next
      const [queueRes] = await Promise.all([
        api.get('/call-dashboard/queue'),
        loadPerformance(),
        loadCallbacks(),
      ]);
      const newQueue = queueRes.data;
      setQueue(newQueue);
      if (newQueue.length > 0) {
        // Find where the just-saved client sits in the refreshed queue, then advance past it
        const savedPos = newQueue.findIndex((c: any) => c.accountid === savedAccountId);
        const nextIdx = savedPos >= 0
          ? savedPos + 1  // advance past the saved client
          : queueIndex;   // client was removed from queue -- stay at same position
        if (nextIdx < newQueue.length) {
          setQueueIndex(nextIdx);
          loadClient(newQueue[nextIdx].accountid);
        } else {
          setCurrentClient(null);
          setQueueIndex(0);
          setFeedback({ type: 'success', message: 'Queue complete! All clients processed.' });
        }
      } else {
        setCurrentClient(null);
        setQueueIndex(0);
        setFeedback({ type: 'success', message: 'No more clients in queue' });
      }
    } catch (err: any) {
      setFeedback({ type: 'error', message: friendlyError(err, 'Failed to save') });
    } finally {
      setSaving(false);
    }
  };

  // ------- Compute exposure by symbol (for Level of Exposure tab) -------
  const exposureBySymbol = currentClient?.positions
    ? currentClient.positions.reduce<Record<string, { symbol: string; net_exposure: number; count: number }>>((acc, p) => {
        const key = p.symbol;
        if (!acc[key]) acc[key] = { symbol: key, net_exposure: 0, count: 0 };
        const sign = p.direction === 'Buy' ? 1 : -1;
        acc[key].net_exposure += sign * p.exposure_usd;
        acc[key].count += p.position_count;
        return acc;
      }, {})
    : {};

  const exposureListDefault = Object.values(exposureBySymbol).sort((a, b) => Math.abs(b.net_exposure) - Math.abs(a.net_exposure));

  // CLAUD-168: Generic sort helper for tab data
  function applySortToData<T extends Record<string, unknown>>(data: T[], col: string, dir: 'asc' | 'desc'): T[] {
    return [...data].sort((a, b) => {
      const av = a[col];
      const bv = b[col];
      if (av === null || av === undefined) return 1;
      if (bv === null || bv === undefined) return -1;
      let cmp = 0;
      if (typeof av === 'number' && typeof bv === 'number') {
        cmp = av - bv;
      } else if (typeof av === 'string' && typeof bv === 'string') {
        const da = Date.parse(av);
        const db = Date.parse(bv);
        if (!isNaN(da) && !isNaN(db)) {
          cmp = da - db;
        } else {
          cmp = av.localeCompare(bv);
        }
      }
      return dir === 'asc' ? cmp : -cmp;
    });
  }

  // CLAUD-168: Toggle sort column/direction (null → asc → desc → null)
  const toggleTabSort = (col: string) => {
    setTabSort((prev) => {
      if (prev.col !== col) return { col, dir: 'asc' };
      if (prev.dir === 'asc') return { col, dir: 'desc' };
      return { col: null, dir: null };
    });
  };

  // CLAUD-168: Sort indicator for a column header
  const sortIndicator = (col: string) => {
    if (tabSort.col !== col || tabSort.dir === null) return null;
    return tabSort.dir === 'asc' ? ' ↑' : ' ↓';
  };

  // Apply sort to each tab's data
  const sortedOpenPositions = (tabSort.col && tabSort.dir) ? applySortToData(openPositions as unknown as Record<string, unknown>[], tabSort.col, tabSort.dir) as unknown as OpenPosition[] : openPositions;
  const sortedExposureList = (tabSort.col && tabSort.dir) ? applySortToData(exposureListDefault as unknown as Record<string, unknown>[], tabSort.col, tabSort.dir) as unknown as typeof exposureListDefault : exposureListDefault;
  const sortedClosedPositions = (tabSort.col && tabSort.dir) ? applySortToData(closedPositions as unknown as Record<string, unknown>[], tabSort.col, tabSort.dir) as unknown as ClosedPosition[] : closedPositions;
  const sortedTransactions = (tabSort.col && tabSort.dir) ? applySortToData(transactions as unknown as Record<string, unknown>[], tabSort.col, tabSort.dir) as unknown as Transaction[] : transactions;
  const sortedCallHistory = (tabSort.col && tabSort.dir) ? applySortToData(callHistory as unknown as Record<string, unknown>[], tabSort.col, tabSort.dir) as unknown as CallHistoryEntry[] : callHistory;

  const exposureList = sortedExposureList;

  // ------- Colour helpers (inline, light mode) -------
  const scoreColorHex = (s: number) => s > 70 ? '#16a34a' : s >= 40 ? '#d97706' : '#dc2626';
  // CLAUD-172: Margin level colour: green ≥300%, orange 150-300%, red <150%
  const marginLevelColor = (pct: number | null | undefined): string => {
    if (pct == null) return '#94a3b8';
    if (pct >= 300) return '#16a34a';
    if (pct >= 150) return '#d97706';
    return '#dc2626';
  };
  const lastContactColorHex = (dateStr: string | null | undefined): string => {
    if (!dateStr) return '#94a3b8';
    const d = workingDaysBetween(new Date(dateStr), new Date());
    if (d === 0) return '#16a34a';
    if (d <= 3) return '#d97706';
    return '#dc2626';
  };

  // ------- Render -------
  return (
    <div
      style={{ background: '#f1f5f9', minHeight: '100vh', fontFamily: "'Plus Jakarta Sans', sans-serif" }}
      onMouseMove={resetInactivity}
      onKeyDown={resetInactivity}
    >
      {/* STICKY HEADER */}
      <div className="sticky top-0 z-50">
        <div style={{ background: '#ffffff', borderBottom: '1px solid #e2e8f0', height: 46, display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '0 16px', boxShadow: '0 1px 3px rgba(0,0,0,0.06)' }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
            <span style={{ color: '#0d9488', fontWeight: 800, fontSize: 12, letterSpacing: '0.12em', textTransform: 'uppercase' }}>Call Dashboard</span>
            <span style={{ color: '#64748b', fontSize: 12 }}>{username}</span>
            <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
              <div style={{ width: 7, height: 7, borderRadius: '50%', background: onlineStatus === 'online' ? '#16a34a' : '#94a3b8' }} />
              <button onClick={toggleOnline} style={{ fontSize: 11, color: onlineStatus === 'online' ? '#16a34a' : '#94a3b8', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
                {onlineStatus === 'online' ? 'Online' : 'Offline'}
              </button>
            </div>
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 20 }}>
            <div style={{ textAlign: 'right' }}>
              <div style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.08em' }}>Shift</div>
              <div style={{ fontFamily: "'JetBrains Mono', monospace", color: '#0f172a', fontSize: 13, fontWeight: 600 }}>{shiftElapsed}</div>
            </div>
            <div style={{ fontFamily: "'JetBrains Mono', monospace", color: '#0d9488', fontSize: 15, fontWeight: 600 }}>{liveClock}</div>
            {dueCallbackNotifs.length > 0 && (
              <div style={{ position: 'relative', cursor: 'pointer' }} onClick={() => setDueCallbackNotifs([])}>
                <span style={{ fontSize: 16 }}>🔔</span>
                <span style={{ position: 'absolute', top: -4, right: -4, background: '#dc2626', color: '#fff', fontSize: 9, fontWeight: 700, borderRadius: '50%', width: 14, height: 14, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                  {dueCallbackNotifs.length}
                </span>
              </div>
            )}
            {loadingQueue && <span style={{ color: '#94a3b8', fontSize: 10 }} className="animate-pulse">syncing…</span>}
          </div>
        </div>
        {dueCallbackNotifs.length > 0 && (
          <div style={{ background: '#fbbf24', color: '#1a1a1a', padding: '6px 16px', display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: 12 }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8, fontSize: 13, fontWeight: 600 }}>
              <span>🔔</span>
              <span>{dueCallbackNotifs.length === 1 ? `Callback due: ${dueCallbackNotifs[0].full_name}${dueCallbackNotifs[0].note ? ` — ${dueCallbackNotifs[0].note}` : ''}` : `${dueCallbackNotifs.length} callbacks are due now`}</span>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {dueCallbackNotifs.length === 1 && (
                <button onClick={() => { loadClient(dueCallbackNotifs[0].accountid); setDueCallbackNotifs([]); }} style={{ fontSize: 11, fontWeight: 700, padding: '3px 10px', borderRadius: 4, background: 'rgba(0,0,0,0.15)', border: 'none', cursor: 'pointer', color: '#1a1a1a' }}>Go to Client</button>
              )}
              <button onClick={() => setDueCallbackNotifs([])} style={{ fontSize: 16, background: 'none', border: 'none', cursor: 'pointer', lineHeight: 1, color: '#1a1a1a' }}>✕</button>
            </div>
          </div>
        )}
      </div>

      <div style={{ padding: 12, maxWidth: 1800, margin: '0 auto' }}>
        {/* PERFORMANCE BAR */}
        <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 14px', marginBottom: 12, boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
          <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 8 }}>
            <div style={{ background: '#f1f5f9', borderRadius: 20, padding: 2, display: 'flex' }}>
              {(['daily', 'monthly'] as const).map((t) => (
                <button key={t} onClick={() => setPerfTab(t)} style={{ padding: '4px 14px', borderRadius: 18, fontSize: 10, fontWeight: 700, letterSpacing: '0.08em', textTransform: 'uppercase', background: perfTab === t ? '#0d9488' : 'transparent', color: perfTab === t ? '#ffffff' : '#94a3b8', border: 'none', cursor: 'pointer' }}>{t}</button>
              ))}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              {perfLastUpdated && <span style={{ fontSize: 9, color: '#94a3b8', fontFamily: "'JetBrains Mono', monospace" }}>Updated {perfLastUpdated}</span>}
              <button onClick={() => loadPerformance()} style={{ color: '#94a3b8', background: 'none', border: 'none', cursor: 'pointer', display: 'flex' }} title="Refresh">
                <svg style={{ width: 12, height: 12 }} fill="none" stroke="currentColor" viewBox="0 0 24 24"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
              </button>
            </div>
          </div>
          <div style={{ overflowX: 'auto' }}>
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(12, minmax(0, 1fr))', gap: 6, minWidth: 560 }}>
              {[
                { label: 'Net Dep', value: performance?.net_deposit ? formatMoneyCompact(performance.net_deposit) : '—', color: '#16a34a', dot: '#16a34a' },
                { label: 'Depositors', value: String(performance?.depositors ?? '—'), color: '#0f172a', dot: '#16a34a' },
                { label: 'Traders', value: String(performance?.traders ?? '—'), color: '#0f172a', dot: '#16a34a' },
                { label: 'Volume', value: performance?.volume ? formatMoneyCompact(performance.volume) : '—', color: '#0f172a', dot: '#16a34a' },
                { label: 'Contacted', value: String(performance?.contacted ?? 0), color: '#0f172a', dot: '#3b82f6' },
                { label: 'Talk Time', value: formatTalkTime(performance?.talk_time_secs ?? 0), color: '#0f172a', dot: '#f97316' },
                { label: 'Target', value: performance?.target != null ? String(performance.target) : '—', color: '#0f172a', dot: '#dc2626' },
                { label: 'Run Rate', value: performance?.run_rate != null ? `${performance.run_rate}%` : '—', color: performance?.run_rate == null ? '#0f172a' : performance.run_rate >= 100 ? '#16a34a' : performance.run_rate >= 70 ? '#d97706' : '#dc2626', dot: '#dc2626' },
                { label: 'Calls', value: String(performance?.calls_made ?? 0), color: '#0f172a', dot: '#3b82f6' },
                { label: 'Contact%', value: performance?.contact_rate != null ? `${performance.contact_rate}%` : '—', color: '#0f172a', dot: '#dc2626' },
                { label: 'Callbacks', value: String(performance?.callbacks_set ?? 0), color: '#0f172a', dot: '#3b82f6' },
                { label: 'Avg Call', value: performance?.avg_call_secs ? formatMM_SS(performance.avg_call_secs) : '—', color: '#0f172a', dot: '#f97316' },
              ].map(({ label, value, color, dot }) => (
                <div key={label} style={{ textAlign: 'center' }}>
                  <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 3, marginBottom: 3 }}>
                    <span style={{ width: 4, height: 4, borderRadius: '50%', background: dot, display: 'inline-block', flexShrink: 0 }} />
                    <span style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em', fontFamily: "'JetBrains Mono', monospace" }}>{label}</span>
                  </div>
                  <span style={{ fontSize: 11, fontWeight: 700, color, fontFamily: "'JetBrains Mono', monospace" }}>{value}</span>
                </div>
              ))}
            </div>
          </div>
        </div>

        {/* QUEUE NAV */}
        <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 10 }}>
          <div>
            <span style={{ color: '#0f172a', fontSize: 13, fontWeight: 700 }}>Next Best Call</span>
            <span style={{ color: '#94a3b8', fontSize: 11, marginLeft: 8, fontFamily: "'JetBrains Mono', monospace" }}>{queue.length} remaining</span>
          </div>
          <div style={{ display: 'flex', gap: 6 }}>
            <button onClick={goToPrevious} disabled={queueIndex <= 0} style={{ padding: '5px 14px', fontSize: 11, border: '1px solid #e2e8f0', borderRadius: 6, color: queueIndex <= 0 ? '#cbd5e1' : '#64748b', background: '#ffffff', cursor: queueIndex <= 0 ? 'not-allowed' : 'pointer' }}>← Back</button>
            <button onClick={goToNext} disabled={queueIndex >= queue.length - 1} style={{ padding: '5px 14px', fontSize: 11, border: '1px solid #e2e8f0', borderRadius: 6, color: queueIndex >= queue.length - 1 ? '#cbd5e1' : '#64748b', background: '#ffffff', cursor: queueIndex >= queue.length - 1 ? 'not-allowed' : 'pointer' }}>Next →</button>
          </div>
        </div>

        {/* 3-COLUMN GRID */}
        <div style={{ display: 'grid', gridTemplateColumns: '286px 1fr 220px', gap: 8, alignItems: 'start' }}>

          {/* LEFT COLUMN */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {loadingClient && <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#94a3b8', fontSize: 12 }} className="animate-pulse">Loading client…</div>}
            {!loadingClient && !currentClient && <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#94a3b8', fontSize: 12 }}>{queue.length === 0 ? 'No clients available' : 'Select a client from the queue'}</div>}
            {!loadingClient && currentClient && (
              <>
                {/* CLIENT CARD */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: 14, position: 'relative', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <button onClick={toggleFavorite} style={{ position: 'absolute', top: 10, right: 10, fontSize: 17, background: 'none', border: 'none', cursor: 'pointer' }}>
                    {currentClient.is_favorite ? <span style={{ color: '#d97706' }}>★</span> : <span style={{ color: '#cbd5e1' }}>☆</span>}
                  </button>
                  <div style={{ marginBottom: 10, paddingRight: 24 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'wrap', marginBottom: 2 }}>
                      <span style={{ fontWeight: 700, fontSize: 15, color: '#0f172a' }}>{currentClient.full_name}</span>
                      {currentClient.lifecycle.some((s) => s.key === 'ftd' && s.reached) && (
                        <span style={{ background: 'rgba(59,130,246,0.1)', color: '#3b82f6', fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 10, border: '1px solid rgba(59,130,246,0.2)' }}>FTD</span>
                      )}
                      <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 6px', borderRadius: 10, background: currentClient.is_active ? 'rgba(22,163,74,0.1)' : '#f1f5f9', color: currentClient.is_active ? '#16a34a' : '#94a3b8', border: `1px solid ${currentClient.is_active ? 'rgba(22,163,74,0.2)' : '#e2e8f0'}` }}>
                        {currentClient.is_active ? 'Active' : 'Inactive'}
                      </span>
                    </div>
                    <div style={{ fontSize: 10, color: '#94a3b8', fontFamily: "'JetBrains Mono', monospace" }}>CL-{currentClient.accountid}</div>
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: 8, marginBottom: 10 }}>
                    <div style={{ background: '#f8fafc', borderRadius: 6, padding: '8px 10px', textAlign: 'center', border: '1px solid #e2e8f0' }}>
                      <div style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>Score</div>
                      <div style={{ fontSize: 22, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: scoreColorHex(currentClient.score) }}>{currentClient.score}</div>
                    </div>
                    <div style={{ background: '#f8fafc', borderRadius: 6, padding: '8px 10px', textAlign: 'center', border: '1px solid #e2e8f0' }}>
                      <div style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>Last Contact</div>
                      {currentClient.last_communication_date ? (
                        <div style={{ fontSize: 11, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: lastContactColorHex(currentClient.last_communication_date) }}>
                          {new Date(currentClient.last_communication_date).toLocaleDateString('en-GB')}
                        </div>
                      ) : (
                        <div style={{ fontSize: 12, color: '#94a3b8', fontFamily: "'JetBrains Mono', monospace" }}>—</div>
                      )}
                    </div>
                    <div style={{ background: '#f8fafc', borderRadius: 6, padding: '8px 10px', textAlign: 'center', border: '1px solid #e2e8f0' }}>
                      <div style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 2 }}>Margin Lvl</div>
                      <div style={{ fontSize: 13, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: marginLevelColor(currentClient.margin_level_pct) }}>
                        {currentClient.margin_level_pct != null ? `${currentClient.margin_level_pct.toLocaleString('en-US', { maximumFractionDigits: 0 })}%` : '—'}
                      </div>
                    </div>
                  </div>
                  {currentClient.country && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 10, marginBottom: 8, flexWrap: 'wrap' }}>
                      <span style={{ fontSize: 11, color: '#64748b' }}>🌍 {_ISO_TO_COUNTRY[currentClient.country.toUpperCase()] ?? currentClient.country}</span>
                      <span style={{ fontSize: 11, color: '#0d9488', fontFamily: "'JetBrains Mono', monospace" }}>🕐 {formatLocalTime(currentClient.country)}</span>
                    </div>
                  )}
                  {currentClient.retention_status && (
                    <div style={{ display: 'inline-flex', alignItems: 'center', background: 'rgba(59,130,246,0.08)', border: '1px solid rgba(59,130,246,0.2)', borderRadius: 6, padding: '4px 10px', marginBottom: 6 }}>
                      <span style={{ fontSize: 10, color: '#3b82f6', fontWeight: 600 }}>{currentClient.retention_status}</span>
                    </div>
                  )}
                  {currentClient.agent_name && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 4 }}>
                      <div style={{ width: 22, height: 22, borderRadius: '50%', background: '#e0f2fe', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 9, fontWeight: 700, color: '#0891b2', flexShrink: 0 }}>
                        {currentClient.agent_name.split(' ').map((w: string) => w[0]).join('').slice(0, 2).toUpperCase()}
                      </div>
                      <span style={{ fontSize: 11, color: '#64748b' }}>{currentClient.agent_name}</span>
                    </div>
                  )}
                  {/* CLAUD-177: MT account numbers in empty space below agent row */}
                  {currentClient.mt_accounts && currentClient.mt_accounts.length > 0 && (
                    <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginTop: 4, flexWrap: 'wrap' }}>
                      <div style={{ width: 22, height: 22, borderRadius: '50%', background: '#f0fdf4', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 8, fontWeight: 700, color: '#16a34a', flexShrink: 0 }}>
                        MT
                      </div>
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
                        {currentClient.mt_accounts.map((acct) => (
                          <span key={acct.login} style={{ fontSize: 10, fontFamily: "'JetBrains Mono', monospace", color: '#475569', background: '#f1f5f9', border: '1px solid #e2e8f0', borderRadius: 4, padding: '1px 6px' }}>
                            {acct.login}
                          </span>
                        ))}
                      </div>
                    </div>
                  )}
                </div>

                {/* CLAUD-173: MT Account Selector — only show when client has multiple accounts */}
                {currentClient.mt_accounts && currentClient.mt_accounts.length > 1 && (
                  <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '10px 14px', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                    <div style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 7 }}>MT Accounts</div>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>
                      {currentClient.mt_accounts.map((acct) => {
                        const isSelected = selectedLogin === acct.login;
                        return (
                          <button
                            key={acct.login}
                            onClick={() => {
                              const next = isSelected ? null : acct.login;
                              setSelectedLogin(next);
                              setClosedLoaded(null);
                              setTxnsLoaded(null);
                              loadClient(currentClient.accountid, next);
                            }}
                            style={{
                              padding: '3px 10px',
                              borderRadius: 20,
                              fontSize: 11,
                              fontWeight: 600,
                              fontFamily: "'JetBrains Mono', monospace",
                              cursor: 'pointer',
                              border: isSelected ? '1.5px solid #0d9488' : '1.5px solid #e2e8f0',
                              background: isSelected ? '#0d9488' : '#f8fafc',
                              color: isSelected ? '#ffffff' : '#64748b',
                              transition: 'all 0.15s',
                            }}
                          >
                            {acct.login}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* FINANCIAL SNAPSHOT */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: 14, boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <div style={{ fontSize: 8, color: '#0d9488', textTransform: 'uppercase', letterSpacing: '0.1em', fontWeight: 700, marginBottom: 10, display: 'flex', alignItems: 'center', gap: 6 }}>
                    <span style={{ width: 5, height: 5, borderRadius: '50%', background: '#16a34a', display: 'inline-block' }} className="animate-pulse" />
                    {selectedLogin ? `Account ${selectedLogin} — LIVE` : 'Financial Snapshot — LIVE'}
                  </div>
                  <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px 14px' }}>
                    {[
                      { label: 'Balance', value: formatMoney(currentClient.balance), color: '#0f172a' },
                      { label: 'Credit', value: formatMoney(currentClient.credit), color: '#0891b2' },
                      { label: 'Equity', value: formatMoney(currentClient.equity), color: '#0f172a' },
                      { label: 'Live Equity', value: formatMoney(Math.abs(currentClient.live_equity)), color: '#16a34a' },
                      { label: 'WD Equity', value: formatMoney(currentClient.live_equity - currentClient.credit), color: (currentClient.live_equity - currentClient.credit) < 0 ? '#dc2626' : '#16a34a' },
                      { label: 'Open P&L', value: formatMoney(currentClient.open_pnl), color: currentClient.open_pnl >= 0 ? '#16a34a' : '#dc2626' },
                      { label: 'Closed P&L', value: formatMoney(currentClient.total_profit), color: (currentClient.total_profit ?? 0) >= 0 ? '#16a34a' : '#dc2626' },
                      { label: 'Exposure USD', value: formatMoney(currentClient.exposure_usd), color: '#0f172a' },
                      { label: 'Exposure %', value: currentClient.exposure_pct != null ? `${currentClient.exposure_pct.toFixed(1)}%` : '—', color: '#0f172a' },
                      { label: 'Margin Level', value: currentClient.margin_level_pct != null ? `${currentClient.margin_level_pct.toLocaleString('en-US', { maximumFractionDigits: 1 })}%` : '—', color: marginLevelColor(currentClient.margin_level_pct) },
                      { label: 'Used Margin', value: formatMoney(currentClient.used_margin), color: '#0f172a' },
                      { label: 'Free Margin', value: formatMoney(currentClient.free_margin), color: currentClient.free_margin < 0 ? '#dc2626' : '#0f172a' },
                      { label: 'Max Volume', value: currentClient.max_volume != null ? currentClient.max_volume.toLocaleString() : '—', color: '#0f172a' },
                      { label: 'Max Open', value: String(currentClient.max_open_trade ?? '—'), color: '#0f172a' },
                      { label: 'Net Dep Ever', value: formatMoney(currentClient.net_deposit_ever), color: currentClient.net_deposit_ever != null && currentClient.net_deposit_ever < 0 ? '#dc2626' : '#0f172a' },
                      { label: 'Last Trade', value: currentClient.last_trade_date ? new Date(currentClient.last_trade_date).toLocaleDateString('en-GB') : '—', color: '#64748b' },
                      { label: 'Last Deposit', value: currentClient.last_deposit_date ? new Date(currentClient.last_deposit_date).toLocaleDateString('en-GB') : '—', color: '#64748b' },
                      { label: 'Last Withdraw', value: currentClient.last_withdrawal_date ? new Date(currentClient.last_withdrawal_date).toLocaleDateString('en-GB') : '—', color: '#64748b' },
                      { label: 'Assigned', value: currentClient.agent_name ?? '—', color: '#64748b' },
                    ].map(({ label, value, color }) => (
                      <div key={label} style={{ borderBottom: '1px solid #f1f5f9', paddingBottom: 5 }}>
                        <div style={{ fontSize: 8, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.05em' }}>{label}</div>
                        <div style={{ fontSize: 12, fontWeight: 600, color, fontFamily: "'JetBrains Mono', monospace", marginTop: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{value}</div>
                      </div>
                    ))}
                  </div>
                </div>
              </>
            )}
          </div>

          {/* CENTRE COLUMN */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8, minWidth: 0 }}>
            {currentClient && (
              <>
                {/* CLIENT LIFECYCLE */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <div style={{ background: 'linear-gradient(135deg, #0d9488, #0891b2)', padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ color: '#ffffff', fontSize: 12 }}>🔄</span>
                      <span style={{ color: '#ffffff', fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em' }}>Client Lifecycle</span>
                      <span style={{ background: 'rgba(255,255,255,0.2)', color: '#ffffff', fontSize: 9, fontWeight: 700, padding: '1px 6px', borderRadius: 10 }}>
                        {currentClient.lifecycle.filter((s) => s.reached).length}/{currentClient.lifecycle.length}
                      </span>
                    </div>
                    <span style={{ color: 'rgba(255,255,255,0.8)', fontSize: 10 }}>
                      {currentClient.lifecycle.find((_s, i) => i === currentClient.lifecycle.findIndex((x) => !x.reached))?.name ?? 'Complete'}
                    </span>
                  </div>
                  <div style={{ padding: '14px 16px', overflowX: 'auto' }}>
                    <div style={{ display: 'flex', alignItems: 'flex-end', minWidth: 'max-content' }}>
                      {currentClient.lifecycle.map((stage, index) => {
                        const firstUnreachedIdx = currentClient.lifecycle.findIndex((s) => !s.reached);
                        const isCurrent = index === firstUnreachedIdx;
                        return (
                          <div key={stage.key} style={{ display: 'flex', alignItems: 'center' }}>
                            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center' }}>
                              {isCurrent && <span style={{ fontSize: 7, color: '#0d9488', fontWeight: 700, textTransform: 'uppercase', marginBottom: 2 }}>NOW</span>}
                              <span style={{ fontSize: 9, marginBottom: 6, whiteSpace: 'nowrap', color: isCurrent ? '#0d9488' : stage.reached ? '#94a3b8' : '#cbd5e1', fontWeight: isCurrent ? 700 : 400 }}>{stage.name}</span>
                              {stage.reached ? (
                                <div style={{ width: 24, height: 24, borderRadius: '50%', background: '#0d9488', display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#ffffff', fontSize: 12, fontWeight: 700 }}>✓</div>
                              ) : isCurrent ? (
                                <div style={{ width: 24, height: 24, borderRadius: '50%', border: '2px solid #0d9488', boxShadow: '0 0 8px rgba(13,148,136,0.4)', display: 'flex', alignItems: 'center', justifyContent: 'center' }} className="animate-pulse">
                                  <div style={{ width: 8, height: 8, borderRadius: '50%', background: '#0d9488' }} />
                                </div>
                              ) : (
                                <div style={{ width: 24, height: 24, borderRadius: '50%', border: '2px solid #e2e8f0', background: '#f8fafc' }} />
                              )}
                            </div>
                            {index < currentClient.lifecycle.length - 1 && (
                              <div style={{ width: 28, height: 2, background: stage.reached ? '#0d9488' : '#e2e8f0', margin: '0 2px', marginBottom: 2 }} />
                            )}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                </div>

                {/* RETENTION TASKS */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <div style={{ background: 'linear-gradient(135deg, #0d9488, #0891b2)', padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                      <span style={{ color: '#ffffff', fontSize: 12 }}>✅</span>
                      <span style={{ color: '#ffffff', fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em' }}>Retention Tasks</span>
                      {clientTasks.length > 0 && <span style={{ background: 'rgba(255,255,255,0.2)', color: '#ffffff', fontSize: 9, fontWeight: 700, padding: '1px 6px', borderRadius: 10 }}>{clientTasks.length}</span>}
                    </div>
                    {canManageTasks && (
                      <a href="/retention-tasks" style={{ fontSize: 10, color: 'rgba(255,255,255,0.85)', border: '1px solid rgba(255,255,255,0.3)', borderRadius: 4, padding: '2px 8px', textDecoration: 'none' }}>+ Add</a>
                    )}
                  </div>
                  <div style={{ padding: '10px 14px' }}>
                    {clientTasks.length === 0 ? (
                      <span style={{ fontSize: 11, color: '#94a3b8', fontStyle: 'italic' }}>No active tasks</span>
                    ) : (
                      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                        {(tasksExpanded ? clientTasks : clientTasks.slice(0, 5)).map((task) => {
                          const now = new Date();
                          const due = task.due_date ? new Date(task.due_date) : null;
                          const isOverdue = due && due < now;
                          const isToday = due && due.toDateString() === now.toDateString();
                          const urgencyColor = isOverdue ? '#dc2626' : isToday ? '#d97706' : '#0d9488';
                          const urgencyBg = isOverdue ? 'rgba(220,38,38,0.08)' : isToday ? 'rgba(217,119,6,0.08)' : 'rgba(13,148,136,0.08)';
                          return (
                            <div key={task.id} style={{ display: 'flex', alignItems: 'center', gap: 5, padding: '4px 8px', borderRadius: 6, border: `1px solid ${urgencyColor}40`, background: urgencyBg }}>
                              <span style={{ fontSize: 10, fontWeight: 600, color: urgencyColor }}>{task.task_type}</span>
                              {task.due_date && <span style={{ fontSize: 9, color: urgencyColor, opacity: 0.8 }}>· {new Date(task.due_date).toLocaleDateString('en-GB')}</span>}
                              {isOverdue && <span style={{ fontSize: 8, fontWeight: 700, color: '#dc2626', textTransform: 'uppercase' }}>OVERDUE</span>}
                              {isToday && !isOverdue && <span style={{ fontSize: 8, fontWeight: 700, color: '#d97706', textTransform: 'uppercase' }}>TODAY</span>}
                            </div>
                          );
                        })}
                        {!tasksExpanded && clientTasks.length > 5 && (
                          <button onClick={() => setTasksExpanded(true)} style={{ fontSize: 10, color: '#0d9488', background: 'none', border: 'none', cursor: 'pointer' }}>+{clientTasks.length - 5} more</button>
                        )}
                      </div>
                    )}
                  </div>
                </div>

                {/* CALL CONTROLS — CLAUD-150: compact 2-row layout */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: '9px 10px', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>

                  {/* Inactivity alert */}
                  {inactivityAlert && (
                    <div style={{ marginBottom: 8, padding: '6px 10px', borderRadius: 6, background: 'rgba(220,38,38,0.06)', border: '1px solid rgba(220,38,38,0.2)' }}>
                      <span style={{ color: '#dc2626', fontSize: 11, fontWeight: 600 }} className="animate-pulse">⚠ No call activity detected — initiate a call now.</span>
                    </div>
                  )}

                  {/* ROW 1: Call + WhatsApp | divider | STATUS | divider | Callback pills */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, flexWrap: 'nowrap' }}>
                    {/* Call button */}
                    <button onClick={handleCall} disabled={callLoading} style={{ height: 32, padding: '0 12px', fontSize: 12, fontWeight: 700, background: '#0f172a', color: '#ffffff', border: 'none', borderRadius: 6, cursor: callLoading ? 'not-allowed' : 'pointer', opacity: callLoading ? 0.5 : 1, whiteSpace: 'nowrap', flexShrink: 0 }}>
                      📞 {callLoading ? 'Calling…' : 'Call'}
                    </button>
                    {/* WhatsApp button */}
                    <button onClick={handleWhatsApp} disabled={waLoading} style={{ height: 32, padding: '0 10px', fontSize: 12, fontWeight: 700, background: '#dcfce7', color: '#16a34a', border: '1px solid rgba(22,163,74,0.3)', borderRadius: 6, cursor: waLoading ? 'not-allowed' : 'pointer', opacity: waLoading ? 0.5 : 1, whiteSpace: 'nowrap', flexShrink: 0 }}>
                      💬 {waLoading ? '…' : 'WhatsApp'}
                    </button>
                    {/* Live timer (when call active) */}
                    {callStart && (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 4, flexShrink: 0 }}>
                        <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#dc2626', display: 'inline-block' }} className="animate-pulse" />
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", color: '#dc2626', fontSize: 12, fontWeight: 700 }}>{formatMM_SS(talkSeconds)}</span>
                      </div>
                    )}

                    {/* Divider */}
                    <div style={{ width: 1, height: 28, background: '#e2e8f0', flexShrink: 0 }} />

                    {/* STATUS label + select */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 5, flexShrink: 0 }}>
                      <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.07em', fontWeight: 700, whiteSpace: 'nowrap' }}>
                        STATUS{currentClient?.retention_status ? `: ${currentClient.retention_status}` : ''}
                      </span>
                      <select value={selectedStatus ?? ''} onChange={(e) => {
                        const newKey = e.target.value ? Number(e.target.value) : null;
                        setSelectedStatus(newKey);
                        if (newKey === NO_ANSWER_STATUS_KEY) { setNote(NO_ANSWER_NOTE_TEXT); setNoteValidationError(false); noteAutoFilled.current = true; }
                        else if (noteAutoFilled.current && note === NO_ANSWER_NOTE_TEXT) { setNote(''); noteAutoFilled.current = false; }
                      }} style={{ height: 28, border: '1px solid #fde68a', borderRadius: 5, padding: '0 6px', fontSize: 11, background: '#fffbeb', color: '#b45309', outline: 'none', cursor: 'pointer', minWidth: 120 }}>
                        <option value="">-- Select status --</option>
                        {retentionStatuses.map((s) => (<option key={s.key} value={s.key}>{s.label}</option>))}
                      </select>
                    </div>

                    {/* Divider */}
                    <div style={{ width: 1, height: 28, background: '#e2e8f0', flexShrink: 0 }} />

                    {/* Quick Callback pills */}
                    <div style={{ display: 'flex', alignItems: 'center', gap: 4, flex: 1, flexWrap: 'wrap', minWidth: 0 }}>
                      <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.07em', fontWeight: 700, whiteSpace: 'nowrap', flexShrink: 0 }}>Callback</span>
                      {CALLBACK_PRESETS.map((p) => (
                        <button key={p.key} onClick={() => { setCallbackPreset(callbackPreset === p.key ? null : p.key); setShowCustomCallback(false); }}
                          style={{ height: 24, padding: '0 8px', fontSize: 10, fontWeight: 600, borderRadius: 12, border: `1px solid ${callbackPreset === p.key ? '#0d9488' : '#e2e8f0'}`, background: callbackPreset === p.key ? 'rgba(13,148,136,0.08)' : '#f8fafc', color: callbackPreset === p.key ? '#0d9488' : '#64748b', cursor: 'pointer', whiteSpace: 'nowrap' }}>
                          {p.key === '15min' ? '15m' : p.key === '1hour' ? '1hr' : p.key === '2hours' ? '2hr' : 'Tomorrow'}
                        </button>
                      ))}
                      <button onClick={() => { setShowCustomCallback(!showCustomCallback); setCallbackPreset(showCustomCallback ? null : 'custom'); }}
                        style={{ height: 24, padding: '0 8px', fontSize: 10, fontWeight: 600, borderRadius: 12, border: `1px solid ${showCustomCallback ? '#0d9488' : '#e2e8f0'}`, background: showCustomCallback ? 'rgba(13,148,136,0.08)' : '#f8fafc', color: showCustomCallback ? '#0d9488' : '#64748b', cursor: 'pointer', whiteSpace: 'nowrap' }}>
                        📅 Other
                      </button>
                    </div>
                  </div>

                  {/* Custom callback date picker (shown when "Other" selected) */}
                  {showCustomCallback && (
                    <input type="datetime-local" value={callbackCustom} onChange={(e) => setCallbackCustom(e.target.value)}
                      style={{ marginTop: 6, width: '100%', borderRadius: 6, padding: '5px 10px', fontSize: 11, background: '#ffffff', color: '#0f172a', border: '1px solid #e2e8f0', outline: 'none', boxSizing: 'border-box' }} />
                  )}
                  {/* Scheduled callback confirmation */}
                  {callbackPreset && callbackPreset !== 'custom' && (
                    <div style={{ marginTop: 5, padding: '5px 10px', background: 'rgba(13,148,136,0.06)', border: '1px solid rgba(13,148,136,0.2)', borderRadius: 6 }}>
                      <span style={{ fontSize: 10, color: '#0d9488', fontFamily: "'JetBrains Mono', monospace" }}>
                        📅 {(() => {
                          const now = new Date();
                          let target: Date | null = null;
                          if (callbackPreset === '15min') target = new Date(now.getTime() + 15 * 60000);
                          if (callbackPreset === '1hour') target = new Date(now.getTime() + 60 * 60000);
                          if (callbackPreset === '2hours') target = new Date(now.getTime() + 2 * 60 * 60000);
                          if (callbackPreset === 'tomorrow') { target = new Date(now); target.setDate(target.getDate() + 1); }
                          return target?.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true }) ?? '';
                        })()}
                      </span>
                    </div>
                  )}

                  {/* Thin separator */}
                  <div style={{ height: 1, background: '#f1f5f9', margin: '8px 0' }} />

                  {/* ROW 2a: Notes label + quick-fill pills inline */}
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6, marginBottom: 5, flexWrap: 'wrap' }}>
                    <span style={{ fontSize: 9, color: '#94a3b8', textTransform: 'uppercase', letterSpacing: '0.07em', fontWeight: 700, whiteSpace: 'nowrap' }}>Notes *</span>
                    {QUICK_COMMENTS.map((qc) => (
                      <button key={qc} type="button" onClick={() => { setNote((prev) => prev.trim() === '' ? qc : `${prev} ${qc}`); noteAutoFilled.current = false; setNoteValidationError(false); setTimeout(() => noteRef.current?.focus(), 0); }}
                        style={{ height: 22, padding: '0 8px', fontSize: 10, borderRadius: 10, background: '#f8fafc', color: '#64748b', border: '1px solid #e2e8f0', cursor: 'pointer', whiteSpace: 'nowrap' }}>
                        {qc}
                      </button>
                    ))}
                  </div>

                  {/* ROW 2b: textarea + SAVE & NEXT side by side */}
                  <div style={{ display: 'flex', gap: 8, alignItems: 'flex-start' }}>
                    <div style={{ flex: 1, minWidth: 0 }}>
                      <textarea ref={noteRef} value={note} onChange={(e) => { setNote(e.target.value); noteAutoFilled.current = false; if (e.target.value.trim() !== '') setNoteValidationError(false); }}
                        placeholder="Add call notes…" rows={2}
                        style={{ width: '100%', height: 52, borderRadius: 6, padding: '7px 10px', fontSize: 12, background: '#ffffff', color: '#0f172a', border: `1px solid ${noteValidationError ? '#dc2626' : note.trim() ? '#0d9488' : '#e2e8f0'}`, outline: 'none', resize: 'none', boxSizing: 'border-box', fontFamily: "'JetBrains Mono', monospace" }} />
                      {noteValidationError && <p style={{ marginTop: 2, fontSize: 10, color: '#dc2626' }}>Please add a note before saving</p>}
                    </div>
                    <button onClick={handleSaveAndNext} disabled={saving || selectedStatus === null || note.trim() === ''}
                      style={{ height: 52, padding: '0 14px', fontSize: 11, fontWeight: 800, letterSpacing: '0.05em', textTransform: 'uppercase', background: (saving || selectedStatus === null || note.trim() === '') ? '#f1f5f9' : '#0d9488', color: (saving || selectedStatus === null || note.trim() === '') ? '#cbd5e1' : '#ffffff', border: `1px solid ${(saving || selectedStatus === null || note.trim() === '') ? '#e2e8f0' : '#0d9488'}`, borderRadius: 6, cursor: (saving || selectedStatus === null || note.trim() === '') ? 'not-allowed' : 'pointer', flexShrink: 0, whiteSpace: 'nowrap' }}>
                      {saving ? 'Saving…' : 'Save & Next →'}
                    </button>
                  </div>

                  {/* Feedback message */}
                  {feedback && (
                    <div style={{ marginTop: 8, padding: '7px 10px', borderRadius: 6, fontSize: 11, background: feedback.type === 'success' ? 'rgba(22,163,74,0.06)' : 'rgba(220,38,38,0.06)', color: feedback.type === 'success' ? '#16a34a' : '#dc2626', border: `1px solid ${feedback.type === 'success' ? 'rgba(22,163,74,0.2)' : 'rgba(220,38,38,0.2)'}` }}>
                      {feedback.message}
                    </div>
                  )}
                </div>

                {/* TABBED DATA PANEL */}
                <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
                  <div style={{ borderBottom: '1px solid #e2e8f0', display: 'flex', alignItems: 'center', overflowX: 'auto' }}>
                    <div style={{ display: 'flex', flex: 1 }}>
                    {[
                      { key: 'open' as const, label: 'Open Positions', count: openPositions.length },
                      { key: 'exposure' as const, label: 'Exposure', count: exposureList.length },
                      { key: 'closed' as const, label: 'Closed', count: closedPositions.length },
                      { key: 'transactions' as const, label: 'Transactions', count: transactions.length },
                      { key: 'history' as const, label: 'Call History', count: callHistory.length },
                    ].map((tab) => (
                      <button key={tab.key} onClick={() => handleTabClick(tab.key)} style={{ flex: 1, padding: '9px 4px', fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.05em', background: activeTab === tab.key ? '#f8fafc' : 'transparent', color: activeTab === tab.key ? '#0d9488' : '#94a3b8', border: 'none', borderBottom: `2px solid ${activeTab === tab.key ? '#0d9488' : 'transparent'}`, cursor: 'pointer', whiteSpace: 'nowrap' }}>
                        {tab.label} <span style={{ fontSize: 9, color: activeTab === tab.key ? '#0d9488' : '#cbd5e1' }}>{tab.count}</span>
                      </button>
                    ))}
                    </div>
                    {/* CLAUD-169: Manual refresh button for live data tabs */}
                    {(activeTab === 'open' || activeTab === 'exposure') && currentClient && (
                      <div style={{ display: 'flex', alignItems: 'center', gap: 6, padding: '0 10px', flexShrink: 0 }}>
                        {openLastUpdated && (
                          <span style={{ fontSize: 9, color: '#94a3b8', whiteSpace: 'nowrap' }}>
                            {openLastUpdated.toLocaleTimeString()}
                          </span>
                        )}
                        <button
                          onClick={() => loadOpenPositions(currentClient.accountid, selectedLogin ?? undefined)}
                          disabled={loadingOpen}
                          title="Refresh live data"
                          style={{ background: 'none', border: '1px solid #e2e8f0', borderRadius: 4, padding: '3px 7px', cursor: loadingOpen ? 'not-allowed' : 'pointer', color: loadingOpen ? '#cbd5e1' : '#0d9488', fontSize: 11, display: 'flex', alignItems: 'center', gap: 3 }}
                        >
                          {loadingOpen ? '⟳' : '↺'} Refresh
                        </button>
                      </div>
                    )}
                  </div>
                  <div style={{ maxHeight: 220, overflowY: 'auto', padding: '8px 12px' }}>
                    {activeTab === 'open' && (loadingOpen ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }} className="animate-pulse">Loading…</div>
                    ) : openPositions.length === 0 ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }}>No open positions</div>
                    ) : (
                      <table style={{ width: '100%', fontSize: 11, borderCollapse: 'collapse' }}>
                        <thead>
                          <tr style={{ background: '#f8fafc', borderBottom: '1px solid #f1f5f9' }}>
                            {([
                              { label: 'Ticket', col: 'ticket', align: 'left' },
                              { label: 'Symbol', col: 'symbol', align: 'left' },
                              { label: 'L/S', col: 'side', align: 'left' },
                              { label: 'Net Lot', col: 'net_lot', align: 'right' },
                              { label: 'Open Price', col: 'open_price', align: 'right' },
                              { label: 'Current Price', col: null, align: 'right' },
                              { label: 'Exposure', col: 'exposure', align: 'right' },
                              { label: 'P&L', col: 'pnl', align: 'right' },
                              { label: 'S/L', col: 'sl', align: 'right' },
                              { label: 'T/P', col: 'tp', align: 'right' },
                              { label: 'Open Time', col: 'open_time', align: 'left' },
                            ] as { label: string; col: string | null; align: string }[]).map(({ label, col, align }) => (
                              <th key={label} onClick={col ? () => toggleTabSort(col) : undefined}
                                style={{ padding: '7px 12px', fontSize: 9.5, textTransform: 'uppercase', color: col && tabSort.col === col ? '#0d9488' : '#94a3b8', textAlign: align as 'left' | 'right', fontWeight: 600, whiteSpace: 'nowrap', cursor: col ? 'pointer' : 'default', userSelect: 'none' }}>
                                {label}{col ? sortIndicator(col) : ''}
                              </th>
                            ))}
                          </tr>
                        </thead>
                        <tbody>
                          {sortedOpenPositions.map((p) => {
                            const currentPrice = livePrices[p.symbol] ?? null;
                            const currentPriceColor = currentPrice === null
                              ? '#64748b'
                              : currentPrice > p.open_price
                                ? (p.side === 'Long' ? '#16a34a' : '#dc2626')
                                : currentPrice < p.open_price
                                  ? (p.side === 'Long' ? '#dc2626' : '#16a34a')
                                  : '#475569';
                            return (
                            <tr key={p.ticket} style={{ borderBottom: '1px solid #f1f5f9' }}>
                              <td style={{ padding: '9px 12px', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.ticket}</td>
                              <td style={{ padding: '9px 12px', fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: '#0f172a' }}>{p.symbol}</td>
                              <td style={{ padding: '9px 12px' }}>
                                <span style={{ fontSize: 9, fontWeight: 700, padding: '2px 8px', borderRadius: 4, background: p.side === 'Long' ? '#f0fdf4' : '#fef2f2', color: p.side === 'Long' ? '#16a34a' : '#dc2626', border: `1px solid ${p.side === 'Long' ? '#bbf7d0' : '#fecaca'}` }}>{p.side}</span>
                              </td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.net_lot > 0 && p.net_lot < 0.01 ? p.net_lot.toFixed(3) : p.net_lot.toFixed(2)}</td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.open_price > 0 ? p.open_price.toFixed(2) : '—'}</td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: currentPriceColor }}>
                                {currentPrice !== null ? currentPrice.toFixed(2) : '—'}
                              </td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', fontFamily: "'JetBrains Mono', monospace" }}>{formatMoneyCompact(p.exposure)}</td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', fontFamily: "'JetBrains Mono', monospace", fontWeight: 600, color: p.pnl > 0 ? '#16a34a' : p.pnl < 0 ? '#dc2626' : '#64748b' }}>
                                {p.pnl !== 0 ? `${p.pnl > 0 ? '▲' : '▼'} $${Math.abs(p.pnl).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
                              </td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.sl > 0 ? p.sl : '—'}</td>
                              <td style={{ padding: '9px 12px', textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.tp > 0 ? p.tp : '—'}</td>
                              <td style={{ padding: '9px 12px', color: '#64748b', whiteSpace: 'nowrap' }}>{p.open_time ?? '—'}</td>
                            </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    ))}
                    {activeTab === 'exposure' && (exposureList.length === 0 ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }}>No exposure data</div>
                    ) : (
                      <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                        <thead><tr style={{ color: '#94a3b8', fontSize: 9, textTransform: 'uppercase' }}>
                          {([
                            { label: 'Symbol', col: 'symbol', align: 'left' },
                            { label: 'Positions', col: 'count', align: 'right' },
                            { label: 'Net Exposure', col: 'net_exposure', align: 'right' },
                            { label: 'Direction', col: null, align: 'left' },
                          ] as { label: string; col: string | null; align: string }[]).map(({ label, col, align }) => (
                            <th key={label} onClick={col ? () => toggleTabSort(col) : undefined}
                              style={{ textAlign: align as 'left' | 'right', paddingBottom: 6, paddingLeft: label === 'Direction' ? 8 : undefined, cursor: col ? 'pointer' : 'default', color: col && tabSort.col === col ? '#0d9488' : undefined, userSelect: 'none' }}>
                              {label}{col ? sortIndicator(col) : ''}
                            </th>
                          ))}
                        </tr></thead>
                        <tbody>
                          {exposureList.map((e) => (
                            <tr key={e.symbol} style={{ borderTop: '1px solid #f1f5f9', height: 32 }}>
                              <td style={{ color: '#0f172a', fontFamily: "'JetBrains Mono', monospace" }}>{e.symbol}</td>
                              <td style={{ textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{e.count}</td>
                              <td style={{ textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{formatMoney(Math.abs(e.net_exposure))}</td>
                              <td style={{ paddingLeft: 8 }}><span style={{ fontSize: 10, fontWeight: 700, padding: '2px 5px', borderRadius: 3, background: e.net_exposure >= 0 ? 'rgba(22,163,74,0.1)' : 'rgba(220,38,38,0.1)', color: e.net_exposure >= 0 ? '#16a34a' : '#dc2626' }}>{e.net_exposure >= 0 ? 'NET LONG' : 'NET SHORT'}</span></td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ))}
                    {activeTab === 'closed' && (loadingClosed ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }} className="animate-pulse">Loading…</div>
                    ) : closedPositions.length === 0 ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }}>No closed positions</div>
                    ) : (
                      <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                        <thead><tr style={{ color: '#94a3b8', fontSize: 9, textTransform: 'uppercase' }}>
                          {([
                            { label: 'Symbol', col: 'symbol', align: 'left' },
                            { label: 'Dir', col: 'direction', align: 'left' },
                            { label: 'Lots', col: 'net_lot', align: 'right' },
                            { label: 'P&L', col: 'pnl', align: 'right' },
                            { label: 'Close', col: 'close_time', align: 'right' },
                          ] as { label: string; col: string; align: string }[]).map(({ label, col, align }) => (
                            <th key={label} onClick={() => toggleTabSort(col)}
                              style={{ textAlign: align as 'left' | 'right', paddingBottom: 6, cursor: 'pointer', color: tabSort.col === col ? '#0d9488' : undefined, userSelect: 'none' }}>
                              {label}{sortIndicator(col)}
                            </th>
                          ))}
                        </tr></thead>
                        <tbody>
                          {sortedClosedPositions.map((p, i) => (
                            <tr key={i} style={{ borderTop: '1px solid #f1f5f9', height: 32 }}>
                              <td style={{ color: '#0f172a', fontFamily: "'JetBrains Mono', monospace" }}>{p.symbol}</td>
                              <td><span style={{ fontSize: 10, fontWeight: 700, padding: '2px 5px', borderRadius: 3, background: p.direction === 'Long' ? 'rgba(22,163,74,0.1)' : 'rgba(220,38,38,0.1)', color: p.direction === 'Long' ? '#16a34a' : '#dc2626' }}>{p.direction}</span></td>
                              <td style={{ textAlign: 'right', color: '#64748b', fontFamily: "'JetBrains Mono', monospace" }}>{p.net_lot > 0 && p.net_lot < 0.01 ? p.net_lot.toFixed(3) : p.net_lot.toFixed(2)}</td>
                              <td style={{ textAlign: 'right', fontWeight: 600, fontFamily: "'JetBrains Mono', monospace", color: p.pnl >= 0 ? '#16a34a' : '#dc2626' }}>{p.pnl >= 0 ? '↑' : '↓'} {formatMoney(Math.abs(p.pnl))}</td>
                              <td style={{ textAlign: 'right', color: '#94a3b8', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>{p.close_time ? new Date(p.close_time).toLocaleDateString() : '—'}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    ))}
                    {activeTab === 'transactions' && (loadingTxns ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }} className="animate-pulse">Loading…</div>
                    ) : transactions.length === 0 ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }}>No successful transactions</div>
                    ) : (
                      <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                        <thead><tr style={{ color: '#94a3b8', fontSize: 9, textTransform: 'uppercase' }}>
                          {([
                            { label: 'Date', col: 'date', align: 'left' },
                            { label: 'Time', col: null, align: 'center' },
                            { label: 'Type', col: 'type', align: 'center' },
                            { label: 'Amount', col: 'usd_amount', align: 'right' },
                            { label: 'Transaction ID', col: 'id', align: 'left' },
                          ] as { label: string; col: string | null; align: string }[]).map(({ label, col, align }) => (
                            <th key={label} onClick={col ? () => toggleTabSort(col) : undefined}
                              style={{ textAlign: align as 'left' | 'right' | 'center', paddingBottom: 6, cursor: col ? 'pointer' : 'default', color: col && tabSort.col === col ? '#0d9488' : undefined, userSelect: 'none' }}>
                              {label}{col ? sortIndicator(col) : ''}
                            </th>
                          ))}
                        </tr></thead>
                        <tbody>
                          {sortedTransactions.map((t, i) => {
                            const isDeposit = t.type === 'Deposit';
                            const d = t.date ? new Date(t.date) : null;
                            const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                            const dateStr = d ? `${String(d.getDate()).padStart(2,'0')} ${MONTHS[d.getMonth()]} ${d.getFullYear()}` : '—';
                            const timeStr = d ? `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}` : '—';
                            const txnId = String(t.id || '—');
                            return (
                              <tr key={i} style={{ borderTop: '1px solid #f1f5f9', height: 34 }}>
                                <td style={{ fontWeight: 600, fontSize: 12, color: '#334155' }}>{dateStr}</td>
                                <td style={{ textAlign: 'center', fontFamily: "'JetBrains Mono', monospace", fontSize: 11, color: '#64748b' }}>{timeStr}</td>
                                <td style={{ textAlign: 'center' }}><span style={{ fontSize: 10, fontWeight: 700, padding: '2px 8px', borderRadius: 4, background: isDeposit ? '#f0fdfa' : '#fff7ed', border: `1px solid ${isDeposit ? '#99f6e4' : '#fed7aa'}`, color: isDeposit ? '#0d9488' : '#ea580c' }}>{t.type}</span></td>
                                <td style={{ textAlign: 'right', fontFamily: "'JetBrains Mono', monospace", fontWeight: 700, fontSize: 13, color: isDeposit ? '#16a34a' : '#dc2626' }}>
                                  {isDeposit ? '+' : '-'}{formatMoney(Math.abs(t.usd_amount))}
                                </td>
                                <td>
                                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                                    <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#64748b', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: 180 }}>{txnId}</span>
                                    <button onClick={() => navigator.clipboard.writeText(txnId)} title="Copy ID" style={{ border: 'none', background: 'none', cursor: 'pointer', color: '#94a3b8', fontSize: 10, padding: '0 2px' }}>⎘</button>
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    ))}
                    {activeTab === 'history' && (loadingHistory ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }} className="animate-pulse">Loading…</div>
                    ) : callHistory.length === 0 ? (
                      <div style={{ textAlign: 'center', color: '#94a3b8', fontSize: 12, padding: '20px 0' }}>No call history</div>
                    ) : (
                      <table style={{ width: '100%', fontSize: 12, borderCollapse: 'collapse' }}>
                        <thead><tr style={{ color: '#94a3b8', fontSize: 9, textTransform: 'uppercase' }}>
                          {([
                            { label: 'Date', col: 'timestamp', align: 'left' },
                            { label: 'Time', col: null, align: 'center' },
                            { label: 'Agent', col: 'agent', align: 'left' },
                            { label: 'Status', col: 'status_label', align: 'left' },
                            { label: 'Duration', col: 'duration_sec', align: 'center' },
                            { label: 'Notes', col: 'note', align: 'left' },
                          ] as { label: string; col: string | null; align: string }[]).map(({ label, col, align }) => (
                            <th key={label} onClick={col ? () => toggleTabSort(col) : undefined}
                              style={{ textAlign: align as 'left' | 'right' | 'center', paddingBottom: 6, cursor: col ? 'pointer' : 'default', color: col && tabSort.col === col ? '#0d9488' : undefined, userSelect: 'none' }}>
                              {label}{col ? sortIndicator(col) : ''}
                            </th>
                          ))}
                        </tr></thead>
                        <tbody>
                          {sortedCallHistory.map((h, i) => {
                            const d = new Date(h.timestamp);
                            const MONTHS = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
                            const dateStr = `${String(d.getDate()).padStart(2,'0')} ${MONTHS[d.getMonth()]} ${d.getFullYear()}`;
                            const timeStr = `${String(d.getHours()).padStart(2,'0')}:${String(d.getMinutes()).padStart(2,'0')}`;
                            const statusCfg: Record<string, { bg: string; border: string; color: string }> = {
                              '1':  { bg: '#fff7ed', border: '#fed7aa', color: '#ea580c' },
                              '3':  { bg: '#f8fafc', border: '#e2e8f0', color: '#64748b' },
                              '6':  { bg: '#fef2f2', border: '#fecaca', color: '#dc2626' },
                              '23': { bg: '#f0fdfa', border: '#99f6e4', color: '#0d9488' },
                            };
                            const sc = statusCfg[h.status_key ?? ''] ?? { bg: 'rgba(22,163,74,0.1)', border: '#bbf7d0', color: '#16a34a' };
                            return (
                              <tr key={i} style={{ borderTop: '1px solid #f1f5f9' }}>
                                <td style={{ padding: '8px 4px', fontWeight: 600, fontSize: 11, color: '#334155', whiteSpace: 'nowrap' }}>{dateStr}</td>
                                <td style={{ padding: '8px 4px', textAlign: 'center', fontFamily: "'JetBrains Mono', monospace", fontSize: 10, color: '#64748b' }}>{timeStr}</td>
                                <td style={{ padding: '8px 4px', color: '#0f172a', fontSize: 11, fontWeight: 600, whiteSpace: 'nowrap' }}>{h.agent}</td>
                                <td style={{ padding: '8px 4px' }}>
                                  {h.status_label ? (
                                    <span style={{ fontSize: 10, fontWeight: 600, padding: '2px 6px', borderRadius: 4, background: sc.bg, border: `1px solid ${sc.border}`, color: sc.color }}>{h.status_label}</span>
                                  ) : <span style={{ color: '#94a3b8' }}>—</span>}
                                </td>
                                <td style={{ padding: '8px 4px', textAlign: 'center', fontFamily: "'JetBrains Mono', monospace", color: '#475569', fontSize: 11 }}>
                                  {h.duration_sec != null ? formatMM_SS(h.duration_sec) : '—'}
                                </td>
                                <td style={{ padding: '8px 4px', maxWidth: 220 }}>
                                  <div style={{ fontSize: 11, color: h.note ? '#475569' : '#cbd5e1', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', cursor: h.note ? 'pointer' : 'default' }}
                                    title={h.note ?? undefined}
                                    onClick={e => { const el = e.currentTarget; el.style.whiteSpace = el.style.whiteSpace === 'normal' ? 'nowrap' : 'normal'; }}>
                                    {h.note ?? '—'}
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    ))}
                  </div>
                </div>
              </>
            )}
          </div>

          {/* RIGHT COLUMN — CALLBACKS ONLY */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, overflow: 'hidden', boxShadow: '0 1px 2px rgba(0,0,0,0.04)' }}>
              <div style={{ background: 'linear-gradient(135deg, #0d9488, #0891b2)', padding: '10px 14px', display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                  <span style={{ color: '#ffffff', fontSize: 12 }}>📅</span>
                  <span style={{ color: '#ffffff', fontSize: 10, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.1em' }}>Callbacks</span>
                  {callbacks.length > 0 && <span style={{ background: 'rgba(255,255,255,0.2)', color: '#ffffff', fontSize: 9, fontWeight: 700, padding: '1px 6px', borderRadius: 10 }}>{callbacks.length}</span>}
                </div>
                <span style={{ color: 'rgba(255,255,255,0.7)', fontSize: 9 }}>nearest first</span>
              </div>
              {callbacks.length === 0 ? (
                <div style={{ fontSize: 11, color: '#94a3b8', padding: '20px 14px', textAlign: 'center' }}>No upcoming callbacks</div>
              ) : (
                <div style={{ maxHeight: 500, overflowY: 'auto' }}>
                  {callbacks.map((cb) => {
                    const countdown = cb.callback_time ? formatCallbackCountdown(cb.callback_time) : { text: '—', color: '' };
                    const initials = cb.full_name.split(' ').map((w) => w[0]).join('').slice(0, 2).toUpperCase();
                    const isUrgent = countdown.color === 'text-red-600' || countdown.color === 'text-orange-600';
                    return (
                      <div key={cb.id} style={{ padding: '10px 14px', borderBottom: '1px solid #f1f5f9' }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 6 }}>
                          <div style={{ width: 28, height: 28, borderRadius: '50%', background: '#e0f2fe', display: 'flex', alignItems: 'center', justifyContent: 'center', fontSize: 10, fontWeight: 700, color: '#0891b2', flexShrink: 0 }}>
                            {initials}
                          </div>
                          <div style={{ minWidth: 0, flex: 1 }}>
                            <div style={{ fontSize: 11, color: '#0f172a', fontWeight: 600, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>{cb.full_name}</div>
                            <div style={{ fontSize: 9, color: '#94a3b8', fontFamily: "'JetBrains Mono', monospace" }}>CL-{cb.accountid}</div>
                          </div>
                          <div style={{ fontSize: 11, fontWeight: 700, fontFamily: "'JetBrains Mono', monospace", color: isUrgent ? '#dc2626' : '#d97706', flexShrink: 0 }}>{countdown.text}</div>
                        </div>
                        <div style={{ display: 'flex', gap: 5 }}>
                          <button
                            onClick={() => { const idx = queue.findIndex((q) => q.accountid === cb.accountid); if (idx >= 0) { setQueueIndex(idx); resetForm(); loadClient(queue[idx].accountid); } else { loadClient(cb.accountid); } }}
                            style={{ flex: 1, height: 26, fontSize: 10, fontWeight: 600, borderRadius: 5, border: '1px solid #0d9488', background: 'transparent', color: '#0d9488', cursor: 'pointer' }}>
                            📞 Call Now
                          </button>
                          <button
                            onClick={(e) => { e.stopPropagation(); handleDeleteCallback(cb.id); }}
                            style={{ width: 26, height: 26, display: 'flex', alignItems: 'center', justifyContent: 'center', borderRadius: 5, border: '1px solid #e2e8f0', background: 'transparent', cursor: 'pointer', color: '#94a3b8', fontSize: 12 }}
                            title="Delete">🗑️</button>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
            {!currentClient && (
              <div style={{ background: '#ffffff', border: '1px solid #e2e8f0', borderRadius: 8, padding: 40, textAlign: 'center', color: '#94a3b8', fontSize: 12 }}>No client selected</div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

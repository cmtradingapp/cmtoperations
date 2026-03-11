import { useEffect, useState, useRef, useCallback, useMemo, ReactNode } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
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


const PAGE_SIZE = 50;
type SortCol = 'accountid' | 'full_name' | 'client_qualification_date' | 'days_in_retention' | 'trade_count' | 'last_trade_date' | 'days_from_last_trade' | 'deposit_count' | 'total_deposit' | 'balance' | 'credit' | 'equity' | 'open_pnl' | 'closed_pnl' | 'live_equity' | 'max_open_trade' | 'max_volume' | 'turnover' | 'avg_trade_size' | 'retention_status' | 'age' | 'agent_name' | 'score' | 'exposure_usd' | 'exposure_pct' | 'margin_level_pct' | 'used_margin' | 'free_margin' | 'last_communication_date' | 'country' | 'sales_client_potential';
type NumOp = '' | 'eq' | 'gt' | 'gte' | 'lt' | 'lte';
type BoolFilter = '' | 'true' | 'false';

// ── Per-column header filters ──────────────────────────────────────────────
type ColNumOp = 'gt' | 'lt' | 'eq' | 'gte' | 'lte' | 'between';
type ColDatePreset = 'today' | 'this_week' | 'this_month' | 'custom';

type ColFilter =
  | { type: 'text'; value: string }
  | { type: 'numeric'; op: ColNumOp; val: string; val2?: string }
  | { type: 'date'; preset?: ColDatePreset; from?: string; to?: string }
  | { type: 'multiselect'; values: string[] };

type ColFilters = Partial<Record<string, ColFilter>>;

interface TaskInfo {
  name: string;
  color: string;
}

interface RetentionClient {
  accountid: string;
  full_name: string;
  client_qualification_date: string | null;
  days_in_retention: number | null;
  trade_count: number;
  total_profit: number;
  last_trade_date: string | null;
  days_from_last_trade: number | null;
  active: boolean;
  active_ftd: boolean;
  deposit_count: number;
  total_deposit: number;
  balance: number;
  credit: number;
  equity: number;
  open_pnl: number;
  closed_pnl: number | null;
  live_equity: number;
  max_open_trade: number | null;
  max_volume: number | null;
  turnover: number | null;
  win_rate: number | null;
  avg_trade_size: number | null;
  exposure_pct: number | null;
  exposure_usd: number;
  used_margin: number;
  free_margin: number;
  margin_level_pct: number | null;
  assigned_to: string | null;
  agent_name: string | null;
  tasks: TaskInfo[];
  score: number;
  retention_status: string | null;
  age: number | null;
  is_favorite: boolean;
  card_type: string | null;
  last_communication_date: string | null;
  last_deposit_date: string | null;
  last_withdrawal_date: string | null;
  net_deposit_ever: number | null;
  client_potential: string | null;
  client_segment: string | null;
  sales_client_potential: string | null;
  country: string | null;
  legacy_id: string | null;
  affiliate: string | null;
  mt_account: string | null;
}

interface Filters {
  accountid: string;
  qual_date_from: string;
  qual_date_to: string;
  trade_count_op: NumOp;
  trade_count_val: string;
  days_op: NumOp;
  days_val: string;
  profit_op: NumOp;
  profit_val: string;
  last_trade_preset: string;
  last_trade_from: string;
  last_trade_to: string;
  days_from_last_trade_op: NumOp;
  days_from_last_trade_val: string;
  deposit_count_op: NumOp;
  deposit_count_val: string;
  total_deposit_op: NumOp;
  total_deposit_val: string;
  balance_op: NumOp;
  balance_val: string;
  credit_op: NumOp;
  credit_val: string;
  equity_op: NumOp;
  equity_val: string;
  live_equity_op: NumOp;
  live_equity_val: string;
  max_open_trade_op: NumOp;
  max_open_trade_val: string;
  max_volume_op: NumOp;
  max_volume_val: string;
  turnover_op: NumOp;
  turnover_val: string;
  assigned_to: string;
  task_id: string;
  active: BoolFilter;
  active_ftd: BoolFilter;
  favorites_only: BoolFilter;
}

const EMPTY_FILTERS: Filters = {
  accountid: '',
  qual_date_from: '',
  qual_date_to: '',
  trade_count_op: '',
  trade_count_val: '',
  days_op: '',
  days_val: '',
  profit_op: '',
  profit_val: '',
  last_trade_preset: '',
  last_trade_from: '',
  last_trade_to: '',
  days_from_last_trade_op: '',
  days_from_last_trade_val: '',
  deposit_count_op: '',
  deposit_count_val: '',
  total_deposit_op: '',
  total_deposit_val: '',
  balance_op: '',
  balance_val: '',
  credit_op: '',
  credit_val: '',
  equity_op: '',
  equity_val: '',
  live_equity_op: '',
  live_equity_val: '',
  max_open_trade_op: '',
  max_open_trade_val: '',
  max_volume_op: '',
  max_volume_val: '',
  turnover_op: '',
  turnover_val: '',
  assigned_to: '',
  task_id: '',
  active: '',
  active_ftd: '',
  favorites_only: '',
};

function countActive(f: Filters) {
  return [
    f.accountid,
    f.qual_date_from,
    f.qual_date_to,
    f.trade_count_op && f.trade_count_val,
    f.days_op && f.days_val,
    f.profit_op && f.profit_val,
    f.last_trade_preset === 'custom' ? f.last_trade_from : f.last_trade_preset,
    f.days_from_last_trade_op && f.days_from_last_trade_val,
    f.deposit_count_op && f.deposit_count_val,
    f.total_deposit_op && f.total_deposit_val,
    f.balance_op && f.balance_val,
    f.credit_op && f.credit_val,
    f.equity_op && f.equity_val,
    f.live_equity_op && f.live_equity_val,
    f.max_open_trade_op && f.max_open_trade_val,
    f.max_volume_op && f.max_volume_val,
    f.turnover_op && f.turnover_val,
    f.assigned_to,
    f.task_id,
    f.active,
    f.active_ftd,
    f.favorites_only,
  ].filter(Boolean).length;
}

// ── ColFilter helper components ───────────────────────────────────────────

function ColTextFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const value = filter?.type === 'text' ? filter.value : '';

  const handleChange = (v: string) => {
    setColFilters((prev) => {
      if (!v) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      return { ...prev, [col]: { type: 'text', value: v } };
    });
  };

  return (
    <div className="flex items-center gap-0.5 mt-1" onClick={(e) => e.stopPropagation()}>
      <input
        type="text"
        value={value}
        onChange={(e) => handleChange(e.target.value)}
        placeholder="Filter..."
        className="w-full min-w-0 border border-gray-300 dark:border-gray-600 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
      />
      {value && (
        <button
          onClick={() => handleChange('')}
          className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xs leading-none flex-shrink-0 ml-0.5"
          title="Clear"
        >
          x
        </button>
      )}
    </div>
  );
}

function ColNumericFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const op: ColNumOp = (filter?.type === 'numeric' ? filter.op : 'gt') as ColNumOp;
  const val = filter?.type === 'numeric' ? filter.val : '';
  const val2 = filter?.type === 'numeric' ? (filter.val2 ?? '') : '';

  const update = (newOp: ColNumOp, newVal: string, newVal2?: string) => {
    setColFilters((prev) => {
      if (!newVal) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      const entry: ColFilter = { type: 'numeric', op: newOp, val: newVal };
      if (newOp === 'between' && newVal2) (entry as { type: 'numeric'; op: ColNumOp; val: string; val2?: string }).val2 = newVal2;
      return { ...prev, [col]: entry };
    });
  };

  return (
    <div className="mt-1 space-y-0.5" onClick={(e) => e.stopPropagation()}>
      <div className="flex items-center gap-0.5">
        <select
          value={op}
          onChange={(e) => update(e.target.value as ColNumOp, val, val2)}
          className="border border-gray-300 dark:border-gray-600 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100 flex-shrink-0"
        >
          <option value="gt">&gt;</option>
          <option value="lt">&lt;</option>
          <option value="eq">=</option>
          <option value="gte">&ge;</option>
          <option value="lte">&le;</option>
          <option value="between">btw</option>
        </select>
        <input
          type="number"
          value={val}
          onChange={(e) => update(op, e.target.value, val2)}
          placeholder="Value"
          className="min-w-0 w-16 border border-gray-300 dark:border-gray-600 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
        />
        {val && (
          <button
            onClick={() => update(op, '', '')}
            className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xs leading-none flex-shrink-0"
            title="Clear"
          >
            x
          </button>
        )}
      </div>
      {op === 'between' && val && (
        <input
          type="number"
          value={val2}
          onChange={(e) => update(op, val, e.target.value)}
          placeholder="To"
          className="w-full border border-gray-300 dark:border-gray-600 rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
        />
      )}
    </div>
  );
}

function ColDateFilter({ col, colFilters, setColFilters }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
}) {
  const filter = colFilters[col];
  const preset = filter?.type === 'date' ? (filter.preset ?? '') : '';
  const from = filter?.type === 'date' ? (filter.from ?? '') : '';
  const to = filter?.type === 'date' ? (filter.to ?? '') : '';

  const today = new Date().toISOString().slice(0, 10);
  const getPresetRange = (p: ColDatePreset): { from: string; to: string } => {
    const now = new Date();
    const fmt = (d: Date) => d.toISOString().slice(0, 10);
    switch (p) {
      case 'today': return { from: today, to: today };
      case 'this_week': {
        const day = now.getDay();
        const monday = new Date(now); monday.setDate(now.getDate() - ((day + 6) % 7));
        return { from: fmt(monday), to: today };
      }
      case 'this_month': return { from: fmt(new Date(now.getFullYear(), now.getMonth(), 1)), to: today };
      default: return { from: '', to: '' };
    }
  };

  const update = (p: string, f?: string, t?: string) => {
    setColFilters((prev) => {
      if (!p) {
        const next = { ...prev };
        delete next[col];
        return next;
      }
      if (p === 'custom') {
        return { ...prev, [col]: { type: 'date', preset: 'custom', from: f ?? '', to: t ?? '' } };
      }
      const range = getPresetRange(p as ColDatePreset);
      return { ...prev, [col]: { type: 'date', preset: p as ColDatePreset, from: range.from, to: range.to } };
    });
  };

  return (
    <div className="mt-1 space-y-0.5" onClick={(e) => e.stopPropagation()}>
      <select
        value={preset}
        onChange={(e) => update(e.target.value)}
        className="w-full border border-gray-300 dark:border-gray-600 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100"
      >
        <option value="">Any</option>
        <option value="today">Today</option>
        <option value="this_week">This Week</option>
        <option value="this_month">This Month</option>
        <option value="custom">Custom</option>
      </select>
      {preset === 'custom' && (
        <div className="flex flex-col gap-0.5">
          <input
            type="date"
            value={from}
            onChange={(e) => update('custom', e.target.value, to)}
            className="w-full border border-gray-300 dark:border-gray-600 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100"
          />
          <input
            type="date"
            value={to}
            onChange={(e) => update('custom', from, e.target.value)}
            className="w-full border border-gray-300 dark:border-gray-600 rounded px-1 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 dark:text-gray-100"
          />
        </div>
      )}
    </div>
  );
}

function ColMultiSelectFilter({ col, colFilters, setColFilters, options }: {
  col: string;
  colFilters: ColFilters;
  setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>;
  options: string[];
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState('');
  const dropdownRef = useRef<HTMLDivElement>(null);
  const searchRef = useRef<HTMLInputElement>(null);
  const filter = colFilters[col];
  const selected: string[] = filter?.type === 'multiselect' ? filter.values : [];
  const showSearch = options.length > 4;
  const visibleOptions = useMemo(
    () => search.trim()
      ? options.filter((o) => o.toLowerCase().includes(search.toLowerCase()))
      : options,
    [search, options],
  );

  // Close on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  // Auto-focus search when dropdown opens
  useEffect(() => {
    if (open && showSearch) {
      setTimeout(() => searchRef.current?.focus(), 0);
    }
    if (!open) setSearch('');
  }, [open, showSearch]);

  const toggle = (val: string) => {
    setColFilters((prev) => {
      const cur = prev[col]?.type === 'multiselect' ? prev[col].values : [];
      const next = cur.includes(val) ? cur.filter((v) => v !== val) : [...cur, val];
      if (next.length === 0) {
        const updated = { ...prev };
        delete updated[col];
        return updated;
      }
      return { ...prev, [col]: { type: 'multiselect', values: next } };
    });
  };

  const selectAll = () => {
    setColFilters((prev) => ({ ...prev, [col]: { type: 'multiselect', values: [...options] } }));
  };

  const clearAll = () => {
    setColFilters((prev) => {
      const next = { ...prev };
      delete next[col];
      return next;
    });
  };

  return (
    <div className="relative mt-1" ref={dropdownRef} onClick={(e) => e.stopPropagation()}>
      <button
        onClick={() => setOpen((v) => !v)}
        className={`w-full flex items-center justify-between border rounded px-1.5 py-0.5 text-xs focus:outline-none focus:ring-1 focus:ring-blue-400 bg-white dark:bg-gray-800 ${selected.length > 0 ? 'border-blue-400 text-blue-700' : 'border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400'}`}
      >
        <span className="truncate">{selected.length > 0 ? `${selected.length} selected` : 'Filter...'}</span>
        <span className="ml-1 text-[10px]">{open ? '\u25B2' : '\u25BC'}</span>
      </button>
      {open && (
        <div className="absolute z-50 left-0 mt-1 w-48 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-md shadow-lg dark:shadow-gray-900">
          {showSearch && (
            <div className="px-2 pt-2 pb-1">
              <input
                ref={searchRef}
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search..."
                className="w-full border border-gray-300 dark:border-gray-600 rounded px-2 py-1 text-xs bg-white dark:bg-gray-700 text-gray-700 dark:text-gray-300 placeholder-gray-400 focus:outline-none focus:ring-1 focus:ring-blue-400"
              />
            </div>
          )}
          <div className="flex items-center justify-between px-2 py-1.5 border-b border-gray-100 dark:border-gray-700">
            <button onClick={selectAll} className="text-xs text-blue-600 hover:text-blue-800 font-medium">Select All</button>
            <button onClick={clearAll} className="text-xs text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 font-medium">Clear</button>
          </div>
          <div className="max-h-52 overflow-y-auto">
            {visibleOptions.map((opt) => (
              <label key={opt} className="flex items-center gap-2 px-2 py-1.5 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer text-xs text-gray-700 dark:text-gray-300">
                <input
                  type="checkbox"
                  checked={selected.includes(opt)}
                  onChange={() => toggle(opt)}
                  className="rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-400"
                />
                {opt}
              </label>
            ))}
            {visibleOptions.length === 0 && (
              <div className="px-2 py-2 text-xs text-gray-400 dark:text-gray-500 text-center">No results found</div>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function SortIcon({ col, sortBy, sortDir }: { col: SortCol; sortBy: SortCol; sortDir: 'asc' | 'desc' }) {
  if (sortBy !== col) return <span className="ml-1 text-gray-300 dark:text-gray-600">↕</span>;
  return <span className="ml-1">{sortDir === 'asc' ? '↑' : '↓'}</span>;
}

// CLAUD-149: Retention statuses are now loaded dynamically from the backend (report.ant_ret_status)

/** Resolve a status value to its display name.
 *  Handles: name string (pass-through), numeric ID string (lookup), null/undefined (dash). */
function resolveStatusDisplay(raw: string | null | undefined, statusList: { key: number; label: string }[]): string {
  if (raw == null || raw === '') return '\u2014';
  // If it matches a known label, return as-is
  if (statusList.some((s) => s.label === raw)) return raw;
  // If it's a numeric ID, look up the label
  const byKey = statusList.find((s) => String(s.key) === raw);
  if (byKey) return byKey.label;
  // Unknown value -- show as-is
  return raw;
}

const TASK_COLOR_STYLES: Record<string, { bg: string; text: string }> = {
  red:    { bg: 'bg-red-100',    text: 'text-red-700' },
  orange: { bg: 'bg-orange-100', text: 'text-orange-700' },
  yellow: { bg: 'bg-yellow-100', text: 'text-yellow-700' },
  green:  { bg: 'bg-green-100',  text: 'text-green-700' },
  blue:   { bg: 'bg-blue-100',   text: 'text-blue-700' },
  purple: { bg: 'bg-purple-100', text: 'text-purple-700' },
  pink:   { bg: 'bg-pink-100',   text: 'text-pink-700' },
  grey:   { bg: 'bg-gray-100',   text: 'text-gray-700' },
};


function formatDate(iso: string | null) {
  if (!iso) return '—';
  return new Date(iso).toLocaleDateString();
}

function fmtNum(v: number, decimals = 2) {
  return v.toLocaleString(undefined, { minimumFractionDigits: decimals, maximumFractionDigits: decimals });
}

// CLAUD-127/128: Country ISO → IANA timezone map (primary timezone per country)
const COUNTRY_TO_TZ: Record<string, string> = {
  AF:'Asia/Kabul', AL:'Europe/Tirane', DZ:'Africa/Algiers', AO:'Africa/Luanda',
  AR:'America/Argentina/Buenos_Aires', AM:'Asia/Yerevan', AU:'Australia/Sydney',
  AT:'Europe/Vienna', AZ:'Asia/Baku', BH:'Asia/Bahrain', BD:'Asia/Dhaka',
  BY:'Europe/Minsk', BE:'Europe/Brussels', BJ:'Africa/Porto-Novo', BO:'America/La_Paz',
  BA:'Europe/Sarajevo', BR:'America/Sao_Paulo', BG:'Europe/Sofia', BF:'Africa/Ouagadougou',
  KH:'Asia/Phnom_Penh', CM:'Africa/Douala', CA:'America/Toronto', CF:'Africa/Bangui',
  CL:'America/Santiago', CN:'Asia/Shanghai', CO:'America/Bogota', CD:'Africa/Kinshasa',
  CR:'America/Costa_Rica', HR:'Europe/Zagreb', CU:'America/Havana', CY:'Asia/Nicosia',
  CZ:'Europe/Prague', DK:'Europe/Copenhagen', DO:'America/Santo_Domingo', EC:'America/Guayaquil',
  EG:'Africa/Cairo', SV:'America/El_Salvador', ET:'Africa/Addis_Ababa', EE:'Europe/Tallinn',
  FI:'Europe/Helsinki', FR:'Europe/Paris', GA:'Africa/Libreville', GE:'Asia/Tbilisi',
  DE:'Europe/Berlin', GH:'Africa/Accra', GR:'Europe/Athens', GT:'America/Guatemala',
  GN:'Africa/Conakry', HT:'America/Port-au-Prince', HN:'America/Tegucigalpa',
  HK:'Asia/Hong_Kong', HU:'Europe/Budapest', IS:'Atlantic/Reykjavik', IN:'Asia/Kolkata',
  ID:'Asia/Jakarta', IR:'Asia/Tehran', IQ:'Asia/Baghdad', IE:'Europe/Dublin',
  IL:'Asia/Jerusalem', IT:'Europe/Rome', CI:'Africa/Abidjan', JM:'America/Jamaica',
  JP:'Asia/Tokyo', JO:'Asia/Amman', KZ:'Asia/Almaty', KE:'Africa/Nairobi',
  KW:'Asia/Kuwait', KG:'Asia/Bishkek', LA:'Asia/Vientiane', LV:'Europe/Riga',
  LB:'Asia/Beirut', LY:'Africa/Tripoli', LT:'Europe/Vilnius', LU:'Europe/Luxembourg',
  MO:'Asia/Macau', MK:'Europe/Skopje', MG:'Indian/Antananarivo', MW:'Africa/Blantyre',
  MY:'Asia/Kuala_Lumpur', ML:'Africa/Bamako', MT:'Europe/Malta', MR:'Africa/Nouakchott',
  MX:'America/Mexico_City', MD:'Europe/Chisinau', MN:'Asia/Ulaanbaatar', ME:'Europe/Podgorica',
  MA:'Africa/Casablanca', MZ:'Africa/Maputo', MM:'Asia/Rangoon', NA:'Africa/Windhoek',
  NP:'Asia/Kathmandu', NL:'Europe/Amsterdam', NZ:'Pacific/Auckland', NI:'America/Managua',
  NE:'Africa/Niamey', NG:'Africa/Lagos', NO:'Europe/Oslo', OM:'Asia/Muscat',
  PK:'Asia/Karachi', PA:'America/Panama', PY:'America/Asuncion', PE:'America/Lima',
  PH:'Asia/Manila', PL:'Europe/Warsaw', PT:'Europe/Lisbon', QA:'Asia/Qatar',
  RO:'Europe/Bucharest', RU:'Europe/Moscow', RW:'Africa/Kigali', SA:'Asia/Riyadh',
  SN:'Africa/Dakar', RS:'Europe/Belgrade', SL:'Africa/Freetown', SG:'Asia/Singapore',
  SK:'Europe/Bratislava', SI:'Europe/Ljubljana', SO:'Africa/Mogadishu', ZA:'Africa/Johannesburg',
  SD:'Africa/Khartoum', SS:'Africa/Juba', ES:'Europe/Madrid', LK:'Asia/Colombo',
  SD2:'Africa/Khartoum', SE:'Europe/Stockholm', CH:'Europe/Zurich', SY:'Asia/Damascus',
  TW:'Asia/Taipei', TJ:'Asia/Dushanbe', TZ:'Africa/Dar_es_Salaam', TH:'Asia/Bangkok',
  TG:'Africa/Lome', TN:'Africa/Tunis', TR:'Europe/Istanbul', TM:'Asia/Ashgabat',
  UG:'Africa/Kampala', UA:'Europe/Kiev', AE:'Asia/Dubai', GB:'Europe/London',
  US:'America/New_York', UY:'America/Montevideo', UZ:'Asia/Tashkent', VE:'America/Caracas',
  VN:'Asia/Ho_Chi_Minh', YE:'Asia/Aden', ZM:'Africa/Lusaka', ZW:'Africa/Harare',
};

// CLAUD-135: ISO alpha-2 → full English country name
const COUNTRY_ISO_TO_NAME: Record<string, string> = {
  AF:'Afghanistan', AL:'Albania', DZ:'Algeria', AO:'Angola', AR:'Argentina',
  AM:'Armenia', AU:'Australia', AT:'Austria', AZ:'Azerbaijan', BH:'Bahrain',
  BD:'Bangladesh', BY:'Belarus', BE:'Belgium', BJ:'Benin', BO:'Bolivia',
  BA:'Bosnia and Herzegovina', BR:'Brazil', BG:'Bulgaria', BF:'Burkina Faso',
  KH:'Cambodia', CM:'Cameroon', CA:'Canada', CF:'Central African Republic',
  CL:'Chile', CN:'China', CO:'Colombia', CD:'Congo (DRC)', CR:'Costa Rica',
  HR:'Croatia', CU:'Cuba', CY:'Cyprus', CZ:'Czech Republic', DK:'Denmark',
  DO:'Dominican Republic', EC:'Ecuador', EG:'Egypt', SV:'El Salvador',
  ET:'Ethiopia', EE:'Estonia', FI:'Finland', FR:'France', GA:'Gabon',
  GE:'Georgia', DE:'Germany', GH:'Ghana', GR:'Greece', GT:'Guatemala',
  GN:'Guinea', HT:'Haiti', HN:'Honduras', HK:'Hong Kong', HU:'Hungary',
  IS:'Iceland', IN:'India', ID:'Indonesia', IR:'Iran', IQ:'Iraq',
  IE:'Ireland', IL:'Israel', IT:'Italy', CI:"Côte d'Ivoire", JM:'Jamaica',
  JP:'Japan', JO:'Jordan', KZ:'Kazakhstan', KE:'Kenya', KW:'Kuwait',
  KG:'Kyrgyzstan', LA:'Laos', LV:'Latvia', LB:'Lebanon', LY:'Libya',
  LT:'Lithuania', LU:'Luxembourg', MO:'Macau', MK:'North Macedonia',
  MG:'Madagascar', MW:'Malawi', MY:'Malaysia', ML:'Mali', MT:'Malta',
  MR:'Mauritania', MX:'Mexico', MD:'Moldova', MN:'Mongolia', ME:'Montenegro',
  MA:'Morocco', MZ:'Mozambique', MM:'Myanmar', NA:'Namibia', NP:'Nepal',
  NL:'Netherlands', NZ:'New Zealand', NI:'Nicaragua', NE:'Niger', NG:'Nigeria',
  NO:'Norway', OM:'Oman', PK:'Pakistan', PA:'Panama', PY:'Paraguay',
  PE:'Peru', PH:'Philippines', PL:'Poland', PT:'Portugal', QA:'Qatar',
  RO:'Romania', RU:'Russia', RW:'Rwanda', SA:'Saudi Arabia', SN:'Senegal',
  RS:'Serbia', SL:'Sierra Leone', SG:'Singapore', SK:'Slovakia', SI:'Slovenia',
  SO:'Somalia', ZA:'South Africa', SD:'Sudan', SS:'South Sudan', ES:'Spain',
  LK:'Sri Lanka', SE:'Sweden', CH:'Switzerland', SY:'Syria', TW:'Taiwan',
  TJ:'Tajikistan', TZ:'Tanzania', TH:'Thailand', TG:'Togo', TN:'Tunisia',
  TR:'Turkey', TM:'Turkmenistan', UG:'Uganda', UA:'Ukraine', AE:'United Arab Emirates',
  GB:'United Kingdom', US:'United States', UY:'Uruguay', UZ:'Uzbekistan',
  VE:'Venezuela', VN:'Vietnam', YE:'Yemen', ZM:'Zambia', ZW:'Zimbabwe',
};


function getLocalTime(countryIso: string | null): string | null {
  if (!countryIso) return null;
  const tz = COUNTRY_TO_TZ[countryIso.toUpperCase()];
  if (!tz) return null;
  try {
    return new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', hour12: false, timeZone: tz }).format(new Date());
  } catch {
    return null;
  }
}

function LocalTimeCell({ countryIso }: { countryIso: string | null }) {
  const [time, setTime] = useState(() => getLocalTime(countryIso));
  useEffect(() => {
    setTime(getLocalTime(countryIso));
    const now = new Date();
    const msToNextMinute = (60 - now.getSeconds()) * 1000 - now.getMilliseconds();
    const timeout = setTimeout(() => {
      setTime(getLocalTime(countryIso));
      const interval = setInterval(() => setTime(getLocalTime(countryIso)), 60000);
      return () => clearInterval(interval);
    }, msToNextMinute);
    return () => clearTimeout(timeout);
  }, [countryIso]);
  if (!time) return <span className="text-xs text-gray-400 dark:text-gray-500">—</span>;
  return <span className="text-sm font-mono text-gray-700 dark:text-gray-300">{time}</span>;
}

function WhatsAppIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="currentColor" xmlns="http://www.w3.org/2000/svg">
      <path d="M17.472 14.382c-.297-.149-1.758-.867-2.03-.967-.273-.099-.471-.148-.67.15-.197.297-.767.966-.94 1.164-.173.199-.347.223-.644.075-.297-.15-1.255-.463-2.39-1.475-.883-.788-1.48-1.761-1.653-2.059-.173-.297-.018-.458.13-.606.134-.133.298-.347.446-.52.149-.174.198-.298.298-.497.099-.198.05-.371-.025-.52-.075-.149-.669-1.612-.916-2.207-.242-.579-.487-.5-.669-.51-.173-.008-.371-.01-.57-.01-.198 0-.52.074-.792.372-.272.297-1.04 1.016-1.04 2.479 0 1.462 1.065 2.875 1.213 3.074.149.198 2.096 3.2 5.077 4.487.709.306 1.262.489 1.694.625.712.227 1.36.195 1.871.118.571-.085 1.758-.719 2.006-1.413.248-.694.248-1.289.173-1.413-.074-.124-.272-.198-.57-.347m-5.421 7.403h-.004a9.87 9.87 0 01-5.031-1.378l-.361-.214-3.741.982.998-3.648-.235-.374a9.86 9.86 0 01-1.51-5.26c.001-5.45 4.436-9.884 9.888-9.884 2.64 0 5.122 1.03 6.988 2.898a9.825 9.825 0 012.893 6.994c-.003 5.45-4.437 9.884-9.885 9.884m8.413-18.297A11.815 11.815 0 0012.05 0C5.495 0 .16 5.335.157 11.892c0 2.096.547 4.142 1.588 5.945L.057 24l6.305-1.654a11.882 11.882 0 005.683 1.448h.005c6.554 0 11.89-5.335 11.893-11.893a11.821 11.821 0 00-3.48-8.413z"/>
    </svg>
  );
}

type ActionTab = 'status' | 'note' | 'whatsapp' | 'call';

function CallIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M22 16.92v3a2 2 0 01-2.18 2 19.79 19.79 0 01-8.63-3.07 19.5 19.5 0 01-6-6 19.79 19.79 0 01-3.07-8.67A2 2 0 014.11 2h3a2 2 0 012 1.72 12.84 12.84 0 00.7 2.81 2 2 0 01-.45 2.11L8.09 9.91a16 16 0 006 6l1.27-1.27a2 2 0 012.11-.45 12.84 12.84 0 002.81.7A2 2 0 0122 16.92z" />
    </svg>
  );
}

function StatusIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M20.59 13.41l-7.17 7.17a2 2 0 01-2.83 0L2 12V2h10l8.59 8.59a2 2 0 010 2.82z" />
      <line x1="7" y1="7" x2="7.01" y2="7" />
    </svg>
  );
}

function NoteIcon({ className }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" xmlns="http://www.w3.org/2000/svg">
      <path d="M11 4H4a2 2 0 00-2 2v14a2 2 0 002 2h14a2 2 0 002-2v-7" />
      <path d="M18.5 2.5a2.121 2.121 0 013 3L12 15l-4 1 1-4 9.5-9.5z" />
    </svg>
  );
}

function friendlyError(err: any, fallback: string): string {
  const status = err.response?.status;
  const detail = err.response?.data?.detail;
  if (detail) return detail;
  switch (status) {
    case 400: return 'Invalid request. Please check your input and try again.';
    case 404: return 'Client not found in CRM. Please verify the account ID.';
    case 502: return 'CRM returned an unexpected error. Please contact an administrator.';
    case 503: return 'CRM service is temporarily unavailable. Please try again later.';
    default: return fallback;
  }
}

// Map popup action tabs to CRM permission action names (CLAUD-16 RBAC)
const TAB_PERMISSION_MAP: Record<ActionTab, string> = {
  status: 'edit_client_status',
  note: 'send_note',
  whatsapp: 'send_whatsapp',
  call: 'make_call',
};

function ClientActionsModal({
  client,
  onClose,
}: {
  client: RetentionClient;
  onClose: () => void;
}) {
  // CRM permissions (CLAUD-16 RBAC)
  const crmPerms = useAuthStore((s) => s.crmPermissions);
  const userRole = useAuthStore((s) => s.role);

  // CLAUD-149: Dynamic retention statuses from report.ant_ret_status
  const { statuses: retentionStatuses } = useRetentionStatuses();

  // Active tab
  const [activeTab, setActiveTab] = useState<ActionTab>('status');

  // Current retention status (fetched on modal open)
  const [currentStatusLabel, setCurrentStatusLabel] = useState<string | null>(null);
  const [currentStatusLoading, setCurrentStatusLoading] = useState(true);

  // Fetch current retention status on modal open
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await api.get(`/clients/${client.accountid}/crm-user`);
        if (cancelled) return;
        const statusKey = res.data?.retentionStatus;
        if (statusKey !== undefined && statusKey !== null) {
          const match = retentionStatuses.find((s) => s.key === Number(statusKey));
          setCurrentStatusLabel(match ? match.label : `Unknown (${statusKey})`);
        }
      } catch {
        // Gracefully ignore — badge simply won't show
      } finally {
        if (!cancelled) setCurrentStatusLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, [client.accountid]);

  // Retention status state
  const [selectedStatus, setSelectedStatus] = useState<string>('');
  const [statusSubmitting, setStatusSubmitting] = useState(false);
  const [statusFeedback, setStatusFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Add note state
  const [noteText, setNoteText] = useState('');
  const [noteSubmitting, setNoteSubmitting] = useState(false);
  const [noteFeedback, setNoteFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // WhatsApp state
  const [waLoading, setWaLoading] = useState(false);
  const [waFeedback, setWaFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  // Call state
  const [callLoading, setCallLoading] = useState(false);
  const [callFeedback, setCallFeedback] = useState<{ type: 'success' | 'error'; message: string } | null>(null);

  const handleStatusSubmit = async () => {
    if (!selectedStatus) return;
    setStatusSubmitting(true);
    setStatusFeedback(null);
    try {
      await api.put(`/clients/${client.accountid}/retention-status`, {
        status_key: Number(selectedStatus),
      });
      const statusLabel = retentionStatuses.find((s) => s.key === Number(selectedStatus))?.label ?? selectedStatus;
      setCurrentStatusLabel(statusLabel);
      setStatusFeedback({ type: 'success', message: `Retention status updated to "${statusLabel}"` });
    } catch (err: any) {
      setStatusFeedback({ type: 'error', message: friendlyError(err, 'Failed to update retention status') });
    } finally {
      setStatusSubmitting(false);
    }
  };

  const handleNoteSubmit = async () => {
    if (!noteText.trim()) return;
    setNoteSubmitting(true);
    setNoteFeedback(null);
    try {
      await api.post(`/clients/${client.accountid}/note`, { note: noteText.trim() });
      setNoteFeedback({ type: 'success', message: 'Note added successfully' });
      setNoteText('');
    } catch (err: any) {
      setNoteFeedback({ type: 'error', message: friendlyError(err, 'Failed to add note') });
    } finally {
      setNoteSubmitting(false);
    }
  };

  const handleWhatsApp = async () => {
    setWaLoading(true);
    setWaFeedback(null);
    try {
      const res = await api.get(`/clients/${client.accountid}/crm-user`, { params: { log_whatsapp: true } });
      const phone = res.data?.fullTelephone || res.data?.telephone || res.data?.phone || res.data?.Phone || res.data?.phoneNumber || res.data?.PhoneNumber || res.data?.mobile || res.data?.Mobile;
      if (!phone) {
        setWaFeedback({ type: 'error', message: 'No phone number found for this client' });
        return;
      }
      const cleanPhone = String(phone).replace(/[^0-9+]/g, '').replace(/^\+/, '');
      const waUrl = `https://api.whatsapp.com/send?phone=${encodeURIComponent(cleanPhone)}`;
      window.open(waUrl, '_blank');
      setWaFeedback({ type: 'success', message: 'WhatsApp tab opened' });
    } catch (err: any) {
      setWaFeedback({ type: 'error', message: friendlyError(err, 'Failed to fetch client phone number') });
    } finally {
      setWaLoading(false);
    }
  };

  const handleCall = async () => {
    setCallLoading(true);
    setCallFeedback(null);
    try {
      const res = await api.post(`/clients/${client.accountid}/call`);
      setCallFeedback({ type: 'success', message: res.data?.message || 'Call initiated successfully' });
    } catch (err: any) {
      setCallFeedback({ type: 'error', message: friendlyError(err, 'Failed to initiate call') });
    } finally {
      setCallLoading(false);
    }
  };

  const feedbackEl = (fb: { type: 'success' | 'error'; message: string } | null) =>
    fb ? (
      <div
        className={`px-3 py-2 rounded-md text-sm mt-3 ${
          fb.type === 'success'
            ? 'bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400 border border-green-200 dark:border-green-800'
            : 'bg-red-50 dark:bg-red-900/20 text-red-700 dark:text-red-400 border border-red-200 dark:border-red-800'
        }`}
      >
        {fb.message}
      </div>
    ) : null;

  const allTabs: { key: ActionTab; label: string; icon: JSX.Element }[] = [
    {
      key: 'status',
      label: 'Status',
      icon: <StatusIcon className="w-5 h-5" />,
    },
    {
      key: 'note',
      label: 'Note',
      icon: <NoteIcon className="w-5 h-5" />,
    },
    {
      key: 'whatsapp',
      label: 'WhatsApp',
      icon: <WhatsAppIcon className="w-5 h-5" />,
    },
    {
      key: 'call',
      label: 'Call',
      icon: <CallIcon className="w-5 h-5" />,
    },
  ];

  // Filter tabs based on CRM permissions (CLAUD-16 RBAC).
  // Admin always sees all tabs. For others, only show tabs where the
  // corresponding CRM permission is enabled.
  const tabs = userRole === 'admin'
    ? allTabs
    : allTabs.filter((tab) => {
        const permKey = TAB_PERMISSION_MAP[tab.key];
        // If permissions haven't loaded yet, show tab by default
        return permKey === undefined || crmPerms[permKey] !== false;
      });

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={onClose}>
      <div
        className="bg-white dark:bg-gray-800 rounded-lg shadow-xl dark:shadow-gray-900 w-full max-w-md mx-4 max-h-[90vh] flex flex-col"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="px-6 py-4 border-b border-gray-200 dark:border-gray-700 flex items-center justify-between shrink-0">
          <div>
            <h2 className="text-lg font-semibold text-gray-800 dark:text-gray-100">Client Actions</h2>
            <p className="text-sm text-gray-500 dark:text-gray-400 mt-0.5">Account: {client.accountid}</p>
          </div>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 text-xl leading-none"
            title="Close"
          >
            x
          </button>
        </div>

        {/* Current retention status badge */}
        <div className="px-6 pt-3 pb-0 shrink-0">
          {currentStatusLoading ? (
            <div className="flex items-center gap-2 text-sm text-gray-400 dark:text-gray-500">
              <svg className="animate-spin h-4 w-4" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
              </svg>
              Loading status...
            </div>
          ) : currentStatusLabel ? (
            <div className="flex items-center gap-2 text-sm">
              <span className="text-gray-500 dark:text-gray-400">Current Status:</span>
              <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                {currentStatusLabel}
              </span>
            </div>
          ) : null}
        </div>

        {/* Icon tab navigation */}
        <div className="px-6 pt-4 pb-0 shrink-0">
          <div className="flex gap-1 justify-center">
            {tabs.map((tab) => (
              <button
                key={tab.key}
                onClick={() => {
                  setActiveTab(tab.key);
                }}
                className={`flex items-center gap-2 px-4 py-2.5 rounded-lg text-sm font-medium transition-colors ${
                  activeTab === tab.key
                    ? 'bg-blue-50 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 border border-blue-200 dark:border-blue-700'
                    : 'text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-700 border border-transparent'
                }`}
                title={tab.label}
              >
                {tab.icon}
                <span>{tab.label}</span>
              </button>
            ))}
          </div>
        </div>

        {/* Body — only active action shown */}
        <div className="px-6 py-5 overflow-y-auto">
          {/* Action: Change Retention Status */}
          {activeTab === 'status' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Change Retention Status
              </label>
              <select
                value={selectedStatus}
                onChange={(e) => {
                  setSelectedStatus(e.target.value);
                  setStatusFeedback(null);
                }}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 bg-white dark:bg-gray-800 dark:text-gray-100"
                disabled={statusSubmitting}
              >
                <option value="">-- Select Status --</option>
                {retentionStatuses.map((s) => (
                  <option key={s.key} value={String(s.key)}>
                    {s.label}
                  </option>
                ))}
              </select>
              <div className="flex justify-end mt-3">
                <button
                  onClick={handleStatusSubmit}
                  disabled={!selectedStatus || statusSubmitting}
                  className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {statusSubmitting ? 'Updating...' : 'Update Status'}
                </button>
              </div>
              {feedbackEl(statusFeedback)}
            </div>
          )}

          {/* Action: Add Note */}
          {activeTab === 'note' && (
            <div>
              <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                Add Note
              </label>
              <textarea
                value={noteText}
                onChange={(e) => {
                  setNoteText(e.target.value);
                  setNoteFeedback(null);
                }}
                placeholder="Type a note for this client..."
                rows={4}
                className="w-full border border-gray-300 dark:border-gray-600 rounded-md px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 resize-none dark:bg-gray-800 dark:text-gray-100 dark:placeholder-gray-500"
                disabled={noteSubmitting}
              />
              <div className="flex justify-end mt-3">
                <button
                  onClick={handleNoteSubmit}
                  disabled={!noteText.trim() || noteSubmitting}
                  className="px-4 py-2 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
                >
                  {noteSubmitting ? 'Submitting...' : 'Submit Note'}
                </button>
              </div>
              {feedbackEl(noteFeedback)}
            </div>
          )}

          {/* Action: Send WhatsApp */}
          {activeTab === 'whatsapp' && (
            <div className="text-center py-4">
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                Send a WhatsApp message to this client. The phone number will be fetched from the CRM automatically.
              </p>
              <button
                onClick={handleWhatsApp}
                disabled={waLoading}
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-white bg-green-600 rounded-md hover:bg-green-700 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <WhatsAppIcon className="w-5 h-5" />
                {waLoading ? 'Fetching phone...' : 'Open WhatsApp'}
              </button>
              {feedbackEl(waFeedback)}
            </div>
          )}

          {/* Action: Call via SquareTalk */}
          {activeTab === 'call' && (
            <div className="text-center py-4">
              <p className="text-sm text-gray-600 dark:text-gray-400 mb-4">
                Initiate a phone call to this client via SquareTalk. Your extension will be looked up automatically.
              </p>
              <button
                onClick={handleCall}
                disabled={callLoading}
                className="inline-flex items-center gap-2 px-5 py-2.5 text-sm font-medium text-white bg-blue-600 rounded-md hover:bg-blue-700 disabled:opacity-40 disabled:cursor-not-allowed"
              >
                <CallIcon className="w-5 h-5" />
                {callLoading ? 'Initiating call...' : 'Call'}
              </button>
              {feedbackEl(callFeedback)}
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-4 border-t border-gray-200 dark:border-gray-700 flex justify-between items-center shrink-0">
          <button
            onClick={() => window.open(`/retention/dial?client_id=${client.accountid}`, '_blank')}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium rounded-lg transition-colors flex items-center gap-1.5"
          >
            <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M10 6H6a2 2 0 00-2 2v10a2 2 0 002 2h10a2 2 0 002-2v-4M14 4h6m0 0v6m0-6L10 14" />
            </svg>
            Open Call Dashboard
          </button>
          <button
            onClick={onClose}
            className="px-4 py-2 text-sm font-medium text-gray-600 dark:text-gray-400 border border-gray-300 dark:border-gray-600 rounded-md hover:bg-gray-50 dark:hover:bg-gray-700"
          >
            Close
          </button>
        </div>
      </div>
    </div>
  );
}

// ── Column definitions ─────────────────────────────────────────────────────

type ColFilterType = 'text' | 'numeric' | 'date' | 'multiselect' | 'none';

interface ColDef {
  key: string;
  label: string;
  sortKey?: SortCol;
  align?: 'left' | 'right' | 'center';
  minWidth?: string;
  filterType: ColFilterType;
  filterParamKey?: string; // override the backend param prefix when it differs from col.key
  renderHeader?: (props: { colFilters: ColFilters; setColFilters: React.Dispatch<React.SetStateAction<ColFilters>>; sortBy: SortCol; sortDir: 'asc' | 'desc' }) => ReactNode;
  renderCell: (c: RetentionClient) => ReactNode;
}

const DEFAULT_COLS: ColDef[] = [
  {
    key: 'tasks',
    label: 'Tasks',
    filterType: 'multiselect',
    renderCell: (c) => (
      c.tasks.length === 0 ? (
        <span className="text-xs text-gray-400 dark:text-gray-500">—</span>
      ) : (
        <div className="flex flex-wrap gap-1">
          {c.tasks.map((t) => {
            const style = TASK_COLOR_STYLES[t.color] || TASK_COLOR_STYLES.grey;
            return (
              <span key={t.name} className={`inline-block px-2 py-0.5 rounded-full text-xs font-medium ${style.bg} ${style.text} whitespace-nowrap`}>{t.name}</span>
            );
          })}
        </div>
      )
    ),
  },
  {
    key: 'score',
    label: 'Score',
    sortKey: 'score',
    align: 'center',
    minWidth: '90px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm font-semibold text-blue-700">{c.score}</span>,
  },
  {
    key: 'agent_name',
    label: 'Agent',
    sortKey: 'agent_name',
    align: 'left',
    minWidth: '120px',
    filterType: 'multiselect',
    filterParamKey: 'agent',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.agent_name ?? '—'}</span>,
  },
  {
    key: 'retention_status',
    label: 'Status',
    sortKey: 'retention_status',
    align: 'center',
    filterType: 'multiselect',
    filterParamKey: 'status',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{resolveStatusDisplay(c.retention_status, [])}</span>,
  },
  {
    key: 'age',
    label: 'Age',
    sortKey: 'age',
    align: 'center',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.age ?? '—'}</span>,
  },
  {
    key: 'client_qualification_date',
    label: 'Qual. Date',
    sortKey: 'client_qualification_date',
    align: 'left',
    minWidth: '130px',
    filterType: 'date',
    filterParamKey: 'reg_date',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{formatDate(c.client_qualification_date)}</span>,
  },
  {
    key: 'days_in_retention',
    label: 'Days in Ret.',
    sortKey: 'days_in_retention',
    align: 'center',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.days_in_retention ?? '—'}</span>,
  },
  {
    key: 'trade_count',
    label: 'Trades',
    sortKey: 'trade_count',
    align: 'center',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.trade_count.toLocaleString()}</span>,
  },
  {
    key: 'last_trade_date',
    label: 'Last Trade',
    sortKey: 'last_trade_date',
    align: 'left',
    minWidth: '130px',
    filterType: 'date',
    filterParamKey: 'last_call',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{formatDate(c.last_trade_date)}</span>,
  },
  {
    key: 'days_from_last_trade',
    label: 'Days from Last Trade',
    sortKey: 'days_from_last_trade',
    align: 'center',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.days_from_last_trade ?? '—'}</span>,
  },
  {
    key: 'deposit_count',
    label: 'Deposits',
    sortKey: 'deposit_count',
    align: 'center',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.deposit_count.toLocaleString()}</span>,
  },
  {
    key: 'total_deposit',
    label: 'Total Deposit',
    sortKey: 'total_deposit',
    align: 'right',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700 dark:text-gray-300">{fmtNum(c.total_deposit)}</span>,
  },
  {
    key: 'balance',
    label: 'Balance',
    sortKey: 'balance',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700 dark:text-gray-300">{fmtNum(c.balance)}</span>,
  },
  {
    key: 'credit',
    label: 'Credit',
    sortKey: 'credit',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700 dark:text-gray-300">{fmtNum(c.credit)}</span>,
  },
  {
    key: 'equity',
    label: 'Equity',
    sortKey: 'equity',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700 dark:text-gray-300">{fmtNum(c.equity)}</span>,
  },
  {
    key: 'open_pnl',
    label: 'Open PNL',
    sortKey: 'open_pnl',
    align: 'right',
    filterType: 'none',
    renderCell: (c) => (
      <span className={`text-sm ${c.open_pnl === 0 ? 'text-gray-900 dark:text-gray-100' : c.open_pnl > 0 ? 'font-medium text-green-600' : 'font-medium text-red-600'}`}>
        {fmtNum(c.open_pnl)}
      </span>
    ),
  },
  {
    key: 'live_equity',
    label: 'Live Equity',
    sortKey: 'live_equity',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => {
      const val = Math.abs(c.live_equity);
      return (
        <span className={`text-sm ${val < 1000 ? 'text-red-600' : 'text-gray-700 dark:text-gray-300'}`}>
          {fmtNum(val)}
        </span>
      );
    },
  },
  // CLAUD-137: WD Equity = Live Equity − Credit (computed on frontend)
  {
    key: 'wd_equity',
    label: 'WD Equity',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => {
      if (c.live_equity == null || c.credit == null) return <span className="text-sm text-right text-gray-400 dark:text-gray-500">—</span>;
      const val = c.live_equity - c.credit;
      return <span className={`text-sm text-right ${val < 0 ? 'text-red-600' : 'text-gray-700 dark:text-gray-300'}`}>{fmtNum(val)}</span>;
    },
  },
  {
    key: 'max_open_trade',
    label: 'Max Open Trade',
    sortKey: 'max_open_trade',
    align: 'center',
    minWidth: '120px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.max_open_trade != null ? fmtNum(c.max_open_trade, 1) : '\u2014'}</span>,
  },
  {
    key: 'max_volume',
    label: 'Max Volume',
    sortKey: 'max_volume',
    align: 'right',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-right text-gray-700 dark:text-gray-300">{c.max_volume != null ? fmtNum(c.max_volume, 1) : '\u2014'}</span>,
  },
  {
    key: 'avg_trade_size',
    label: 'Avg Trade Size',
    sortKey: 'avg_trade_size',
    align: 'right',
    minWidth: '120px',
    filterType: 'none',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.avg_trade_size != null ? fmtNum(c.avg_trade_size) : '—'}</span>,
  },
  {
    key: 'turnover',
    label: 'Turnover',
    sortKey: 'turnover',
    align: 'center',
    minWidth: '110px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.turnover != null ? fmtNum(c.turnover, 1) : '—'}</span>,
  },
  {
    key: 'exposure_pct',
    label: 'Exposure %',
    sortKey: 'exposure_pct',
    align: 'center',
    minWidth: '130px',
    filterType: 'numeric',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{c.exposure_pct != null ? `${c.exposure_pct.toFixed(2)}%` : '—'}</span>,
  },
  {
    key: 'exposure_usd',
    label: 'Exposure (USD)',
    sortKey: 'exposure_usd',
    align: 'right',
    minWidth: '130px',
    filterType: 'numeric',
    renderCell: (c) => (
      <span className={`text-sm ${c.exposure_usd === 0 ? 'text-gray-400 dark:text-gray-500' : 'text-gray-900 dark:text-gray-100'}`}>
        {c.exposure_usd === 0 ? '—' : `$${c.exposure_usd.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
      </span>
    ),
  },
  // CLAUD-171: Margin columns
  {
    key: 'margin_level_pct',
    label: 'Margin Level %',
    sortKey: 'margin_level_pct',
    align: 'right',
    minWidth: '130px',
    filterType: 'numeric',
    renderCell: (c) => {
      if (c.margin_level_pct == null) return <span className="text-sm text-gray-400 dark:text-gray-500">—</span>;
      const color = c.margin_level_pct >= 300 ? '#16a34a' : c.margin_level_pct >= 150 ? '#d97706' : '#dc2626';
      return (
        <span className="text-sm font-semibold" style={{ color, fontFamily: "'JetBrains Mono', monospace" }}>
          {c.margin_level_pct.toLocaleString('en-US', { maximumFractionDigits: 1 })}%
        </span>
      );
    },
  },
  {
    key: 'used_margin',
    label: 'Used Margin',
    sortKey: 'used_margin',
    align: 'right',
    minWidth: '120px',
    filterType: 'numeric',
    renderCell: (c) => (
      <span className={`text-sm ${c.used_margin === 0 ? 'text-gray-400 dark:text-gray-500' : 'text-gray-900 dark:text-gray-100'}`}>
        {c.used_margin === 0 ? '—' : `$${c.used_margin.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
      </span>
    ),
  },
  {
    key: 'free_margin',
    label: 'Free Margin',
    sortKey: 'free_margin',
    align: 'right',
    minWidth: '120px',
    filterType: 'numeric',
    renderCell: (c) => {
      const color = c.free_margin < 0 ? '#dc2626' : undefined;
      return (
        <span className="text-sm" style={{ color }}>
          {`$${c.free_margin.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`}
        </span>
      );
    },
  },
  {
    key: 'last_communication_date',
    label: 'Last Contact',
    sortKey: 'last_communication_date',
    align: 'left',
    minWidth: '140px',
    filterType: 'date',
    filterParamKey: 'last_contact',
    renderCell: (c) => (
      <span className="text-sm text-gray-700 dark:text-gray-300">
        {c.last_communication_date
          ? new Date(c.last_communication_date).toLocaleString('en-GB', {
              day: '2-digit', month: '2-digit', year: 'numeric',
              hour: '2-digit', minute: '2-digit', hour12: false,
            })
          : '\u2014'}
      </span>
    ),
  },
  {
    key: 'card_type',
    label: 'Card Type',
    filterType: 'text',
    renderCell: (c) => (
      <span className="text-sm text-gray-600 dark:text-gray-400">{c.card_type || '—'}</span>
    ),
  },
  // CLAUD-127: Country column — CLAUD-143: changed to multiselect with real client countries
  {
    key: 'country',
    label: 'Country',
    sortKey: 'country',
    align: 'left' as const,
    minWidth: '120px',
    filterType: 'multiselect' as const,
    renderCell: (c) => (
      <span className="text-sm text-gray-700 dark:text-gray-300">
        {c.country ? (COUNTRY_ISO_TO_NAME[c.country.toUpperCase()] ?? c.country) : '—'}
      </span>
    ),
  },
  // CLAUD-128: Local Time column — derived client-side from country ISO
  {
    key: 'local_time',
    label: 'Local Time',
    align: 'left' as const,
    minWidth: '110px',
    filterType: 'none' as const,
    renderCell: (c) => <LocalTimeCell countryIso={c.country} />,
  },
  // CLAUD-121: New fields
  {
    key: 'last_deposit_date',
    label: 'Last Deposit',
    align: 'left' as const,
    minWidth: '130px',
    filterType: 'date' as const,
    filterParamKey: 'last_deposit',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{formatDate(c.last_deposit_date)}</span>,
  },
  {
    key: 'last_withdrawal_date',
    label: 'Last Withdrawal',
    align: 'left' as const,
    minWidth: '140px',
    filterType: 'date' as const,
    filterParamKey: 'last_withdrawal',
    renderCell: (c) => <span className="text-sm text-gray-700 dark:text-gray-300">{formatDate(c.last_withdrawal_date)}</span>,
  },
  {
    key: 'net_deposit_ever',
    label: 'Net Deposit Ever',
    align: 'right' as const,
    minWidth: '140px',
    filterType: 'numeric' as const,
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.net_deposit_ever != null && c.net_deposit_ever < 0 ? 'text-red-600' : 'text-gray-900 dark:text-gray-100'}`}>
        {c.net_deposit_ever != null ? `$${c.net_deposit_ever.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
      </span>
    ),
  },
  // CLAUD-121: Closed P&L
  {
    key: 'closed_pnl',
    label: 'Closed P&L',
    sortKey: 'closed_pnl' as SortCol,
    align: 'right' as const,
    minWidth: '120px',
    filterType: 'numeric' as const,
    renderCell: (c) => (
      <span className={`text-sm font-medium ${c.closed_pnl != null && c.closed_pnl < 0 ? 'text-red-600' : c.closed_pnl != null && c.closed_pnl > 0 ? 'text-green-600' : 'text-gray-700 dark:text-gray-300'}`}>
        {c.closed_pnl != null ? `$${c.closed_pnl.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}` : '—'}
      </span>
    ),
  },
  // CLAUD-138: Sales Potential (sales_client_potential from MV)
  {
    key: 'sales_client_potential',
    label: 'SCP',
    sortKey: 'sales_client_potential' as SortCol,
    align: 'left' as const,
    minWidth: '140px',
    filterType: 'multiselect' as const,
    filterParamKey: 'sales_client_potential',
    renderCell: (c) => (
      <span className="text-sm text-gray-700 dark:text-gray-300">
        {c.sales_client_potential ?? '—'}
      </span>
    ),
  },
  // CLAUD-165: MT Trading Account
  {
    key: 'mt_account',
    label: 'MT Account',
    align: 'left' as const,
    minWidth: '120px',
    filterType: 'none' as const,
    renderCell: (c) => (
      <span className="text-sm font-mono text-gray-700 dark:text-gray-300">
        {c.mt_account ?? '—'}
      </span>
    ),
  },
  // CLAUD-167: Legacy ID
  {
    key: 'legacy_id',
    label: 'Legacy ID',
    align: 'left' as const,
    minWidth: '120px',
    filterType: 'text' as const,
    filterParamKey: 'legacy_id',
    renderCell: (c) => (
      <span className="text-sm font-mono text-gray-700 dark:text-gray-300">
        {c.legacy_id ?? '—'}
      </span>
    ),
  },
  // CLAUD-167: Affiliate
  {
    key: 'affiliate',
    label: 'Affiliate',
    align: 'left' as const,
    minWidth: '140px',
    filterType: 'text' as const,
    filterParamKey: 'affiliate',
    renderCell: (c) => (
      <span className="text-sm text-gray-700 dark:text-gray-300">
        {c.affiliate ?? '—'}
      </span>
    ),
  },
];

// Keys of the draggable columns (excludes pinned: accountid, full_name)
const DEFAULT_COL_ORDER = DEFAULT_COLS.map((c) => c.key);

// Lookup map for quick access
const COL_DEF_MAP = Object.fromEntries(DEFAULT_COLS.map((c) => [c.key, c]));

export function RetentionPage() {
  // CRM permissions (CLAUD-148 export_data)
  const crmPerms = useAuthStore((s) => s.crmPermissions);
  const canExport = crmPerms['export_data'] !== false; // default true if not yet loaded

  // CLAUD-149: Dynamic retention statuses from report.ant_ret_status
  const { statuses: retentionStatuses } = useRetentionStatuses();

  // Override retention_status renderCell to use dynamic statuses; all other cols remain static
  const colDefMap = useMemo<Record<string, ColDef>>(() => ({
    ...COL_DEF_MAP,
    retention_status: {
      ...COL_DEF_MAP['retention_status'],
      renderCell: (c) => (
        <span className="text-sm text-gray-700 dark:text-gray-300">
          {resolveStatusDisplay(c.retention_status, retentionStatuses)}
        </span>
      ),
    },
  }), [retentionStatuses]);

  // Labels for the retention_status multiselect filter — only approved display statuses
  const retentionStatusLabels = [
    'Appointment', 'Call Again', 'Daily Trading with me', 'New', 'No Answer',
    'Potential', 'Reassigned', 'Remove From my Portfolio', 'Terminated/Complain/Legal',
  ];

  const [data, setData] = useState<{ total: number; clients: RetentionClient[] } | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [sortBy, setSortBy] = useState<SortCol>('score');
  const [sortDir, setSortDir] = useState<'asc' | 'desc'>('desc');
  const [applied, setApplied] = useState<Filters>(EMPTY_FILTERS);
  // CLAUD-126: Active-only toggle — on by default, not persisted between sessions
  const [showActiveOnly, setShowActiveOnly] = useState(true);
  const activityDays = '35';
  const [agents, setAgents] = useState<{ id: string; name: string }[]>([]);
  const [taskList, setTaskList] = useState<{ id: number; name: string }[]>([]);
  const [salesPotentialOptions, setSalesPotentialOptions] = useState<string[]>([]);
  const [countryOptions, setCountryOptions] = useState<string[]>([]); // CLAUD-143
  const [selectedClient, setSelectedClient] = useState<RetentionClient | null>(null);
  const [colFilters, setColFilters] = useState<ColFilters>({});
  // Debounced colFilters that actually trigger the API call
  const [debouncedColFilters, setDebouncedColFilters] = useState<ColFilters>({});
  const colFiltersDebounceRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  // AbortController ref for cancelling in-flight retention requests (race condition fix)
  const loadAbortRef = useRef<AbortController | null>(null);

  // ── CLAUD-85: Column visibility per role ────────────────────────────────
  // Map visibility config keys to the actual column keys used in the grid
  const VIS_KEY_TO_COL_KEY: Record<string, string> = useMemo(() => ({
    score: 'score',
    balance: 'balance',
    live_equity: 'live_equity',
    exposure_usd: 'exposure_usd',
    exposure_pct: 'exposure_pct',
    open_pnl: 'open_pnl',
    retention_status: 'retention_status',
    last_contact: 'last_communication_date',
    assigned_to: 'agent_name',
    task_type: 'tasks',
    sales_client_potential: 'sales_client_potential',
  }), []);

  const [colVisibility, setColVisibility] = useState<Record<string, boolean> | null>(null);

  // Fetch column visibility on mount
  useEffect(() => {
    api.get('/retention/column-visibility')
      .then((res) => {
        setColVisibility(res.data.columns ?? null);
      })
      .catch(() => {
        // Silently fall back — show all columns
        setColVisibility(null);
      });
  }, []);

  // Compute hidden column keys from visibility config
  const hiddenColKeys = useMemo(() => {
    if (!colVisibility) return new Set<string>();
    const hidden = new Set<string>();
    for (const [visKey, isVisible] of Object.entries(colVisibility)) {
      if (!isVisible) {
        const colKey = VIS_KEY_TO_COL_KEY[visKey];
        if (colKey) hidden.add(colKey);
      }
    }
    return hidden;
  }, [colVisibility, VIS_KEY_TO_COL_KEY]);

  // Pinned column visibility
  const showFavorites = colVisibility ? (colVisibility['favorites'] !== false) : true;
  const showClientId = colVisibility ? (colVisibility['client_id'] !== false) : true;
  const showClientName = colVisibility ? (colVisibility['client_name'] !== false) : true;

  // ── Column order state ─────────────────────────────────────────────────
  const [colOrder, setColOrder] = useState<string[]>(DEFAULT_COL_ORDER);
  const [colOrderLoaded, setColOrderLoaded] = useState(false);
  // Drag state — track which column key is being dragged and which is the drop target
  const dragColRef = useRef<string | null>(null);
  const [dragOverCol, setDragOverCol] = useState<string | null>(null);

  // On mount: fetch saved column order from server
  useEffect(() => {
    api.get('/preferences/columns')
      .then((res) => {
        const saved: string[] | null = res.data?.column_order ?? null;
        if (Array.isArray(saved) && saved.length > 0) {
          // Merge: saved order first, then append any new columns not in saved order
          const validSaved = saved.filter((k) => COL_DEF_MAP[k]);
          const missing = DEFAULT_COL_ORDER.filter((k) => !validSaved.includes(k));
          setColOrder([...validSaved, ...missing]);
        }
      })
      .catch(() => {
        // Silently fall back to default order
      })
      .finally(() => setColOrderLoaded(true));
  }, []);

  // Persist column order to server (immediate after drop)
  const saveColOrder = useCallback((order: string[]) => {
    api.put('/preferences/columns', { column_order: order }).catch(() => {
      // Silently ignore persistence failures
    });
  }, []);

  // ── Drag & drop handlers ───────────────────────────────────────────────
  const handleDragStart = useCallback((key: string) => {
    dragColRef.current = key;
  }, []);

  const handleDragOver = useCallback((e: React.DragEvent, key: string) => {
    e.preventDefault(); // required to allow drop
    setDragOverCol(key);
  }, []);

  const handleDragLeave = useCallback(() => {
    setDragOverCol(null);
  }, []);

  const handleDrop = useCallback((e: React.DragEvent, targetKey: string) => {
    e.preventDefault();
    setDragOverCol(null);
    const sourceKey = dragColRef.current;
    if (!sourceKey || sourceKey === targetKey) return;
    setColOrder((prev) => {
      const next = [...prev];
      const fromIdx = next.indexOf(sourceKey);
      const toIdx = next.indexOf(targetKey);
      if (fromIdx === -1 || toIdx === -1) return prev;
      next.splice(fromIdx, 1);
      next.splice(toIdx, 0, sourceKey);
      saveColOrder(next);
      return next;
    });
    dragColRef.current = null;
  }, [saveColOrder]);

  const handleDragEnd = useCallback(() => {
    dragColRef.current = null;
    setDragOverCol(null);
  }, []);

  // Reset column order to default
  const resetColOrder = useCallback(() => {
    setColOrder(DEFAULT_COL_ORDER);
    saveColOrder(DEFAULT_COL_ORDER);
  }, [saveColOrder]);

  // ── CLAUD-122: Saved Searches ─────────────────────────────────────────
  interface SavedSearch {
    id: string; name: string; filters: any; column_order: string[];
    column_visibility: any; col_filters: any; sort_field: string | null;
    sort_direction: string | null; status_filter: string | null;
    created_at: string; updated_at: string;
  }
  const [savedSearches, setSavedSearches] = useState<SavedSearch[]>([]);
  const [activeSavedSearch, setActiveSavedSearch] = useState<SavedSearch | null>(null);
  const [savedSearchModified, setSavedSearchModified] = useState(false);
  const [showSavedSearchDropdown, setShowSavedSearchDropdown] = useState(false);
  const [showSaveModal, setShowSaveModal] = useState(false);
  const [saveModalName, setSaveModalName] = useState('');
  const [saveModalError, setSaveModalError] = useState('');
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState('');

  useEffect(() => {
    api.get('/retention/saved-searches').then((res) => setSavedSearches(res.data)).catch(() => {});
  }, []);

  const applySearch = useCallback((s: SavedSearch) => {
    setActiveSavedSearch(s);
    setSavedSearchModified(false);
    setShowSavedSearchDropdown(false);
    if (s.sort_field) setSortBy(s.sort_field as SortCol);
    if (s.sort_direction) setSortDir(s.sort_direction as 'asc' | 'desc');
    if (s.col_filters) setColFilters(s.col_filters);
    if (Array.isArray(s.column_order) && s.column_order.length > 0) {
      const validSaved = s.column_order.filter((k: string) => COL_DEF_MAP[k]);
      const missing = DEFAULT_COL_ORDER.filter((k) => !validSaved.includes(k));
      setColOrder([...validSaved, ...missing]);
    }
    setPage(1);
  }, []);

  const clearActiveSavedSearch = useCallback(() => {
    setActiveSavedSearch(null);
    setSavedSearchModified(false);
  }, []);

  const handleSaveSearch = useCallback(async (overwriteId?: string) => {
    const name = saveModalName.trim();
    if (!name) { setSaveModalError('Name is required.'); return; }
    const payload = {
      name, filters: applied, column_order: colOrder,
      column_visibility: colVisibility || {}, col_filters: colFilters,
      sort_field: sortBy, sort_direction: sortDir,
    };
    try {
      if (overwriteId) {
        await api.put(`/retention/saved-searches/${overwriteId}`, payload);
        setSavedSearches((prev) => prev.map((s) => s.id === overwriteId ? { ...s, ...payload } : s));
      } else {
        const res = await api.post('/retention/saved-searches', payload);
        const newSearch: SavedSearch = { id: res.data.id, ...payload, status_filter: null, created_at: res.data.created_at, updated_at: res.data.created_at };
        setSavedSearches((prev) => [newSearch, ...prev]);
        setActiveSavedSearch(newSearch);
      }
      setSavedSearchModified(false);
      setShowSaveModal(false);
      setSaveModalName('');
      setSaveModalError('');
    } catch (err: any) {
      if (err.response?.status === 409) {
        setSaveModalError('A search with this name already exists.');
      } else {
        setSaveModalError('Failed to save. Please try again.');
      }
    }
  }, [saveModalName, applied, colOrder, colVisibility, colFilters, sortBy, sortDir]);

  const handleDeleteSearch = useCallback(async (id: string) => {
    if (!window.confirm('Delete this saved search? This cannot be undone.')) return;
    await api.delete(`/retention/saved-searches/${id}`).catch(() => {});
    setSavedSearches((prev) => prev.filter((s) => s.id !== id));
    if (activeSavedSearch?.id === id) clearActiveSavedSearch();
  }, [activeSavedSearch, clearActiveSavedSearch]);

  const handleRenameSearch = useCallback(async (id: string) => {
    const name = renameValue.trim();
    if (!name) return;
    const s = savedSearches.find((x) => x.id === id);
    if (!s) return;
    await api.put(`/retention/saved-searches/${id}`, { ...s, name }).catch(() => {});
    setSavedSearches((prev) => prev.map((x) => x.id === id ? { ...x, name } : x));
    if (activeSavedSearch?.id === id) setActiveSavedSearch((prev) => prev ? { ...prev, name } : prev);
    setRenamingId(null);
  }, [renameValue, savedSearches, activeSavedSearch]);

  // Toggle a client as a favorite (CLAUD-50)
  const toggleFavorite = useCallback(async (accountid: string, e: React.MouseEvent) => {
    e.stopPropagation();
    // Optimistic update
    setData((prev) => {
      if (!prev) return prev;
      return {
        ...prev,
        clients: prev.clients.map((c) =>
          c.accountid === accountid ? { ...c, is_favorite: !c.is_favorite } : c
        ),
      };
    });
    try {
      const res = await api.post<{ is_favorite: boolean }>(`/retention/favorites/${accountid}`);
      setData((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          clients: prev.clients.map((c) =>
            c.accountid === accountid ? { ...c, is_favorite: res.data.is_favorite } : c
          ),
        };
      });
    } catch {
      // Revert on error
      setData((prev) => {
        if (!prev) return prev;
        return {
          ...prev,
          clients: prev.clients.map((c) =>
            c.accountid === accountid ? { ...c, is_favorite: !c.is_favorite } : c
          ),
        };
      });
    }
  }, []);

  // Ordered column definitions (draggable columns only; pinned prepended in render)
  // CLAUD-85: Filter out columns hidden by role visibility config
  const orderedCols = useMemo(
    () => colOrder
      .map((k) => colDefMap[k])
      .filter((col): col is ColDef => !!col && !hiddenColKeys.has(col.key)),
    [colOrder, hiddenColKeys, colDefMap],
  );

  // Total column count: pinned (★ + accountid + full_name — only if visible) + ordered draggable cols
  const pinnedCount = (showFavorites ? 1 : 0) + (showClientId ? 1 : 0) + (showClientName ? 1 : 0);
  const totalColCount = pinnedCount + orderedCols.length;

  useEffect(() => {
    api.get('/retention/agents').then((r) => setAgents(r.data)).catch(() => {});
    api.get('/retention/tasks').then((r) => setTaskList(r.data)).catch(() => {});
    api.get('/retention/sales-potential-options').then((r) => setSalesPotentialOptions(r.data)).catch(() => {});
    api.get('/retention/countries').then((r) => setCountryOptions(r.data)).catch(() => {}); // CLAUD-143
  }, []);

  const load = async (p: number, col: SortCol, dir: 'asc' | 'desc', f: Filters, actDays: string, cf: ColFilters, activeOnly: boolean) => {
    // Cancel any in-flight request before starting a new one (prevents race
    // conditions when the user sorts or changes filters rapidly — CLAUD-37)
    if (loadAbortRef.current) loadAbortRef.current.abort();
    const controller = new AbortController();
    loadAbortRef.current = controller;

    setLoading(true);
    setError('');

    // Build col-filter params
    const colFilterParams: Record<string, string> = {};
    for (const [key, filter] of Object.entries(cf)) {
      if (!filter) continue;
      // Use filterParamKey override if defined (handles mismatches like agent_name→agent)
      const paramKey = COL_DEF_MAP[key]?.filterParamKey || key;
      if (filter.type === 'text' && filter.value) {
        colFilterParams[`filter_${paramKey}`] = filter.value;
      } else if (filter.type === 'numeric' && filter.val) {
        colFilterParams[`filter_${paramKey}_op`] = filter.op;
        colFilterParams[`filter_${paramKey}_val`] = filter.val;
        if (filter.op === 'between' && filter.val2) {
          colFilterParams[`filter_${paramKey}_val2`] = filter.val2;
        }
      } else if (filter.type === 'date') {
        if (filter.preset && filter.preset !== 'custom') {
          colFilterParams[`filter_${paramKey}_preset`] = filter.preset;
        } else if (filter.from) {
          colFilterParams[`filter_${paramKey}_from`] = filter.from;
          if (filter.to) colFilterParams[`filter_${paramKey}_to`] = filter.to;
        }
      } else if (filter.type === 'multiselect' && filter.values.length > 0) {
        if (key === 'tasks') {
          colFilterParams['filter_task_types'] = filter.values.join(',');
        } else {
          colFilterParams[`filter_${paramKey}`] = filter.values.join(',');
        }
      }
    }

    try {
      const res = await api.get('/retention/clients', {
        signal: controller.signal,
        params: {
          page: p, page_size: PAGE_SIZE, sort_by: col, sort_dir: dir,
          accountid: f.accountid,
          ...colFilterParams,
          qual_date_from: f.qual_date_from || undefined,
          qual_date_to: f.qual_date_to || undefined,
          trade_count_op: f.trade_count_op, trade_count_val: f.trade_count_val || undefined,
          days_op: f.days_op, days_val: f.days_val || undefined,
          profit_op: f.profit_op, profit_val: f.profit_val || undefined,
          last_trade_from: f.last_trade_from || undefined,
          last_trade_to: f.last_trade_to || undefined,
          days_from_last_trade_op: f.days_from_last_trade_op, days_from_last_trade_val: f.days_from_last_trade_val || undefined,
          deposit_count_op: f.deposit_count_op, deposit_count_val: f.deposit_count_val || undefined,
          total_deposit_op: f.total_deposit_op, total_deposit_val: f.total_deposit_val || undefined,
          balance_op: f.balance_op, balance_val: f.balance_val || undefined,
          credit_op: f.credit_op, credit_val: f.credit_val || undefined,
          equity_op: f.equity_op, equity_val: f.equity_val || undefined,
          live_equity_op: f.live_equity_op, live_equity_val: f.live_equity_val || undefined,
          max_open_trade_op: f.max_open_trade_op, max_open_trade_val: f.max_open_trade_val || undefined,
          max_volume_op: f.max_volume_op, max_volume_val: f.max_volume_val || undefined,
          turnover_op: f.turnover_op, turnover_val: f.turnover_val || undefined,
          assigned_to: f.assigned_to || undefined,
          task_id: f.task_id || undefined,
          // CLAUD-126: activeOnly toggle overrides the panel's active filter
          active: activeOnly ? 'true' : f.active,
          active_ftd: f.active_ftd,
          favorites_only: f.favorites_only || undefined,
          activity_days: actDays || 35,
        },
      });
      setData(res.data);
    } catch (err: any) {
      // Ignore aborted requests — they are superseded by a newer request
      if (err.code === 'ERR_CANCELED' || err.name === 'CanceledError') return;
      setError(err.response?.data?.detail || 'Failed to load retention data');
    } finally {
      setLoading(false);
    }
  };

  // CLAUD-144: Export current grid to Excel
  const [exportLoading, setExportLoading] = useState(false);
  const handleExport = async () => {
    setExportLoading(true);
    try {
      // Build column filter params (same as load())
      const cf = debouncedColFilters;
      const colFilterParams: Record<string, string> = {};
      for (const [key, filter] of Object.entries(cf)) {
        if (!filter) continue;
        const paramKey = COL_DEF_MAP[key]?.filterParamKey || key;
        if (filter.type === 'text' && filter.value) {
          colFilterParams[`filter_${paramKey}`] = filter.value;
        } else if (filter.type === 'multiselect' && filter.values.length > 0) {
          if (key === 'tasks') {
            colFilterParams['filter_task_types'] = filter.values.join(',');
          } else {
            colFilterParams[`filter_${paramKey}`] = filter.values.join(',');
          }
        }
      }
      const res = await api.get('/retention/export', {
        responseType: 'blob',
        params: {
          sort_by: sortBy, sort_dir: sortDir,
          accountid: applied.accountid || undefined,
          ...colFilterParams,
          active: showActiveOnly ? 'true' : applied.active,
          active_ftd: applied.active_ftd || undefined,
          assigned_to: applied.assigned_to || undefined,
          activity_days: activityDays || 35,
          filter_balance_op: applied.balance_op || undefined,
          filter_balance_val: applied.balance_val ? parseFloat(applied.balance_val) : undefined,
          filter_credit_op: applied.credit_op || undefined,
          filter_credit_val: applied.credit_val ? parseFloat(applied.credit_val) : undefined,
          filter_equity_op: applied.equity_op || undefined,
          filter_equity_val: applied.equity_val ? parseFloat(applied.equity_val) : undefined,
        },
      });
      const url = URL.createObjectURL(new Blob([res.data]));
      const link = document.createElement('a');
      link.href = url;
      link.download = `retention-grid-export-${new Date().toISOString().slice(0, 10)}.xlsx`;
      link.click();
      URL.revokeObjectURL(url);
    } catch {
      alert('Export failed. Please try again.');
    } finally {
      setExportLoading(false);
    }
  };

  // Debounce colFilters changes → update debouncedColFilters after 400ms
  useEffect(() => {
    if (colFiltersDebounceRef.current) clearTimeout(colFiltersDebounceRef.current);
    colFiltersDebounceRef.current = setTimeout(() => {
      setDebouncedColFilters(colFilters);
      setPage(1);
    }, 400);
    return () => {
      if (colFiltersDebounceRef.current) clearTimeout(colFiltersDebounceRef.current);
    };
  }, [colFilters]);

  useEffect(() => { load(page, sortBy, sortDir, applied, activityDays, debouncedColFilters, showActiveOnly); }, [page, sortBy, sortDir, applied, activityDays, debouncedColFilters, showActiveOnly]);

  const handleSort = (col: SortCol) => {
    if (sortBy === col) setSortDir((d) => (d === 'asc' ? 'desc' : 'asc'));
    else { setSortBy(col); setSortDir('asc'); }
    setPage(1);
  };

  const clearFilters = () => { setApplied(EMPTY_FILTERS); setPage(1); };
  const clearColFilters = () => { setColFilters({}); };

  const activeColFilterCount = useMemo(() => Object.keys(colFilters).length, [colFilters]);

  const totalPages = data ? Math.ceil(data.total / PAGE_SIZE) : 0;
  const activeCount = countActive(applied);

  // ── Active filter chips ─────────────────────────────────────────────────
  const filterChips = useMemo(() => {
    const chips: { label: string; key: string; onDismiss: () => void }[] = [];
    const opLabel = (op: string) => ({ eq: '=', gt: '>', lt: '<', gte: '≥', lte: '≤' }[op] ?? op);
    const fmtD = (d: string) => d ? new Date(d + 'T00:00:00').toLocaleDateString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric' }) : '';
    const presetLabel: Record<string, string> = { today: 'Today', this_week: 'This Week', this_month: 'This Month' };
    const dismiss = (fields: Partial<Filters>) => () => {
      setApplied((prev) => ({ ...prev, ...fields }));
      setPage(1);
    };

    if (applied.accountid) chips.push({ key: 'accountid', label: `Account ID contains "${applied.accountid}"`, onDismiss: dismiss({ accountid: '' }) });
    if (applied.qual_date_from || applied.qual_date_to) chips.push({ key: 'qual_date', label: `Qual Date: ${fmtD(applied.qual_date_from)} – ${fmtD(applied.qual_date_to)}`, onDismiss: dismiss({ qual_date_from: '', qual_date_to: '' }) });
    if (applied.trade_count_op && applied.trade_count_val) chips.push({ key: 'trade_count', label: `Trades ${opLabel(applied.trade_count_op)} ${applied.trade_count_val}`, onDismiss: dismiss({ trade_count_op: '', trade_count_val: '' }) });
    if (applied.days_op && applied.days_val) chips.push({ key: 'days', label: `Days in Retention ${opLabel(applied.days_op)} ${applied.days_val}`, onDismiss: dismiss({ days_op: '', days_val: '' }) });
    if (applied.profit_op && applied.profit_val) chips.push({ key: 'profit', label: `Total Profit ${opLabel(applied.profit_op)} ${applied.profit_val}`, onDismiss: dismiss({ profit_op: '', profit_val: '' }) });
    if (applied.last_trade_preset && applied.last_trade_preset !== 'custom')
      chips.push({ key: 'last_trade', label: `Last Trade: ${presetLabel[applied.last_trade_preset] ?? applied.last_trade_preset}`, onDismiss: dismiss({ last_trade_preset: '', last_trade_from: '', last_trade_to: '' }) });
    else if (applied.last_trade_from || applied.last_trade_to)
      chips.push({ key: 'last_trade', label: `Last Trade: ${fmtD(applied.last_trade_from)} – ${fmtD(applied.last_trade_to)}`, onDismiss: dismiss({ last_trade_preset: '', last_trade_from: '', last_trade_to: '' }) });
    if (applied.days_from_last_trade_op && applied.days_from_last_trade_val) chips.push({ key: 'days_from_last_trade', label: `Days from Last Trade ${opLabel(applied.days_from_last_trade_op)} ${applied.days_from_last_trade_val}`, onDismiss: dismiss({ days_from_last_trade_op: '', days_from_last_trade_val: '' }) });
    if (applied.deposit_count_op && applied.deposit_count_val) chips.push({ key: 'deposit_count', label: `Deposits ${opLabel(applied.deposit_count_op)} ${applied.deposit_count_val}`, onDismiss: dismiss({ deposit_count_op: '', deposit_count_val: '' }) });
    if (applied.total_deposit_op && applied.total_deposit_val) chips.push({ key: 'total_deposit', label: `Total Deposit ${opLabel(applied.total_deposit_op)} ${applied.total_deposit_val}`, onDismiss: dismiss({ total_deposit_op: '', total_deposit_val: '' }) });
    if (applied.balance_op && applied.balance_val) chips.push({ key: 'balance', label: `Balance ${opLabel(applied.balance_op)} ${applied.balance_val}`, onDismiss: dismiss({ balance_op: '', balance_val: '' }) });
    if (applied.credit_op && applied.credit_val) chips.push({ key: 'credit', label: `Credit ${opLabel(applied.credit_op)} ${applied.credit_val}`, onDismiss: dismiss({ credit_op: '', credit_val: '' }) });
    if (applied.equity_op && applied.equity_val) chips.push({ key: 'equity', label: `Equity ${opLabel(applied.equity_op)} ${applied.equity_val}`, onDismiss: dismiss({ equity_op: '', equity_val: '' }) });
    if (applied.live_equity_op && applied.live_equity_val) chips.push({ key: 'live_equity', label: `Live Equity ${opLabel(applied.live_equity_op)} ${applied.live_equity_val}`, onDismiss: dismiss({ live_equity_op: '', live_equity_val: '' }) });
    if (applied.max_open_trade_op && applied.max_open_trade_val) chips.push({ key: 'max_open_trade', label: `Max Open Trade ${opLabel(applied.max_open_trade_op)} ${applied.max_open_trade_val}`, onDismiss: dismiss({ max_open_trade_op: '', max_open_trade_val: '' }) });
    if (applied.max_volume_op && applied.max_volume_val) chips.push({ key: 'max_volume', label: `Max Volume ${opLabel(applied.max_volume_op)} ${applied.max_volume_val}`, onDismiss: dismiss({ max_volume_op: '', max_volume_val: '' }) });
    if (applied.turnover_op && applied.turnover_val) chips.push({ key: 'turnover', label: `Turnover ${opLabel(applied.turnover_op)} ${applied.turnover_val}`, onDismiss: dismiss({ turnover_op: '', turnover_val: '' }) });
    if (applied.assigned_to) {
      const agentName = agents.find((a) => a.id === applied.assigned_to)?.name ?? applied.assigned_to;
      chips.push({ key: 'assigned_to', label: `Agent: ${agentName}`, onDismiss: dismiss({ assigned_to: '' }) });
    }
    if (applied.task_id) {
      const taskName = taskList.find((t) => String(t.id) === applied.task_id)?.name ?? `Task #${applied.task_id}`;
      chips.push({ key: 'task_id', label: `Task: ${taskName}`, onDismiss: dismiss({ task_id: '' }) });
    }
    if (applied.active) chips.push({ key: 'active', label: `Active: ${applied.active === 'true' ? 'Yes' : 'No'}`, onDismiss: dismiss({ active: '' }) });
    if (applied.active_ftd) chips.push({ key: 'active_ftd', label: `Active FTD: ${applied.active_ftd === 'true' ? 'Yes' : 'No'}`, onDismiss: dismiss({ active_ftd: '' }) });
    if (applied.favorites_only === 'true') chips.push({ key: 'favorites_only', label: '★ Favorites Only', onDismiss: dismiss({ favorites_only: '' }) });
    if (applied.favorites_only === 'false') chips.push({ key: 'favorites_only', label: 'Non-Favorites Only', onDismiss: dismiss({ favorites_only: '' }) });

    // Column header filters
    for (const [colKey, cf] of Object.entries(colFilters)) {
      if (!cf) continue;
      const colLabel = COL_DEF_MAP[colKey]?.label ?? colKey;
      const dismissCol = () => setColFilters((prev) => { const next = { ...prev }; delete next[colKey]; return next; });
      if (cf.type === 'text' && cf.value) {
        chips.push({ key: `col_${colKey}`, label: `${colLabel} contains "${cf.value}"`, onDismiss: dismissCol });
      } else if (cf.type === 'numeric' && cf.val) {
        const lbl = cf.op === 'between' ? `${colLabel} between ${cf.val} – ${cf.val2 ?? ''}` : `${colLabel} ${opLabel(cf.op)} ${cf.val}`;
        chips.push({ key: `col_${colKey}`, label: lbl, onDismiss: dismissCol });
      } else if (cf.type === 'date') {
        if (cf.preset && cf.preset !== 'custom') {
          chips.push({ key: `col_${colKey}`, label: `${colLabel}: ${presetLabel[cf.preset] ?? cf.preset}`, onDismiss: dismissCol });
        } else if (cf.from) {
          const rangeLabel = cf.to ? `${fmtD(cf.from)} – ${fmtD(cf.to)}` : `from ${fmtD(cf.from)}`;
          chips.push({ key: `col_${colKey}`, label: `${colLabel}: ${rangeLabel}`, onDismiss: dismissCol });
        }
      } else if (cf.type === 'multiselect' && cf.values.length > 0) {
        // Each selected value gets its own dismissible chip
        for (const val of cf.values) {
          chips.push({
            key: `col_${colKey}_${val}`,
            label: `${colLabel}: ${val}`,
            onDismiss: () => setColFilters((prev) => {
              const cur = prev[colKey];
              if (!cur || cur.type !== 'multiselect') return prev;
              const next = cur.values.filter((v) => v !== val);
              if (next.length === 0) {
                const updated = { ...prev };
                delete updated[colKey];
                return updated;
              }
              return { ...prev, [colKey]: { type: 'multiselect', values: next } };
            }),
          });
        }
      }
    }
    return chips;
  }, [applied, colFilters, agents, taskList]); // eslint-disable-line react-hooks/exhaustive-deps

  // Check if order differs from default
  const isCustomOrder = useMemo(
    () => colOrder.join(',') !== DEFAULT_COL_ORDER.join(','),
    [colOrder],
  );

  // ── Virtual scrolling setup ──
  const ROW_HEIGHT = 44; // estimated row height in px
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const clients = data?.clients ?? [];

  const rowVirtualizer = useVirtualizer({
    count: clients.length,
    getScrollElement: () => scrollContainerRef.current,
    estimateSize: useCallback(() => ROW_HEIGHT, []),
    overscan: 10,
  });

  // Render a column header <th>
  const renderColHeader = (col: ColDef) => {
    const isDragOver = dragOverCol === col.key;
    const baseClass = [
      'px-4 pt-3 pb-1 text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider whitespace-nowrap align-top select-none',
      col.sortKey ? 'cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700' : '',
      col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : 'text-left',
      col.minWidth ? `min-w-[${col.minWidth}]` : '',
      // Drag-over highlight
      isDragOver ? 'border-l-2 border-blue-500 bg-blue-50 dark:bg-blue-900/20' : '',
    ].filter(Boolean).join(' ');

    return (
      <th
        key={col.key}
        className={baseClass}
        draggable
        onDragStart={() => handleDragStart(col.key)}
        onDragOver={(e) => handleDragOver(e, col.key)}
        onDragLeave={handleDragLeave}
        onDrop={(e) => handleDrop(e, col.key)}
        onDragEnd={handleDragEnd}
        onClick={col.sortKey ? () => handleSort(col.sortKey!) : undefined}
        title="Drag to reorder"
        style={col.minWidth ? { minWidth: col.minWidth } : undefined}
      >
        <span className="flex items-center gap-0.5 cursor-grab active:cursor-grabbing">
          <span className="text-gray-300 dark:text-gray-600 text-xs mr-0.5" aria-hidden>⠿</span>
          {col.label}
          {col.sortKey && <SortIcon col={col.sortKey} sortBy={sortBy} sortDir={sortDir} />}
        </span>
        {col.filterType === 'text' && (
          <ColTextFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
        {col.filterType === 'numeric' && (
          <ColNumericFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
        {col.filterType === 'date' && (
          <ColDateFilter col={col.key} colFilters={colFilters} setColFilters={setColFilters} />
        )}
        {col.filterType === 'multiselect' && (
          <ColMultiSelectFilter
            col={col.key}
            colFilters={colFilters}
            setColFilters={setColFilters}
            options={
              col.key === 'retention_status'
                ? retentionStatusLabels
                : col.key === 'agent_name'
                ? agents.filter((a) => a.name).map((a) => a.name)
                : col.key === 'sales_client_potential'
                ? salesPotentialOptions
                : col.key === 'country'
                ? countryOptions
                : taskList.map((t) => t.name)
            }
          />
        )}
      </th>
    );
  };

  // Don't render until column order is loaded to avoid flicker
  if (!colOrderLoaded) {
    return (
      <div className="flex items-center justify-center py-20 text-sm text-gray-400 dark:text-gray-500">
        Loading...
      </div>
    );
  }

  return (
    <div className="space-y-4">
      {/* Active filter chips bar */}
      {filterChips.length > 0 && (
        <div className="flex flex-nowrap md:flex-wrap gap-2 items-center px-3 py-2.5 bg-blue-50 dark:bg-blue-900/20 border border-blue-100 dark:border-blue-800 rounded-lg overflow-x-auto">
          {filterChips.map((chip) => (
            <span key={chip.key} className="inline-flex items-center gap-1 px-2.5 py-1 bg-white dark:bg-gray-700 border border-blue-200 dark:border-blue-700 text-blue-800 dark:text-blue-300 text-xs font-medium rounded-full shadow-sm">
              {chip.label}
              <button onClick={chip.onDismiss} className="ml-0.5 text-blue-400 dark:text-blue-500 hover:text-blue-700 dark:hover:text-blue-300 leading-none text-sm font-bold" aria-label={`Remove filter: ${chip.label}`}>×</button>
            </span>
          ))}
          <button onClick={() => { clearFilters(); clearColFilters(); }} className="ml-auto text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 dark:hover:text-blue-200 font-medium underline whitespace-nowrap">
            Clear all
          </button>
        </div>
      )}

      {/* Table */}
      <div className="bg-white dark:bg-gray-800 rounded-lg shadow dark:shadow-gray-900 overflow-hidden">
        <div className="px-2 md:px-4 py-2 md:py-3 border-b border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-900 flex items-center justify-between gap-2 flex-wrap">
          <div className="flex items-center gap-2">
            <span className="text-sm text-gray-600 dark:text-gray-400">
              {loading ? 'Loading…' : `${data?.total?.toLocaleString() ?? 0} ${showActiveOnly ? 'active ' : ''}accounts${activeCount > 0 ? ' (filtered)' : ''} — showing ${((page - 1) * PAGE_SIZE) + 1}–${Math.min(page * PAGE_SIZE, data?.total ?? 0)}`}
            </span>
            {/* CLAUD-126: Active clients only toggle */}
            <label className="inline-flex items-center gap-1.5 cursor-pointer select-none">
              <input
                type="checkbox"
                checked={showActiveOnly}
                onChange={(e) => { setShowActiveOnly(e.target.checked); setPage(1); }}
                className="w-3.5 h-3.5 rounded border-gray-300 text-green-600 focus:ring-green-500"
              />
              <span className={`text-xs font-medium ${showActiveOnly ? 'text-green-700 dark:text-green-400' : 'text-gray-500 dark:text-gray-400'}`}>
                Active clients only
              </span>
            </label>
            {activeColFilterCount > 0 && (
              <button
                onClick={clearColFilters}
                className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-orange-300 text-orange-700 bg-orange-50 hover:bg-orange-100"
                title="Clear all column filters"
              >
                Clear Column Filters ({activeColFilterCount})
              </button>
            )}
            {isCustomOrder && (
              <button
                onClick={resetColOrder}
                className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700"
                title="Reset column order to default"
              >
                Reset Columns
              </button>
            )}

            {/* CLAUD-144/CLAUD-148: Export to Excel — hidden if user lacks export_data permission */}
            {canExport && (
              <button
                onClick={handleExport}
                disabled={exportLoading}
                className="inline-flex items-center gap-1.5 px-2.5 py-1 text-xs font-medium rounded-md border border-green-300 text-green-700 bg-green-50 hover:bg-green-100 disabled:opacity-50 disabled:cursor-not-allowed dark:border-green-700 dark:text-green-400 dark:bg-green-900/20 dark:hover:bg-green-900/30"
                title="Export current filtered grid to Excel"
              >
                {exportLoading ? 'Exporting…' : '↓ Export to Excel'}
              </button>
            )}

            {/* CLAUD-122: Saved Searches */}
            <div className="relative">
              <button
                onClick={() => setShowSavedSearchDropdown((v) => !v)}
                className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700"
              >
                🔖 Saved Searches {savedSearches.length > 0 && <span className="bg-gray-200 dark:bg-gray-700 rounded-full px-1.5 py-0.5">{savedSearches.length}</span>}
              </button>
              {showSavedSearchDropdown && (
                <div className="absolute right-0 top-full mt-1 w-72 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg z-50 overflow-hidden">
                  <div className="px-3 py-2 border-b border-gray-100 dark:border-gray-700 font-medium text-xs text-gray-700 dark:text-gray-300">Saved Searches</div>
                  {savedSearches.length === 0 && (
                    <div className="px-3 py-3 text-xs text-gray-500 dark:text-gray-400 italic">No saved searches yet.</div>
                  )}
                  {savedSearches.map((s) => (
                    <div key={s.id} className="px-3 py-2 border-b border-gray-50 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700/50">
                      {renamingId === s.id ? (
                        <div className="flex gap-1">
                          <input
                            className="flex-1 text-xs border border-gray-300 dark:border-gray-600 rounded px-1.5 py-0.5 bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100"
                            value={renameValue}
                            onChange={(e) => setRenameValue(e.target.value)}
                            onKeyDown={(e) => { if (e.key === 'Enter') handleRenameSearch(s.id); if (e.key === 'Escape') setRenamingId(null); }}
                            autoFocus
                          />
                          <button onClick={() => handleRenameSearch(s.id)} className="text-xs text-blue-600 font-medium">✓</button>
                          <button onClick={() => setRenamingId(null)} className="text-xs text-gray-400">✕</button>
                        </div>
                      ) : (
                        <div className="flex items-start justify-between gap-2">
                          <button onClick={() => applySearch(s)} className="flex-1 text-left">
                            <span className="text-xs font-medium text-gray-900 dark:text-gray-100 block">{s.name}</span>
                            <span className="text-[10px] text-gray-400">{new Date(s.updated_at).toLocaleDateString('en-GB')}</span>
                          </button>
                          <div className="flex gap-1 shrink-0">
                            <button onClick={() => { setRenamingId(s.id); setRenameValue(s.name); }} className="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200 text-xs" title="Rename">✏️</button>
                            <button onClick={() => handleDeleteSearch(s.id)} className="text-gray-400 hover:text-red-500 text-xs" title="Delete">🗑️</button>
                          </div>
                        </div>
                      )}
                    </div>
                  ))}
                  <div className="px-3 py-2">
                    <button
                      onClick={() => { setShowSavedSearchDropdown(false); setSaveModalName(''); setSaveModalError(''); setShowSaveModal(true); }}
                      className="w-full text-xs text-blue-600 dark:text-blue-400 font-medium hover:underline text-left"
                    >
                      + Save Current Search
                    </button>
                  </div>
                </div>
              )}
            </div>
            <button
              onClick={() => { setSaveModalName(''); setSaveModalError(''); setShowSaveModal(true); }}
              className="inline-flex items-center gap-1 px-2.5 py-1 text-xs font-medium rounded-md border border-blue-300 dark:border-blue-700 text-blue-700 dark:text-blue-300 bg-blue-50 dark:bg-blue-900/30 hover:bg-blue-100 dark:hover:bg-blue-900/50"
            >
              🔖 Save Search
            </button>
            {activeSavedSearch && (
              <span className="inline-flex items-center gap-1 px-2 py-0.5 bg-blue-100 dark:bg-blue-900/30 text-blue-800 dark:text-blue-300 text-xs font-medium rounded-full">
                🔖 {activeSavedSearch.name}{savedSearchModified ? ' (modified)' : ''}
                <button onClick={clearActiveSavedSearch} className="ml-0.5 text-blue-400 hover:text-blue-700 font-bold leading-none">×</button>
              </span>
            )}

            {/* Save Search Modal */}
            {showSaveModal && (
              <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40" onClick={() => setShowSaveModal(false)}>
                <div className="bg-white dark:bg-gray-800 rounded-xl shadow-xl p-6 w-80 mx-4" onClick={(e) => e.stopPropagation()}>
                  <h2 className="text-sm font-bold text-gray-900 dark:text-gray-100 mb-4">Save Search</h2>
                  <input
                    className="w-full border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-2 text-sm bg-white dark:bg-gray-900 text-gray-900 dark:text-gray-100 mb-1 focus:outline-none focus:ring-2 focus:ring-blue-500"
                    placeholder="Search name (max 40 chars)"
                    maxLength={40}
                    value={saveModalName}
                    onChange={(e) => { setSaveModalName(e.target.value); setSaveModalError(''); }}
                    onKeyDown={(e) => { if (e.key === 'Enter') handleSaveSearch(); }}
                    autoFocus
                  />
                  {saveModalError && <p className="text-xs text-red-500 mb-2">{saveModalError}</p>}
                  <p className="text-xs text-gray-500 dark:text-gray-400 mb-4">
                    Saves current filters, column order, and sort.
                  </p>
                  <div className="flex gap-2 justify-end">
                    <button onClick={() => setShowSaveModal(false)} className="px-3 py-1.5 text-xs font-medium rounded-lg border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-700">Cancel</button>
                    <button onClick={() => handleSaveSearch()} className="px-3 py-1.5 text-xs font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700">Save Search</button>
                  </div>
                </div>
              </div>
            )}
          </div>
          {totalPages > 1 && (
            <div className="flex items-center gap-2">
              <button onClick={() => setPage((p) => Math.max(1, p - 1))} disabled={page === 1 || loading} className="min-h-[44px] min-w-[44px] px-3 py-1 text-xs rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 disabled:opacity-40 hover:bg-gray-50 dark:hover:bg-gray-700">Prev</button>
              <span className="text-xs text-gray-500 dark:text-gray-400 whitespace-nowrap">{page} / {totalPages}</span>
              <button onClick={() => setPage((p) => Math.min(totalPages, p + 1))} disabled={page === totalPages || loading} className="min-h-[44px] min-w-[44px] px-3 py-1 text-xs rounded-md border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 disabled:opacity-40 hover:bg-gray-50 dark:hover:bg-gray-700">Next</button>
            </div>
          )}
        </div>

        {error && <div className="px-4 py-3 bg-red-50 dark:bg-red-900/20 border-b border-red-100 dark:border-red-800 text-sm text-red-600 dark:text-red-400">{error}</div>}

        {/* Scroll container: persistent horizontal scrollbar + fixed height for virtual scrolling + sticky header */}
        <div
          ref={scrollContainerRef}
          className="overflow-x-scroll overflow-y-auto"
          style={{ maxHeight: 'calc(100vh - 260px)' }}
        >
          <table className="w-full">
            <thead className="bg-gray-50 dark:bg-gray-900 sticky top-0 z-10 shadow-[0_1px_0_0_rgba(229,231,235,1)] dark:shadow-[0_1px_0_0_rgba(55,65,81,1)]">
              <tr>
                {/* Pinned: Favorite star — toggle favorites filter (CLAUD-85: conditionally visible) */}
                {showFavorites && (
                  <th
                    className="px-2 pt-3 pb-1 text-center text-xs font-medium uppercase tracking-wider whitespace-nowrap align-top w-10 min-w-[40px] cursor-pointer hover:bg-gray-100 dark:hover:bg-gray-700 select-none"
                    title={applied.favorites_only === 'true' ? 'Show all clients' : 'Show favorites only'}
                    onClick={() => {
                      const next: BoolFilter = applied.favorites_only === 'true' ? '' : 'true';
                      setApplied((prev) => ({ ...prev, favorites_only: next }));
                      setPage(1);
                    }}
                  >
                    <span className={applied.favorites_only === 'true' ? 'text-yellow-400' : 'text-gray-400'}>★</span>
                  </th>
                )}
                {/* Pinned: Account ID — not draggable (CLAUD-85: conditionally visible) */}
                {showClientId && (
                  <th className="px-4 pt-3 pb-1 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 dark:hover:bg-gray-700 whitespace-nowrap align-top min-w-[120px]" onClick={() => handleSort('accountid')}>
                    <span>Account ID <SortIcon col="accountid" sortBy={sortBy} sortDir={sortDir} /></span>
                    <ColTextFilter col="accountid" colFilters={colFilters} setColFilters={setColFilters} />
                  </th>
                )}
                {/* Pinned: Full Name — not draggable (CLAUD-85: conditionally visible) */}
                {showClientName && (
                  <th className="px-4 pt-3 pb-1 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider cursor-pointer select-none hover:bg-gray-100 dark:hover:bg-gray-700 whitespace-nowrap align-top min-w-[140px]" onClick={() => handleSort('full_name')}>
                    <span>Full Name <SortIcon col="full_name" sortBy={sortBy} sortDir={sortDir} /></span>
                    <ColTextFilter col="full_name" colFilters={colFilters} setColFilters={setColFilters} />
                  </th>
                )}
                {/* Draggable columns in current order */}
                {orderedCols.map((col) => renderColHeader(col))}
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={totalColCount} className="px-4 py-12 text-center text-sm text-gray-400 dark:text-gray-500">Loading…</td></tr>
              ) : !data || clients.length === 0 ? (
                <tr><td colSpan={totalColCount} className="px-4 py-12 text-center text-sm text-gray-400 dark:text-gray-500">No accounts found.</td></tr>
              ) : (
                <>
                  {/* Spacer for virtual scroll — pushes visible rows to correct offset */}
                  {rowVirtualizer.getVirtualItems().length > 0 && (
                    <tr style={{ height: rowVirtualizer.getVirtualItems()[0].start }}>
                      <td colSpan={totalColCount} style={{ padding: 0, border: 'none' }} />
                    </tr>
                  )}
                  {rowVirtualizer.getVirtualItems().map((virtualRow) => {
                    const c = clients[virtualRow.index];
                    return (
                      <tr
                        key={c.accountid}
                        data-index={virtualRow.index}
                        ref={rowVirtualizer.measureElement}
                        className="border-t border-gray-100 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-gray-700 cursor-pointer"
                        onDoubleClick={() => setSelectedClient(c)}
                      >
                        {/* Pinned: favorite star (CLAUD-85: conditionally visible) */}
                        {showFavorites && (
                          <td className="px-2 py-3 text-center w-10">
                            <button
                              onClick={(e) => toggleFavorite(c.accountid, e)}
                              className={`text-lg leading-none transition-colors ${c.is_favorite ? 'text-yellow-400 hover:text-yellow-500' : 'text-gray-300 hover:text-yellow-300'}`}
                              title={c.is_favorite ? 'Remove from favorites' : 'Add to favorites'}
                            >
                              ★
                            </button>
                          </td>
                        )}
                        {/* Pinned cells (CLAUD-85: conditionally visible) */}
                        {showClientId && (
                          <td className="px-4 py-3 text-sm font-medium">
                            <a href={`https://crm.cmtrading.com/#/users/user/${c.accountid}`} target="_blank" rel="noreferrer" className="text-blue-600 hover:underline">{c.accountid}</a>
                          </td>
                        )}
                        {showClientName && (
                          <td className="px-4 py-3 text-sm text-gray-700 dark:text-gray-300 whitespace-nowrap">{c.full_name || '\u2014'}</td>
                        )}
                        {/* Ordered draggable cells */}
                        {orderedCols.map((col) => (
                          <td
                            key={col.key}
                            className={`px-4 py-3 ${col.align === 'right' ? 'text-right' : col.align === 'center' ? 'text-center' : ''}`}
                          >
                            {col.renderCell(c)}
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                  {/* Bottom spacer for virtual scroll */}
                  {rowVirtualizer.getVirtualItems().length > 0 && (
                    <tr style={{ height: rowVirtualizer.getTotalSize() - (rowVirtualizer.getVirtualItems()[rowVirtualizer.getVirtualItems().length - 1].end) }}>
                      <td colSpan={totalColCount} style={{ padding: 0, border: 'none' }} />
                    </tr>
                  )}
                </>
              )}
            </tbody>
          </table>
        </div>
      </div>

      {/* Client Actions Modal (triggered by double-click) */}
      {selectedClient && (
        <ClientActionsModal
          client={selectedClient}
          onClose={() => setSelectedClient(null)}
        />
      )}
    </div>
  );
}

import csv
import hashlib
import io
import time
from datetime import date
from typing import Any

import openpyxl
from openpyxl.styles import Font

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

from app.auth_deps import get_current_user, get_jwt_payload, JWTPayload
from app.pg_database import get_db
from app.rbac import get_client_scope_filter, make_page_guard
from app.database import execute_query

router = APIRouter()

# ---------------------------------------------------------------------------
# CLAUD-149: Dynamic retention statuses cache (1-hour TTL)
# ---------------------------------------------------------------------------
_statuses_cache: dict = {"data": None, "at": 0.0}


@router.get("/retention/statuses")
async def get_retention_statuses():
    """Return all retention statuses from report.ant_ret_status (type='2'), ordered by status_key."""
    now = time.time()
    if _statuses_cache["data"] is not None and now - _statuses_cache["at"] < 3600:
        return _statuses_cache["data"]
    rows = await execute_query(
        "SELECT status_key, value FROM report.ant_ret_status WHERE type='2' ORDER BY status_key ASC",
        (),
    )
    result = [{"key": r["status_key"], "label": r["value"]} for r in rows]
    _statuses_cache["data"] = result
    _statuses_cache["at"] = now
    return result

# CLAUD-124: Server-side page guard — returns 403 if the caller's role
# does not have the 'retention' permission.
_require_retention = make_page_guard("retention")

# ---------------------------------------------------------------------------
# Approved retention status mapping (CLAUD-74).
# The CRM stores numeric IDs in sales_client_potential; the UI must show names.
# Keys are the string representations of the CRM numeric codes.
# ---------------------------------------------------------------------------
_STATUS_ID_TO_NAME: dict[str, str] = {
    "0":  "New",
    "1":  "CallBack",
    "2":  "Invalid",
    "3":  "No Answer",
    "4":  "Reassign - Has Potential",
    "5":  "Reassign - No Potential",
    "6":  "Not Interested",
    "7":  "Under 18",
    "8":  "Wrong Language",
    "9":  "Deposited With Me",
    "10": "Sessions Only",
    "11": "Recovery",
    "12": "Depositor",
    "13": "Received Withdrawal",
    "14": "Never Answers",
    "15": "AvailableInNinja",
    "17": "Recycle",
    "18": "Never answer",
    "19": "Potential",
    "20": "Appointment",
    "21": "High Potential",
    "22": "Reshuffle",
    "23": "Call Again",
    "24": "Low potential",
    "25": "Auto Trading",
    "26": "No Balance",
    "27": "IB",
    "28": "Reassigned",
    "29": "Language barrier",
    "30": "Potential IB",
    "32": "Wrong Details",
    "33": "Don't want assistance",
    "34": "Terminated/Complain/Legal",
    "35": "Remove From my Portfolio",
    "36": "Daily Trading with me",
    "37": "A+ Client",
}
_STATUS_NAME_TO_ID: dict[str, str] = {v: k for k, v in _STATUS_ID_TO_NAME.items()}

# CLAUD-134: Only these 9 labels are approved for display in the retention grid.
# Legacy CRM codes (e.g. "7" → "Under 18") must not leak through the fallback.
_APPROVED_STATUS_NAMES: frozenset[str] = frozenset({
    "New", "No Answer", "Potential", "Appointment", "Call Again",
    "Reassigned", "Terminated/Complain/Legal", "Remove From my Portfolio",
    "Daily Trading with me",
})


def _resolve_status_name(raw: str | None) -> str | None:
    """Translate a numeric status ID to its human-readable name.

    If the value is already a known name (idempotent), return it as-is.
    If unknown, return the raw value unchanged so data is never silently lost.
    """
    if raw is None:
        return None
    s = str(raw).strip()
    if s in _STATUS_ID_TO_NAME:
        return _STATUS_ID_TO_NAME[s]
    # Already a name or an unknown value -- return as-is
    return s if s else None


def _resolve_display_status(status_id: str | None) -> str:
    """Map a CRM numeric status ID to an approved display name.

    Returns "New" if the ID is unknown or maps to a non-approved status.
    """
    if status_id is None:
        return "New"
    name = _STATUS_ID_TO_NAME.get(str(status_id).strip())
    if name is None or name not in _APPROVED_STATUS_NAMES:
        return "New"
    return name

# CLAUD-143: Country ISO → full name mapping (covers all countries in client data)
_ISO_TO_COUNTRY: dict[str, str] = {
    "AE": "United Arab Emirates", "AO": "Angola", "AR": "Argentina",
    "AU": "Australia", "AW": "Aruba", "AZ": "Azerbaijan",
    "BD": "Bangladesh", "BF": "Burkina Faso", "BG": "Bulgaria",
    "BH": "Bahrain", "BR": "Brazil", "BW": "Botswana",
    "BY": "Belarus", "BZ": "Belize", "CA": "Canada",
    "CD": "Democratic Republic of the Congo", "CH": "Switzerland",
    "CI": "Ivory Coast", "CM": "Cameroon", "CN": "China",
    "CO": "Colombia", "CY": "Cyprus", "DE": "Germany",
    "DK": "Denmark", "DZ": "Algeria", "EG": "Egypt",
    "ET": "Ethiopia", "FJ": "Fiji", "GB": "United Kingdom",
    "GH": "Ghana", "GR": "Greece", "GT": "Guatemala",
    "GY": "Guyana", "HK": "Hong Kong", "HN": "Honduras",
    "ID": "Indonesia", "IE": "Ireland", "IN": "India",
    "IQ": "Iraq", "IT": "Italy", "JM": "Jamaica",
    "JO": "Jordan", "JP": "Japan", "KE": "Kenya",
    "KH": "Cambodia", "KW": "Kuwait", "LB": "Lebanon",
    "LK": "Sri Lanka", "LR": "Liberia", "LS": "Lesotho",
    "MD": "Moldova", "MU": "Mauritius", "MV": "Maldives",
    "MX": "Mexico", "MY": "Malaysia", "MZ": "Mozambique",
    "NA": "Namibia", "NG": "Nigeria", "NI": "Nicaragua",
    "NL": "Netherlands", "NO": "Norway", "NZ": "New Zealand",
    "OM": "Oman", "PA": "Panama", "PE": "Peru",
    "PH": "Philippines", "PK": "Pakistan", "PL": "Poland",
    "QA": "Qatar", "RO": "Romania", "RS": "Serbia",
    "RW": "Rwanda", "SA": "Saudi Arabia", "SE": "Sweden",
    "SG": "Singapore", "SL": "Sierra Leone", "SN": "Senegal",
    "SS": "South Sudan", "SV": "El Salvador", "SY": "Syria",
    "SZ": "Eswatini", "TG": "Togo", "TH": "Thailand",
    "TR": "Turkey", "TW": "Taiwan", "TZ": "Tanzania",
    "UA": "Ukraine", "UG": "Uganda", "US": "United States",
    "UZ": "Uzbekistan", "VG": "British Virgin Islands",
    "VN": "Vietnam", "ZA": "South Africa", "ZM": "Zambia",
    "ZW": "Zimbabwe",
}
_COUNTRY_TO_ISO: dict[str, str] = {v: k for k, v in _ISO_TO_COUNTRY.items()}

# ---------------------------------------------------------------------------
# COUNT cache: keyed by (where_clause_hash, params_hash) with 60s TTL.
# Avoids a full MV scan on every page flip — the total rarely changes mid-session.
# ---------------------------------------------------------------------------
_count_cache: dict = {}  # key -> (count, expires_at)
_COUNT_TTL = 60  # seconds


def _cached_count_key(where_clause: str, params: dict) -> str:
    raw = where_clause + repr(sorted(params.items()))
    return hashlib.md5(raw.encode()).hexdigest()  # noqa: S324

# active = had a trade OR deposit in last N days, OR currently has open position(s)
# CLAUD-125: 3rd criterion — has open position(s) right now (via open_pnl_cache, synced every 3 min)
_MV_ACTIVE = (
    "COALESCE("
    "m.last_trade_date > CURRENT_DATE - make_interval(days => :activity_days)"
    " OR m.last_deposit_time > CURRENT_DATE - make_interval(days => :activity_days)"
    " OR EXISTS (SELECT 1 FROM open_pnl_cache opc WHERE opc.login = m.accountid::text)"
    ", false)"
)
_MV_ACTIVE_FTD = (
    f"(m.client_qualification_date > CURRENT_DATE - INTERVAL '7 days' AND {_MV_ACTIVE})"
)

# Each value is a SQL expression used in ORDER BY.
# The query builder appends "NULLS LAST" for all columns so NULLs always
# sort to the bottom regardless of direction.
#
# Numeric columns: expressions that must compare as numbers are kept as their
# native numeric MV column/expression — PostgreSQL sorts these correctly when
# the column type is numeric/float.  The one exception is sales_client_potential
# which is stored as TEXT and must be cast explicitly.
#
# "score" is computed per-page in Python (not stored in retention_mv), so
# server-side sorting by score is not available; it falls back to accountid.
_SORT_COLS = {
    # --- text columns ---
    "accountid":            "m.accountid",
    "full_name":            "m.full_name",
    "assigned_to":          "m.assigned_to",
    "agent_name":           "m.agent_name",

    # --- date / timestamp columns (already correct types in MV) ---
    "client_qualification_date": "m.client_qualification_date",
    "last_trade_date":      "m.last_trade_date",
    "days_from_last_trade": "m.last_trade_date",   # sort by when last trade was opened

    # --- integer/numeric columns (native numeric type in MV) ---
    "days_in_retention":    "(CURRENT_DATE - m.client_qualification_date)",
    "trade_count":          "m.trade_count",
    "total_profit":         "m.total_profit",
    "closed_pnl":           "m.total_profit",
    "deposit_count":        "m.deposit_count",
    "total_deposit":        "m.total_deposit",
    "balance":              "m.total_balance",
    "credit":               "m.total_credit",
    "equity":               "m.total_equity",
    "max_open_trade":       "m.max_open_trade",
    "max_volume":           "m.max_volume",
    "win_rate":             "m.win_rate",
    "avg_trade_size":       "m.avg_trade_size",
    "age":                  "EXTRACT(year FROM AGE(m.birth_date))",

    # --- computed numeric expressions ---
    "live_equity":          "ABS(m.total_balance + m.total_credit)",  # MV proxy (excludes live open_pnl)
    "open_pnl":             "m.total_equity",  # proxy; open_pnl is fetched live
    "turnover":             (
        "CASE WHEN m.total_equity != 0"
        " THEN m.max_volume / ABS(m.total_equity)"
        " ELSE NULL END"
    ),
    "exposure_pct":         "exp.exposure_pct",
    "exposure_usd":         "COALESCE(exp.exposure_usd, 0)",

    # --- boolean expressions ---
    "active":               _MV_ACTIVE,
    "active_ftd":           _MV_ACTIVE_FTD,

    # --- retention status (CLAUD-114: from client_retention_status, not legacy sales_client_potential) ---
    "retention_status": "crs.status_label",

    # --- score: pre-computed in client_scores table, joined at query time ---
    "score":                "COALESCE(cs.score, 0)",

    # --- last communication date from audit_log (CLAUD-58) ---
    "last_communication_date": "lc.last_communication_date",

    # --- CLAUD-116: sales client potential & segmentation ---
    "client_potential": "csp.client_potential",
    "client_segment":   "csp.client_segment",

    # --- CLAUD-138: raw sales_client_potential from MV ---
    "sales_client_potential": "m.sales_client_potential",

    # --- CLAUD-127: client country from ant_acc ---
    "country": "aa.country_iso",
}

_OP_MAP = {"eq": "=", "gt": ">", "lt": "<", "gte": ">=", "lte": "<="}

# Valid operators including "between" (requires two values)
_VALID_OPS = set(_OP_MAP.keys()) | {"between"}

# CLAUD-77: Simple LEFT JOIN on pre-computed account_exposure_cache (replaces expensive subquery)
_EXPOSURE_JOIN = (
    " LEFT JOIN account_exposure_cache exp ON exp.accountid = m.accountid"
)

# SQL fragment to join user_favorites for the current user (CLAUD-50)
_FAVORITES_JOIN = (
    " LEFT JOIN user_favorites uf"
    " ON uf.accountid = m.accountid AND uf.user_id = :current_user_id"
)

# SQL fragment to join client_card_type (IIN/BIN lookup result)
_CARD_TYPE_JOIN = (
    " LEFT JOIN client_card_type cct ON cct.accountid = m.accountid"
)

# CLAUD-114: Join locally-stored retention status (set via PUT /retention-status)
_RETENTION_STATUS_JOIN = (
    " LEFT JOIN client_retention_status crs ON crs.accountid = m.accountid"
)

# CLAUD-116: Join sales client potential & segmentation profile
_SALES_PROFILE_JOIN = (
    " LEFT JOIN client_sales_profile csp ON csp.account_id = m.accountid"
)

# CLAUD-121: Join ant_acc for net_deposit_ever and total_withdrawal
_ANT_ACC_JOIN = (
    " LEFT JOIN ant_acc aa ON aa.accountid = m.accountid"
)

# CLAUD-165: Join vtiger_trading_accounts + dealio_users to get MT account login(s) per client
# Priority: logins with positive equity first; if none, fall back to all logins.
# CLAUD-171: Also capture leverage (from dealio_users) for margin calculations.
_MT_ACCOUNT_JOIN = (
    " LEFT JOIN ("
    "  SELECT ta.vtigeraccountid AS accountid,"
    "         COALESCE("
    "           NULLIF(STRING_AGG(CASE WHEN COALESCE(du.equity, 0) > 0 THEN ta.login::TEXT END, ', '), ''),"
    "           STRING_AGG(ta.login::TEXT, ', ')"
    "         ) AS mt_account,"
    "         COALESCE("
    "           MAX(CASE WHEN COALESCE(du.equity, 0) > 0 THEN NULLIF(du.leverage, 0) END),"
    "           NULLIF(MAX(du.leverage), 0)"
    "         ) AS leverage"
    "  FROM vtiger_trading_accounts ta"
    "  LEFT JOIN dealio_users du ON du.login = ta.login"
    "  GROUP BY ta.vtigeraccountid"
    " ) mt ON mt.accountid = m.accountid"
)

# CLAUD-121: Join vtiger_mttransactions via vtiger_trading_accounts for last_withdrawal_date
_LAST_WITHDRAWAL_JOIN = (
    " LEFT JOIN ("
    "  SELECT ta.vtigeraccountid AS accountid,"
    "         MAX(tx.modifiedtime) AS last_withdrawal_date"
    "  FROM vtiger_mttransactions tx"
    "  JOIN vtiger_trading_accounts ta ON ta.login = tx.login"
    "  WHERE tx.transactiontype IN ('Withdrawal', 'Withdraw')"
    "  GROUP BY ta.vtigeraccountid"
    " ) lw ON lw.accountid = m.accountid"
)

# SQL fragment to join audit_log last communication date (CLAUD-58)
_LAST_COMM_JOIN = (
    " LEFT JOIN ("
    "  SELECT al.client_account_id,"
    "         MAX(al.timestamp) AS last_communication_date"
    "  FROM audit_log al"
    "  WHERE al.action_type IN ('status_change','note_added','call_initiated','whatsapp_opened')"
    "  GROUP BY al.client_account_id"
    " ) lc ON lc.client_account_id = m.accountid"
)


def _num_cond(op: str, expr: str, param: str, param2: str | None = None) -> str | None:
    """Build a numeric WHERE condition.

    For op='between', param2 must be provided; returns BETWEEN clause.
    For all other ops, uses _OP_MAP for the SQL operator.
    Returns None if the operator is unrecognised.
    """
    if op == "between":
        if param2 is None:
            return None
        return f"{expr} BETWEEN :{param} AND :{param2}"
    sql_op = _OP_MAP.get(op)
    return f"{expr} {sql_op} :{param}" if sql_op else None


def _date_preset_cond(expr: str, preset: str) -> str | None:
    """Return a SQL fragment for a named date preset (today / this_week / this_month).

    expr must be a date-typed SQL expression (cast if needed).
    Returns None for unknown preset values.
    """
    if preset == "today":
        return f"{expr} = CURRENT_DATE"
    if preset == "this_week":
        return f"{expr} >= date_trunc('week', CURRENT_DATE) AND {expr} < date_trunc('week', CURRENT_DATE) + INTERVAL '7 days'"
    if preset == "this_month":
        return f"{expr} >= date_trunc('month', CURRENT_DATE) AND {expr} < date_trunc('month', CURRENT_DATE) + INTERVAL '1 month'"
    return None


@router.get("/retention/clients")
async def get_retention_clients(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    sort_by: str = Query("accountid"),
    sort_dir: str = Query("asc"),
    accountid: str = Query(""),
    filter_accountid: str = Query(""),  # column header filter variant
    # numeric filters
    trade_count_op: str = Query(""),
    trade_count_val: float | None = Query(None),
    days_op: str = Query(""),
    days_val: float | None = Query(None),
    profit_op: str = Query(""),
    profit_val: float | None = Query(None),
    days_from_last_trade_op: str = Query(""),
    days_from_last_trade_val: float | None = Query(None),
    deposit_count_op: str = Query(""),
    deposit_count_val: float | None = Query(None),
    total_deposit_op: str = Query(""),
    total_deposit_val: float | None = Query(None),
    balance_op: str = Query(""),
    balance_val: float | None = Query(None),
    credit_op: str = Query(""),
    credit_val: float | None = Query(None),
    equity_op: str = Query(""),
    equity_val: float | None = Query(None),
    live_equity_op: str = Query(""),
    live_equity_val: float | None = Query(None),
    max_open_trade_op: str = Query(""),
    max_open_trade_val: float | None = Query(None),
    max_volume_op: str = Query(""),
    max_volume_val: float | None = Query(None),
    turnover_op: str = Query(""),
    turnover_val: float | None = Query(None),
    exposure_usd_op: str = Query(""),
    exposure_usd_val: float | None = Query(None),
    # date range filters
    qual_date_from: str = Query(""),
    qual_date_to: str = Query(""),
    last_trade_from: str = Query(""),
    last_trade_to: str = Query(""),
    # agent filter
    assigned_to: str = Query(""),
    # task filter
    task_id: int | None = Query(None),
    # boolean filters
    active: str = Query(""),
    active_ftd: str = Query(""),
    # activity window
    activity_days: int = Query(35, ge=1, le=365),
    # -----------------------------------------------------------------------
    # Per-column text filters (ILIKE contains)
    # -----------------------------------------------------------------------
    filter_full_name: str = Query(""),
    filter_status: str = Query(""),       # maps to m.sales_client_potential (no retention_status column in MV)
    filter_agent: str = Query(""),        # ILIKE match on resolved agent name via vtiger_users subquery
    filter_card_type: str = Query(""),    # ILIKE match on cct.card_type
    filter_country: str = Query(""),      # CLAUD-143: comma-separated country names → ISO filter on aa.country_iso
    # -----------------------------------------------------------------------
    # Per-column numeric filters with operator + optional second value (between)
    # -----------------------------------------------------------------------
    filter_balance_op: str = Query(""),
    filter_balance_val: float | None = Query(None),
    filter_balance_val2: float | None = Query(None),
    filter_credit_op: str = Query(""),
    filter_credit_val: float | None = Query(None),
    filter_credit_val2: float | None = Query(None),
    filter_equity_op: str = Query(""),
    filter_equity_val: float | None = Query(None),
    filter_equity_val2: float | None = Query(None),
    filter_live_equity_op: str = Query(""),
    filter_live_equity_val: float | None = Query(None),
    filter_live_equity_val2: float | None = Query(None),
    filter_max_open_trade_op: str = Query(""),
    filter_max_open_trade_val: float | None = Query(None),
    filter_max_open_trade_val2: float | None = Query(None),
    filter_max_volume_op: str = Query(""),
    filter_max_volume_val: float | None = Query(None),
    filter_max_volume_val2: float | None = Query(None),
    filter_turnover_op: str = Query(""),
    filter_turnover_val: float | None = Query(None),
    filter_turnover_val2: float | None = Query(None),
    filter_exposure_usd_op: str = Query(""),
    filter_exposure_usd_val: float | None = Query(None),
    filter_exposure_usd_val2: float | None = Query(None),
    filter_exposure_pct_op: str = Query(""),
    filter_exposure_pct_val: float | None = Query(None),
    filter_exposure_pct_val2: float | None = Query(None),
    # CLAUD-137: WD Equity = balance + open_pnl (open_pnl not in MV; filter uses total_balance as approximation)
    filter_wd_equity_op: str = Query(""),
    filter_wd_equity_val: float | None = Query(None),
    filter_wd_equity_val2: float | None = Query(None),
    filter_score_op: str = Query(""),
    filter_score_val: float | None = Query(None),
    filter_score_val2: float | None = Query(None),
    # -----------------------------------------------------------------------
    # Per-column date filters: preset (today/this_week/this_month) OR from/to range
    # last_call  → m.last_trade_date (most recent trade / contact date in MV)
    # last_note  → m.last_deposit_time (most recent deposit, used as last note proxy in MV)
    # reg_date   → m.client_qualification_date
    # -----------------------------------------------------------------------
    filter_last_call_preset: str = Query(""),
    filter_last_call_from: str = Query(""),
    filter_last_call_to: str = Query(""),
    filter_last_note_preset: str = Query(""),
    filter_last_note_from: str = Query(""),
    filter_last_note_to: str = Query(""),
    filter_reg_date_preset: str = Query(""),
    filter_reg_date_from: str = Query(""),
    filter_reg_date_to: str = Query(""),
    filter_last_contact_preset: str = Query(""),
    filter_last_contact_from: str = Query(""),
    filter_last_contact_to: str = Query(""),
    favorites_only: str = Query(""),
    # Multi-select task type filter (comma-separated task names)
    filter_task_types: str = Query(""),
    # CLAUD-116: categorical multiselect filters
    filter_client_potential: str = Query(""),
    filter_client_segment: str = Query(""),
    # CLAUD-138: sales_client_potential multiselect filter
    filter_sales_client_potential: str = Query(""),
    # CLAUD-167: Legacy ID and Affiliate text filters
    filter_legacy_id: str = Query(""),
    filter_affiliate: str = Query(""),
    jwt_payload: JWTPayload = Depends(_require_retention),
    db: AsyncSession = Depends(get_db),
) -> dict:
    try:
        # Fetch configured extra columns
        _ec_result = await db.execute(
            text("SELECT source_column FROM retention_extra_columns ORDER BY id")
        )
        _extra_col_names = [r[0] for r in _ec_result.fetchall()]
        _sort_cols_ext = dict(_SORT_COLS)
        for _ecn in _extra_col_names:
            _sort_cols_ext[_ecn] = "m." + _ecn
        sort_col = _sort_cols_ext.get(sort_by, "m.accountid")
        direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

        where: list[str] = ["m.client_qualification_date IS NOT NULL"]
        params: dict = {"activity_days": activity_days, "current_user_id": jwt_payload.user_id}

        # RBAC data scoping (CLAUD-16) — inject WHERE fragment based on user role
        _scope_fragment, _scope_params = get_client_scope_filter(
            jwt_payload.role,
            jwt_payload.vtiger_user_id,
            jwt_payload.vtiger_department,
            team=jwt_payload.team,
            app_department=jwt_payload.app_department,
        )
        if _scope_fragment:
            # Strip leading "AND " since we append with " AND ".join
            _clean = _scope_fragment.strip()
            if _clean.upper().startswith("AND "):
                _clean = _clean[4:]
            where.append(_clean)
            params.update(_scope_params)

        _acct_filter = accountid or filter_accountid
        if _acct_filter:
            where.append("(m.accountid ILIKE :accountid_pattern OR m.full_name ILIKE :accountid_pattern)")
            params["accountid_pattern"] = f"%{_acct_filter}%"

        if qual_date_from:
            where.append("m.client_qualification_date >= :qual_date_from")
            params["qual_date_from"] = date.fromisoformat(qual_date_from)
        if qual_date_to:
            where.append("m.client_qualification_date <= :qual_date_to")
            params["qual_date_to"] = date.fromisoformat(qual_date_to)

        if days_op and days_val is not None:
            cond = _num_cond(days_op, "(CURRENT_DATE - m.client_qualification_date)", "days_val")
            if cond:
                where.append(cond)
                params["days_val"] = int(days_val)

        if trade_count_op and trade_count_val is not None:
            cond = _num_cond(trade_count_op, "m.trade_count", "trade_count_val")
            if cond:
                where.append(cond)
                params["trade_count_val"] = int(trade_count_val)

        if profit_op and profit_val is not None:
            cond = _num_cond(profit_op, "m.total_profit", "profit_val")
            if cond:
                where.append(cond)
                params["profit_val"] = profit_val

        if last_trade_from:
            where.append("m.last_trade_date::date >= :last_trade_from")
            params["last_trade_from"] = date.fromisoformat(last_trade_from)
        if last_trade_to:
            where.append("m.last_trade_date::date <= :last_trade_to")
            params["last_trade_to"] = date.fromisoformat(last_trade_to)

        if days_from_last_trade_op and days_from_last_trade_val is not None:
            cond = _num_cond(days_from_last_trade_op, "(CURRENT_DATE - m.last_trade_date::date)", "days_from_last_trade_val")
            if cond:
                where.append(f"m.last_trade_date IS NOT NULL AND {cond}")
                params["days_from_last_trade_val"] = int(days_from_last_trade_val)

        if deposit_count_op and deposit_count_val is not None:
            cond = _num_cond(deposit_count_op, "m.deposit_count", "deposit_count_val")
            if cond:
                where.append(cond)
                params["deposit_count_val"] = int(deposit_count_val)

        if total_deposit_op and total_deposit_val is not None:
            cond = _num_cond(total_deposit_op, "m.total_deposit", "total_deposit_val")
            if cond:
                where.append(cond)
                params["total_deposit_val"] = total_deposit_val

        if balance_op and balance_val is not None:
            cond = _num_cond(balance_op, "m.total_balance", "balance_val")
            if cond:
                where.append(cond)
                params["balance_val"] = balance_val

        if credit_op and credit_val is not None:
            cond = _num_cond(credit_op, "m.total_credit", "credit_val")
            if cond:
                where.append(cond)
                params["credit_val"] = credit_val

        if equity_op and equity_val is not None:
            cond = _num_cond(equity_op, "m.total_equity", "equity_val")
            if cond:
                where.append(cond)
                params["equity_val"] = equity_val

        if live_equity_op and live_equity_val is not None:
            cond = _num_cond(live_equity_op, "ABS(m.total_balance + m.total_credit)", "live_equity_val")
            if cond:
                where.append(cond)
                params["live_equity_val"] = live_equity_val

        if max_open_trade_op and max_open_trade_val is not None:
            cond = _num_cond(max_open_trade_op, "m.max_open_trade", "max_open_trade_val")
            if cond:
                where.append(cond)
                params["max_open_trade_val"] = max_open_trade_val

        if max_volume_op and max_volume_val is not None:
            cond = _num_cond(max_volume_op, "m.max_volume", "max_volume_val")
            if cond:
                where.append(cond)
                params["max_volume_val"] = max_volume_val

        if turnover_op and turnover_val is not None:
            cond = _num_cond(turnover_op, "CASE WHEN m.total_equity != 0 THEN m.max_volume / ABS(m.total_equity) ELSE NULL END", "turnover_val")
            if cond:
                where.append(cond)
                params["turnover_val"] = turnover_val

        if exposure_usd_op and exposure_usd_val is not None:
            cond = _num_cond(exposure_usd_op, "COALESCE(exp.exposure_usd, 0)", "exposure_usd_val")
            if cond:
                where.append(cond)
                params["exposure_usd_val"] = exposure_usd_val

        # -----------------------------------------------------------------------
        # Per-column text filters (ILIKE contains, case-insensitive)
        # -----------------------------------------------------------------------
        if filter_full_name:
            where.append("m.full_name ILIKE :filter_full_name_pattern")
            params["filter_full_name_pattern"] = f"%{filter_full_name}%"

        if filter_status:
            # Frontend sends approved display names (e.g. "Call Again,No Answer").
            # Match against:
            #  1. crs.status_label — labels set manually via the app
            #  2. aa.retention_status (numeric ID from MSSQL) — source of truth for all clients
            _status_list = [s.strip() for s in filter_status.split(",") if s.strip()]
            if _status_list:
                _id_list = [int(_STATUS_NAME_TO_ID[n]) for n in _status_list if n in _STATUS_NAME_TO_ID]
                params["filter_status_list"] = _status_list
                params["filter_status_id_list"] = _id_list if _id_list else [-1]
                where.append(
                    "(crs.status_label = ANY(:filter_status_list)"
                    " OR (crs.status_label IS NULL"
                    "     AND aa.retention_status = ANY(:filter_status_id_list)))"
                )

        if filter_agent:
            # agent_name is now pre-computed in retention_mv — simple ILIKE on the column.
            where.append("m.agent_name ILIKE :filter_agent_pattern")
            params["filter_agent_pattern"] = f"%{filter_agent}%"

        if filter_country:
            # CLAUD-143: comma-separated full country names → translate to ISO codes
            _cn_list = [s.strip() for s in filter_country.split(",") if s.strip()]
            _iso_list = [_COUNTRY_TO_ISO.get(c, c) for c in _cn_list]
            if len(_iso_list) == 1:
                where.append("aa.country_iso = :filter_country_iso")
                params["filter_country_iso"] = _iso_list[0]
            elif _iso_list:
                where.append("aa.country_iso = ANY(:filter_country_iso_list)")
                params["filter_country_iso_list"] = _iso_list

        # -----------------------------------------------------------------------
        # Per-column numeric filters (op + val + optional val2 for between)
        # -----------------------------------------------------------------------
        _numeric_filter_defs = [
            # (op_param_value, val_param_value, val2_param_value, sql_expr, param_prefix)
            (filter_balance_op,        filter_balance_val,        filter_balance_val2,        "m.total_balance",                                                                                                                       "filter_balance"),
            (filter_credit_op,         filter_credit_val,         filter_credit_val2,         "m.total_credit",                                                                                                                        "filter_credit"),
            (filter_equity_op,         filter_equity_val,         filter_equity_val2,         "m.total_equity",                                                                                                                        "filter_equity"),
            (filter_live_equity_op,    filter_live_equity_val,    filter_live_equity_val2,    "ABS(m.total_balance + m.total_credit)",                                                                                                 "filter_live_equity"),
            (filter_max_open_trade_op, filter_max_open_trade_val, filter_max_open_trade_val2, "m.max_open_trade",                                                                                                                      "filter_max_open_trade"),
            (filter_max_volume_op,     filter_max_volume_val,     filter_max_volume_val2,     "m.max_volume",                                                                                                                          "filter_max_volume"),
            (filter_turnover_op,       filter_turnover_val,       filter_turnover_val2,       "CASE WHEN m.total_equity != 0 THEN m.max_volume / ABS(m.total_equity) ELSE NULL END",                                                 "filter_turnover"),
            (filter_exposure_usd_op,   filter_exposure_usd_val,   filter_exposure_usd_val2,   "COALESCE(exp.exposure_usd, 0)",                                                                                                         "filter_exposure_usd"),
            (filter_exposure_pct_op,   filter_exposure_pct_val,   filter_exposure_pct_val2,   "exp.exposure_pct",                                                                                                                              "filter_exposure_pct"),
            # CLAUD-137: WD Equity ≈ total_balance (open_pnl excluded — not in MV)
            (filter_wd_equity_op,      filter_wd_equity_val,      filter_wd_equity_val2,      "m.total_balance",                                                                                                                           "filter_wd_equity"),
        ]
        for _op, _val, _val2, _expr, _prefix in _numeric_filter_defs:
            if not _op or _op not in _VALID_OPS or _val is None:
                continue
            _p1 = f"{_prefix}_val"
            _p2 = f"{_prefix}_val2"
            # For "between", both values must be present; skip if val2 is missing.
            if _op == "between" and _val2 is None:
                continue
            _cond = _num_cond(_op, _expr, _p1, _p2 if _op == "between" else None)
            if _cond:
                # Exclude NULLs so the filter doesn't silently skip rows with a NULL column.
                where.append(f"{_expr} IS NOT NULL AND {_cond}")
                params[_p1] = _val
                if _op == "between":
                    params[_p2] = _val2

        # score filter: score is now stored in client_scores (joined as cs) — apply server-side.
        if filter_score_op and filter_score_op in _VALID_OPS and filter_score_val is not None:
            if filter_score_op == "between" and filter_score_val2 is None:
                pass  # skip incomplete between filter
            else:
                _score_cond = _num_cond(
                    filter_score_op,
                    "COALESCE(cs.score, 0)",
                    "filter_score_val",
                    "filter_score_val2" if filter_score_op == "between" else None,
                )
                if _score_cond:
                    where.append(_score_cond)
                    params["filter_score_val"] = filter_score_val
                    if filter_score_op == "between":
                        params["filter_score_val2"] = filter_score_val2

        # -----------------------------------------------------------------------
        # Per-column date filters
        # last_call preset/range  → m.last_trade_date (::date cast for comparisons)
        # last_note preset/range  → m.last_deposit_time (::date cast)
        # reg_date  preset/range  → m.client_qualification_date
        # -----------------------------------------------------------------------
        _date_filter_defs = [
            # (preset_val, from_val, to_val, sql_date_expr, param_prefix, null_guard_col)
            (filter_last_call_preset,  filter_last_call_from,  filter_last_call_to,  "m.last_trade_date::date",   "filter_last_call",  "m.last_trade_date"),
            (filter_last_note_preset,  filter_last_note_from,  filter_last_note_to,  "m.last_deposit_time::date", "filter_last_note",  "m.last_deposit_time"),
            (filter_reg_date_preset,   filter_reg_date_from,   filter_reg_date_to,   "m.client_qualification_date", "filter_reg_date", None),
            (filter_last_contact_preset, filter_last_contact_from, filter_last_contact_to, "lc.last_communication_date::date", "filter_last_contact", "lc.last_communication_date"),
        ]
        for _preset, _from, _to, _date_expr, _dp, _null_col in _date_filter_defs:
            _date_conds: list[str] = []
            if _null_col:
                _null_guard = f"{_null_col} IS NOT NULL"
            else:
                _null_guard = None

            if _preset:
                _pc = _date_preset_cond(_date_expr, _preset)
                if _pc:
                    _date_conds.append(_pc)
            else:
                if _from:
                    _date_conds.append(f"{_date_expr} >= :{_dp}_from")
                    params[f"{_dp}_from"] = date.fromisoformat(_from)
                if _to:
                    _date_conds.append(f"{_date_expr} <= :{_dp}_to")
                    params[f"{_dp}_to"] = date.fromisoformat(_to)

            if _date_conds:
                combined = " AND ".join(_date_conds)
                if _null_guard:
                    where.append(f"{_null_guard} AND {combined}")
                else:
                    where.append(combined)

        if assigned_to:
            where.append("m.assigned_to = :assigned_to")
            params["assigned_to"] = assigned_to

        if active == "true":
            where.append(f"({_MV_ACTIVE})")
        elif active == "false":
            where.append(f"NOT ({_MV_ACTIVE})")

        if active_ftd == "true":
            where.append(f"({_MV_ACTIVE_FTD})")
        elif active_ftd == "false":
            where.append(f"NOT ({_MV_ACTIVE_FTD})")

        # Favorites filter (CLAUD-50)
        if favorites_only == "true":
            where.append("uf.accountid IS NOT NULL")
        elif favorites_only == "false":
            where.append("uf.accountid IS NULL")

        # Card type filter
        if filter_card_type:
            where.append("cct.card_type ILIKE :filter_card_type")
            params["filter_card_type"] = f"%{filter_card_type}%"

        # Multi-select task type filter (CLAUD-62)
        # Uses client_task_assignments + retention_tasks to filter by task name(s).
        # OR logic within selected types; excludes clients with no matching task.
        if filter_task_types:
            _task_type_names = [t.strip() for t in filter_task_types.split(",") if t.strip()]
            if _task_type_names:
                where.append(
                    "EXISTS ("
                    "  SELECT 1 FROM client_task_assignments cta"
                    "  JOIN retention_tasks rt ON rt.id = cta.task_id"
                    "  WHERE cta.accountid = m.accountid"
                    "  AND rt.name = ANY(:filter_task_type_names)"
                    ")"
                )
                params["filter_task_type_names"] = _task_type_names

        # CLAUD-116: Client potential / segment multiselect filters
        if filter_client_potential:
            _pot_list = [s.strip() for s in filter_client_potential.split(",") if s.strip()]
            if len(_pot_list) == 1:
                where.append("csp.client_potential = :filter_client_potential_val")
                params["filter_client_potential_val"] = _pot_list[0]
            elif _pot_list:
                where.append("csp.client_potential = ANY(:filter_client_potential_list)")
                params["filter_client_potential_list"] = _pot_list

        if filter_client_segment:
            _seg_list = [s.strip() for s in filter_client_segment.split(",") if s.strip()]
            if len(_seg_list) == 1:
                where.append("csp.client_segment = :filter_client_segment_val")
                params["filter_client_segment_val"] = _seg_list[0]
            elif _seg_list:
                where.append("csp.client_segment = ANY(:filter_client_segment_list)")
                params["filter_client_segment_list"] = _seg_list

        # CLAUD-138: sales_client_potential multiselect filter
        if filter_sales_client_potential:
            _scp_list = [s.strip() for s in filter_sales_client_potential.split(",") if s.strip()]
            if len(_scp_list) == 1:
                where.append("m.sales_client_potential = :filter_scp_val")
                params["filter_scp_val"] = _scp_list[0]
            elif _scp_list:
                where.append("m.sales_client_potential = ANY(:filter_scp_list)")
                params["filter_scp_list"] = _scp_list

        # CLAUD-167: Legacy ID and Affiliate text filters
        if filter_legacy_id:
            where.append("aa.customer_id ILIKE :filter_legacy_id_pattern")
            params["filter_legacy_id_pattern"] = f"%{filter_legacy_id}%"

        if filter_affiliate:
            where.append("aa.original_affiliate ILIKE :filter_affiliate_pattern")
            params["filter_affiliate_pattern"] = f"%{filter_affiliate}%"

        # Task filter — inject task conditions into the main WHERE clause
        if task_id is not None:
            import json as _json
            from sqlalchemy import select as _select
            from app.models.retention_task import RetentionTask
            from app.routers.retention_tasks import _build_task_where
            _task = await db.get(RetentionTask, task_id)
            if _task is None:
                raise HTTPException(status_code=404, detail="Task not found")
            _t_where, _t_params = _build_task_where(_json.loads(_task.conditions))
            where.extend(_t_where[1:])  # skip the first clause (client_qualification_date IS NOT NULL) — already in main where
            params.update(_t_params)

        where_clause = " AND ".join(where)

        _ck = _cached_count_key(where_clause, params)
        _now = time.time()
        if _ck in _count_cache and _count_cache[_ck][1] > _now:
            total = _count_cache[_ck][0]
        else:
            count_result = await db.execute(
                text(f"SELECT COUNT(*) FROM retention_mv m LEFT JOIN client_scores cs ON cs.accountid = m.accountid{_EXPOSURE_JOIN}{_FAVORITES_JOIN}{_CARD_TYPE_JOIN}{_LAST_COMM_JOIN}{_RETENTION_STATUS_JOIN}{_SALES_PROFILE_JOIN}{_ANT_ACC_JOIN} WHERE {where_clause}"),
                params,
            )
            total = count_result.scalar() or 0
            _count_cache[_ck] = (total, _now + _COUNT_TTL)
            # Evict stale entries to prevent unbounded growth
            if len(_count_cache) > 500:
                _expired = [k for k, v in _count_cache.items() if v[1] <= _now]
                for k in _expired:
                    del _count_cache[k]

        _extra_sel = ""
        if _extra_col_names:
            _extra_sel = ",\n                    " + ",\n                    ".join("m." + c for c in _extra_col_names)
        rows_result = await db.execute(
            text(f"""
                SELECT
                    m.accountid,
                    m.full_name,
                    m.client_qualification_date,
                    (CURRENT_DATE - m.client_qualification_date) AS days_in_retention,
                    m.trade_count,
                    m.total_profit,
                    m.last_trade_date,
                    CASE WHEN m.last_trade_date IS NOT NULL
                         THEN (CURRENT_DATE - m.last_trade_date::date) END AS days_from_last_trade,
                    {_MV_ACTIVE} AS active,
                    {_MV_ACTIVE_FTD} AS active_ftd,
                    m.deposit_count,
                    m.total_deposit,
                    m.total_balance AS balance,
                    m.total_credit AS credit,
                    m.total_equity AS equity,
                    m.max_open_trade,
                    m.max_volume,
                    m.win_rate,
                    m.avg_trade_size{_extra_sel},
                    m.assigned_to,
                    m.agent_name,
                    crs.status_label AS retention_status,
                    aa.retention_status AS mssql_retention_status,
                    m.sales_client_potential,
                    CASE WHEN m.birth_date IS NOT NULL
                         THEN EXTRACT(year FROM AGE(m.birth_date))::int END AS age,
                    COALESCE(cs.score, 0) AS score,
                    COALESCE(exp.exposure_usd, 0) AS exposure_usd,
                    exp.exposure_pct,
                    (uf.accountid IS NOT NULL) AS is_favorite,
                    cct.card_type,
                    lc.last_communication_date,
                    csp.client_potential,
                    csp.client_segment,
                    m.last_deposit_time AS last_deposit_date,
                    lw.last_withdrawal_date,
                    COALESCE(aa.net_deposit, aa.total_deposit - aa.total_withdrawal) AS net_deposit_ever,
                    aa.country_iso AS country,
                    aa.customer_id AS legacy_id,
                    aa.original_affiliate AS affiliate,
                    mt.mt_account,
                    COALESCE(mt.leverage, 200) AS leverage
                FROM retention_mv m
                LEFT JOIN client_scores cs ON cs.accountid = m.accountid{_EXPOSURE_JOIN}{_FAVORITES_JOIN}{_CARD_TYPE_JOIN}{_LAST_COMM_JOIN}{_RETENTION_STATUS_JOIN}{_SALES_PROFILE_JOIN}{_ANT_ACC_JOIN}{_LAST_WITHDRAWAL_JOIN}{_MT_ACCOUNT_JOIN}
                WHERE {where_clause}
                ORDER BY {sort_col} {direction} NULLS LAST
                LIMIT :limit OFFSET :offset
            """),
            {**params, "limit": page_size, "offset": (page - 1) * page_size},
        )
        rows = rows_result.mappings().all()

        # Fetch Open PNL from local open_pnl_cache (synced from dealio.positions every 3 minutes)
        open_pnl_map: dict = {}
        try:
            if rows:
                account_ids = [str(r["accountid"]) for r in rows]
                login_result = await db.execute(
                    text("SELECT login, vtigeraccountid FROM vtiger_trading_accounts WHERE vtigeraccountid = ANY(:ids)"),
                    {"ids": account_ids},
                )
                login_rows = login_result.fetchall()
                logins = [str(lr[0]) for lr in login_rows]
                login_to_account = {str(lr[0]): str(lr[1]) for lr in login_rows}
                if logins:
                    pnl_result = await db.execute(
                        text("SELECT login, pnl FROM open_pnl_cache WHERE login = ANY(:logins)"),
                        {"logins": logins},
                    )
                    for pnl_row in pnl_result.fetchall():
                        acct = login_to_account.get(str(pnl_row[0]))
                        if acct:
                            open_pnl_map[acct] = open_pnl_map.get(acct, 0.0) + float(pnl_row[1] or 0)
        except Exception as pnl_err:
            logger.warning("Could not fetch open PNL from local cache: %s", pnl_err)

        # Evaluate retention tasks for this page using a single UNION ALL query.
        # A CTE restricts the MV to the 50 page accounts first (index scan),
        # so each task sub-query operates on 50 rows, not 24 000+.
        from sqlalchemy import select as _select
        from app.models.retention_task import RetentionTask
        from app.routers.retention_tasks import _build_task_where
        import json as _json
        tasks_map: dict = {str(r["accountid"]): [] for r in rows}
        try:
            page_aids = [str(r["accountid"]) for r in rows]
            if page_aids:
                all_tasks_result = await db.execute(
                    _select(RetentionTask).order_by(RetentionTask.id)
                )
                all_tasks = all_tasks_result.scalars().all()
                if all_tasks:
                    union_parts: list[str] = []
                    combined_params: dict = {"_page_aids": page_aids}
                    for tidx, task in enumerate(all_tasks):
                        try:
                            conditions = _json.loads(task.conditions)
                        except Exception:
                            continue
                        t_where, t_params = _build_task_where(conditions)
                        # Prefix each task's params to avoid name collisions across tasks
                        prefixed = {f"t{tidx}_{k}": v for k, v in t_params.items()}
                        combined_params.update(prefixed)
                        # Replace :cond_N → :tTidx_cond_N in WHERE clauses
                        prefixed_where = [
                            w.replace(":cond_", f":t{tidx}_cond_")
                            for w in t_where
                        ]
                        # _build_task_where uses "m." prefix — CTE must use alias "m" to match
                        t_clause = " AND ".join(prefixed_where)
                        union_parts.append(
                            f"SELECT m.accountid, {tidx}::int AS tidx "
                            f"FROM page_accts m LEFT JOIN account_exposure_cache aec ON aec.accountid = m.accountid WHERE {t_clause}"
                        )
                    if union_parts:
                        union_sql = " UNION ALL ".join(union_parts)
                        t_result = await db.execute(
                            text(
                                "WITH page_accts AS ("
                                "  SELECT * FROM retention_mv WHERE accountid = ANY(:_page_aids)"
                                ") " + union_sql
                            ),
                            combined_params,
                        )
                        for tr in t_result.fetchall():
                            aid = str(tr[0])
                            task = all_tasks[tr[1]]
                            if aid in tasks_map:
                                tasks_map[aid].append({"name": task.name, "color": task.color or "grey"})
        except Exception as tasks_err:
            logger.warning("Could not evaluate retention tasks for page: %s", tasks_err)

        # CLAUD-171: Build client rows with margin calculations
        clients_out = []
        for r in rows:
            _aid = str(r["accountid"])
            _open_pnl = open_pnl_map.get(_aid, 0.0)
            _exposure = float(r["exposure_usd"])
            _leverage = max(float(r["leverage"] or 200), 1)
            _live_eq = float(r["balance"]) + float(r["credit"]) + _open_pnl
            _used_margin = round(_exposure / _leverage, 2) if _exposure > 0 else 0.0
            _free_margin = round(_live_eq - _used_margin, 2)
            _margin_level_pct = round(_live_eq / _used_margin * 100, 1) if _used_margin > 0 else None
            clients_out.append({
                "accountid": _aid,
                "full_name": r["full_name"] or "",
                "client_qualification_date": r["client_qualification_date"].isoformat() if r["client_qualification_date"] else None,
                "days_in_retention": int(r["days_in_retention"]) if r["days_in_retention"] is not None else None,
                "trade_count": int(r["trade_count"]),
                "total_profit": float(r["total_profit"]),
                "last_trade_date": r["last_trade_date"].isoformat() if r["last_trade_date"] else None,
                "days_from_last_trade": int(r["days_from_last_trade"]) if r["days_from_last_trade"] is not None else None,
                "active": bool(r["active"]),
                "active_ftd": bool(r["active_ftd"]),
                "deposit_count": int(r["deposit_count"]),
                "total_deposit": float(r["total_deposit"]),
                "balance": float(r["balance"]),
                "credit": float(r["credit"]),
                "equity": float(r["equity"]),
                "open_pnl": _open_pnl,
                "max_open_trade": round(float(r["max_open_trade"]), 1) if r["max_open_trade"] is not None else None,
                "max_volume": round(float(r["max_volume"]), 1) if r["max_volume"] is not None else None,
                "win_rate": round(float(r["win_rate"]), 1) if r["win_rate"] is not None else None,
                "avg_trade_size": round(float(r["avg_trade_size"]), 2) if r["avg_trade_size"] is not None else None,
                "live_equity": round(abs(_live_eq), 2),
                "turnover": round(float(r["max_volume"]) / abs(float(r["equity"])), 1) if r["max_volume"] is not None and float(r["equity"]) != 0 else None,
                "exposure_pct": round(float(r["exposure_pct"]), 2) if r["exposure_pct"] is not None else None,
                "exposure_usd": round(_exposure, 2),
                "used_margin": _used_margin,
                "free_margin": _free_margin,
                "margin_level_pct": _margin_level_pct,
                "is_favorite": bool(r["is_favorite"]),
                "card_type": r["card_type"],
                "assigned_to": r["assigned_to"],
                "agent_name": r["agent_name"] or None,
                "tasks": tasks_map.get(_aid, []),
                "score": int(r["score"]),
                "retention_status": (
                    r["retention_status"] if r["retention_status"] in _APPROVED_STATUS_NAMES
                    else _resolve_display_status(
                        str(r["mssql_retention_status"]) if r["mssql_retention_status"] is not None else None
                    )
                ),
                "closed_pnl": round(float(r["total_profit"]), 2) if r["total_profit"] is not None else None,
                "age": int(r["age"]) if r["age"] is not None else None,
                "last_communication_date": r["last_communication_date"].isoformat() if r["last_communication_date"] else None,
                "client_potential": r["client_potential"],
                "client_segment": r["client_segment"],
                "sales_client_potential": r["sales_client_potential"],
                "last_deposit_date": r["last_deposit_date"].isoformat() if r["last_deposit_date"] else None,
                "last_withdrawal_date": r["last_withdrawal_date"].isoformat() if r["last_withdrawal_date"] else None,
                "net_deposit_ever": round(float(r["net_deposit_ever"]), 2) if r["net_deposit_ever"] is not None else None,
                "country": r["country"] or None,
                "legacy_id": r["legacy_id"] or None,
                "affiliate": r["affiliate"] or None,
                "mt_account": r["mt_account"] or None,
                **{col: r[col] for col in _extra_col_names},
            })

        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "clients": clients_out,
        }
    except Exception as e:
        if "has not been populated" in str(e):
            raise HTTPException(status_code=503, detail="Data is being prepared, please try again in a moment.")
        raise HTTPException(status_code=502, detail=f"Query failed: {e}")


@router.get("/retention/sales-potential-options")
async def get_sales_potential_options(
    jwt_payload: JWTPayload = Depends(_require_retention),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    """CLAUD-138: Return distinct non-null sales_client_potential values for multiselect filter."""
    result = await db.execute(
        text(
            "SELECT DISTINCT sales_client_potential FROM retention_mv"
            " WHERE sales_client_potential IS NOT NULL AND sales_client_potential <> ''"
            " ORDER BY sales_client_potential"
        )
    )
    return [row[0] for row in result.fetchall()]


@router.get("/retention/export")
async def export_retention_grid(
    jwt_payload: JWTPayload = Depends(_require_retention),
    db: AsyncSession = Depends(get_db),
    # Core
    sort_by: str = Query("accountid"),
    sort_dir: str = Query("asc"),
    activity_days: int = Query(35, ge=1, le=365),
    active: str = Query(""),
    active_ftd: str = Query(""),
    assigned_to: str = Query(""),
    accountid: str = Query(""),
    # Text/multiselect column filters (same as /retention/clients)
    filter_full_name: str = Query(""),
    filter_status: str = Query(""),
    filter_agent: str = Query(""),
    filter_card_type: str = Query(""),
    filter_country: str = Query(""),
    filter_client_potential: str = Query(""),
    filter_client_segment: str = Query(""),
    filter_sales_client_potential: str = Query(""),
    # Numeric filters
    filter_balance_op: str = Query(""),
    filter_balance_val: float | None = Query(None),
    filter_balance_val2: float | None = Query(None),
    filter_credit_op: str = Query(""),
    filter_credit_val: float | None = Query(None),
    filter_credit_val2: float | None = Query(None),
    filter_equity_op: str = Query(""),
    filter_equity_val: float | None = Query(None),
    filter_equity_val2: float | None = Query(None),
    filter_score_op: str = Query(""),
    filter_score_val: float | None = Query(None),
    filter_score_val2: float | None = Query(None),
) -> StreamingResponse:
    """CLAUD-144: Export Retention Grid to Excel (.xlsx). Requires retention_grid_export permission."""
    # Permission check
    perms_result = await db.execute(
        text("SELECT permissions FROM roles WHERE name = :role"),
        {"role": jwt_payload.role},
    )
    prow = perms_result.fetchone()
    perms = prow[0] if prow else []
    if "retention_grid_export" not in (perms or []):
        raise HTTPException(status_code=403, detail="Export permission not granted for this role.")

    # Get column visibility for caller's role
    col_vis_result = await db.execute(
        text("SELECT column_key, is_visible FROM role_column_visibility WHERE role_name = :role"),
        {"role": jwt_payload.role},
    )
    col_vis: dict[str, bool] = {r[0]: r[1] for r in col_vis_result.fetchall()}
    if not col_vis:
        from app.routers.column_visibility import DEFAULT_VISIBILITY
        for col_key, roles in DEFAULT_VISIBILITY.items():
            col_vis[col_key] = roles.get(jwt_payload.role, True)

    def _vis(key: str) -> bool:
        return col_vis.get(key, True)

    # WHERE clause
    _scope_fragment, _scope_params = get_client_scope_filter(
        jwt_payload.role,
        jwt_payload.vtiger_user_id,
        jwt_payload.vtiger_department,
        team=jwt_payload.team,
        app_department=jwt_payload.app_department,
    )
    where: list[str] = ["m.client_qualification_date IS NOT NULL"]
    params: dict = {"activity_days": activity_days, "current_user_id": jwt_payload.user_id}

    if _scope_fragment:
        _clean = _scope_fragment.strip()
        if _clean.upper().startswith("AND "):
            _clean = _clean[4:]
        where.append(_clean)
        params.update(_scope_params)

    if active == "true":
        where.append(_MV_ACTIVE)
    elif active == "false":
        where.append(f"NOT ({_MV_ACTIVE})")
    if active_ftd == "true":
        where.append(_MV_ACTIVE_FTD)
    if assigned_to:
        where.append("m.assigned_to = :export_assigned_to")
        params["export_assigned_to"] = assigned_to
    if accountid:
        where.append("m.accountid::text ILIKE :export_accountid_pat")
        params["export_accountid_pat"] = f"%{accountid}%"
    if filter_full_name:
        where.append("m.full_name ILIKE :exp_fn_pat")
        params["exp_fn_pat"] = f"%{filter_full_name}%"
    if filter_status:
        _sl = [s.strip() for s in filter_status.split(",") if s.strip()]
        if len(_sl) == 1:
            where.append("crs.status_label = :exp_status_label")
            params["exp_status_label"] = _sl[0]
        elif _sl:
            where.append("crs.status_label = ANY(:exp_status_list)")
            params["exp_status_list"] = _sl
    if filter_agent:
        where.append("m.agent_name ILIKE :exp_agent_pat")
        params["exp_agent_pat"] = f"%{filter_agent}%"
    if filter_card_type:
        where.append("cct.card_type ILIKE :exp_card_pat")
        params["exp_card_pat"] = f"%{filter_card_type}%"
    if filter_country:
        _cl = [s.strip() for s in filter_country.split(",") if s.strip()]
        _il = [_COUNTRY_TO_ISO.get(c, c) for c in _cl]
        if len(_il) == 1:
            where.append("aa.country_iso = :exp_country_iso")
            params["exp_country_iso"] = _il[0]
        elif _il:
            where.append("aa.country_iso = ANY(:exp_country_iso_list)")
            params["exp_country_iso_list"] = _il
    if filter_client_potential:
        _pl = [s.strip() for s in filter_client_potential.split(",") if s.strip()]
        if len(_pl) == 1:
            where.append("csp.client_potential = :exp_pot")
            params["exp_pot"] = _pl[0]
        elif _pl:
            where.append("csp.client_potential = ANY(:exp_pot_list)")
            params["exp_pot_list"] = _pl
    if filter_client_segment:
        _segl = [s.strip() for s in filter_client_segment.split(",") if s.strip()]
        if len(_segl) == 1:
            where.append("csp.client_segment = :exp_seg")
            params["exp_seg"] = _segl[0]
        elif _segl:
            where.append("csp.client_segment = ANY(:exp_seg_list)")
            params["exp_seg_list"] = _segl
    if filter_sales_client_potential:
        _spl = [s.strip() for s in filter_sales_client_potential.split(",") if s.strip()]
        if len(_spl) == 1:
            where.append("m.sales_client_potential = :exp_scp")
            params["exp_scp"] = _spl[0]
        elif _spl:
            where.append("m.sales_client_potential = ANY(:exp_scp_list)")
            params["exp_scp_list"] = _spl
    for _op, _val, _val2, _expr, _prefix in [
        (filter_balance_op, filter_balance_val, filter_balance_val2, "m.total_balance", "exp_bal"),
        (filter_credit_op,  filter_credit_val,  filter_credit_val2,  "m.total_credit",  "exp_crd"),
        (filter_equity_op,  filter_equity_val,  filter_equity_val2,  "m.total_equity",  "exp_eq"),
        (filter_score_op,   filter_score_val,   filter_score_val2,   "COALESCE(cs.score, 0)", "exp_sc"),
    ]:
        if not _op or _op not in _VALID_OPS or _val is None:
            continue
        _p1, _p2 = f"{_prefix}_v1", f"{_prefix}_v2"
        if _op == "between" and _val2 is None:
            continue
        _cond = _num_cond(_op, _expr, _p1, _p2 if _op == "between" else None)
        if _cond:
            where.append(f"{_expr} IS NOT NULL AND {_cond}")
            params[_p1] = _val
            if _op == "between":
                params[_p2] = _val2

    where_clause = " AND ".join(where)
    valid_sort = _SORT_COLS.get(sort_by, "m.accountid")
    direction = "DESC" if sort_dir.lower() == "desc" else "ASC"

    query = text(f"""
        SELECT
            m.accountid,
            m.full_name,
            m.agent_name,
            m.client_qualification_date,
            (CURRENT_DATE - m.client_qualification_date) AS days_in_retention,
            m.trade_count,
            m.total_profit AS closed_pnl,
            m.last_trade_date,
            CASE WHEN m.last_trade_date IS NOT NULL
                 THEN (CURRENT_DATE - m.last_trade_date::date) END AS days_from_last_trade,
            m.deposit_count,
            m.total_deposit,
            m.total_balance AS balance,
            m.total_credit AS credit,
            m.total_equity AS equity,
            m.max_open_trade,
            m.max_volume,
            m.win_rate,
            m.avg_trade_size,
            m.last_deposit_time AS last_deposit_date,
            CASE WHEN m.birth_date IS NOT NULL
                 THEN EXTRACT(year FROM AGE(m.birth_date))::int END AS age,
            COALESCE(cs.score, 0) AS score,
            COALESCE(exp.exposure_usd, 0) AS exposure_usd,
            exp.exposure_pct,
            cct.card_type,
            lc.last_communication_date,
            csp.client_potential,
            csp.client_segment,
            m.sales_client_potential,
            lw.last_withdrawal_date,
            COALESCE(aa.net_deposit, aa.total_deposit - aa.total_withdrawal) AS net_deposit_ever,
            aa.country_iso AS country,
            crs.status_label AS retention_status,
            aa.retention_status AS mssql_retention_status
        FROM retention_mv m
        LEFT JOIN client_scores cs ON cs.accountid = m.accountid
        {_EXPOSURE_JOIN}
        {_CARD_TYPE_JOIN}
        {_LAST_COMM_JOIN}
        {_RETENTION_STATUS_JOIN}
        {_SALES_PROFILE_JOIN}
        {_ANT_ACC_JOIN}
        {_LAST_WITHDRAWAL_JOIN}
        WHERE {where_clause}
        ORDER BY {valid_sort} {direction} NULLS LAST
        LIMIT 50000
    """)

    result = await db.execute(query, params)
    rows = result.mappings().all()

    # Define columns: (vis_key_or_None, header, getter)
    # vis_key=None means always include the column
    _COLS: list[tuple[str | None, str, Any]] = [
        (None,                   "Account ID",         lambda r: str(r["accountid"])),
        ("client_name",          "Full Name",          lambda r: r["full_name"] or ""),
        ("retention_status",     "Retention Status",   lambda r: (
            r["retention_status"] if r["retention_status"] in _APPROVED_STATUS_NAMES
            else _resolve_display_status(
                str(r["mssql_retention_status"]) if r["mssql_retention_status"] is not None else None
            )
        )),
        ("assigned_to",          "Agent",              lambda r: r["agent_name"] or ""),
        ("score",                "Score",              lambda r: int(r["score"])),
        ("registration_date",    "Reg Date",           lambda r: r["client_qualification_date"].isoformat() if r["client_qualification_date"] else ""),
        ("days_in_retention",    "Days in Retention",  lambda r: int(r["days_in_retention"]) if r["days_in_retention"] is not None else ""),
        ("last_trade_date",      "Last Trade Date",    lambda r: r["last_trade_date"].isoformat() if r["last_trade_date"] else ""),
        ("days_from_last_trade", "Days Last Trade",    lambda r: int(r["days_from_last_trade"]) if r["days_from_last_trade"] is not None else ""),
        ("trade_count",          "Trades",             lambda r: int(r["trade_count"])),
        ("balance",              "Balance",            lambda r: float(r["balance"])),
        ("credit",               "Credit",             lambda r: float(r["credit"])),
        ("equity",               "Equity",             lambda r: float(r["equity"])),
        ("live_equity",          "Live Equity",        lambda r: round(abs(float(r["balance"]) + float(r["credit"])), 2)),
        ("closed_pnl",           "Closed PnL",         lambda r: round(float(r["closed_pnl"]), 2) if r["closed_pnl"] is not None else ""),
        ("net_deposit_ever",     "Net Deposit Ever",   lambda r: round(float(r["net_deposit_ever"]), 2) if r["net_deposit_ever"] is not None else ""),
        ("total_deposit",        "Total Deposit",      lambda r: float(r["total_deposit"])),
        ("deposit_count",        "Deposit Count",      lambda r: int(r["deposit_count"])),
        ("max_volume",           "Max Volume",         lambda r: round(float(r["max_volume"]), 1) if r["max_volume"] is not None else ""),
        ("max_open_trade",       "Max Open Trade",     lambda r: round(float(r["max_open_trade"]), 1) if r["max_open_trade"] is not None else ""),
        ("avg_trade_size",       "Avg Trade Size",     lambda r: round(float(r["avg_trade_size"]), 2) if r["avg_trade_size"] is not None else ""),
        ("turnover",             "Turnover",           lambda r: round(float(r["max_volume"]) / abs(float(r["equity"])), 1) if r["max_volume"] is not None and float(r["equity"]) != 0 else ""),
        ("exposure_usd",         "Exposure USD",       lambda r: round(float(r["exposure_usd"]), 2)),
        ("exposure_pct",         "Exposure %",         lambda r: round(float(r["exposure_pct"]), 2) if r["exposure_pct"] is not None else ""),
        ("last_contact",         "Last Contact",       lambda r: r["last_communication_date"].isoformat() if r["last_communication_date"] else ""),
        ("last_deposit_date",    "Last Deposit",       lambda r: r["last_deposit_date"].isoformat() if r["last_deposit_date"] else ""),
        ("last_withdrawal_date", "Last Withdrawal",    lambda r: r["last_withdrawal_date"].isoformat() if r["last_withdrawal_date"] else ""),
        ("card_type",            "Card Type",          lambda r: r["card_type"] or ""),
        ("age",                  "Age",                lambda r: int(r["age"]) if r["age"] is not None else ""),
        ("client_potential",     "Client Potential",   lambda r: r["client_potential"] or ""),
        ("client_segment",       "Client Segment",     lambda r: r["client_segment"] or ""),
        ("sales_client_potential", "Sales Potential",  lambda r: r["sales_client_potential"] or ""),
        (None,                   "Country",            lambda r: _ISO_TO_COUNTRY.get(r["country"] or "", r["country"] or "")),
    ]

    # Filter to visible columns
    visible_cols = [(hdr, getter) for (vis_key, hdr, getter) in _COLS if vis_key is None or _vis(vis_key)]

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Retention Grid"

    # Header row (bold)
    ws.append([hdr for hdr, _ in visible_cols])
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Data rows
    for r in rows:
        ws.append([getter(r) for _, getter in visible_cols])

    bio = io.BytesIO()
    wb.save(bio)
    bio.seek(0)
    today = date.today().isoformat()
    return StreamingResponse(
        bio,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=retention-grid-export-{today}.xlsx"},
    )


@router.get("/retention/agents")
async def get_retention_agents(
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list:
    try:
        result = await db.execute(
            text("SELECT id, first_name, last_name FROM vtiger_users ORDER BY first_name, last_name")
        )
        rows = result.fetchall()
        return [
            {"id": str(r[0]), "name": f"{r[1] or ''} {r[2] or ''}".strip()}
            for r in rows
            if r[0]
        ]
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch agents: {e}")


@router.get("/retention/countries")
async def get_retention_countries(
    _: Any = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[str]:
    """CLAUD-143: Return distinct full country names for clients currently in the retention grid."""
    try:
        result = await db.execute(
            text(
                "SELECT DISTINCT aa.country_iso"
                " FROM retention_mv m"
                " JOIN ant_acc aa ON aa.accountid = m.accountid"
                " WHERE aa.country_iso IS NOT NULL AND aa.country_iso <> ''"
                " ORDER BY aa.country_iso"
            )
        )
        names = sorted(
            _ISO_TO_COUNTRY.get(row[0], row[0]) for row in result.fetchall()
        )
        return names
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Failed to fetch countries: {e}")

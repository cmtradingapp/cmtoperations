import logging
from typing import Any, Dict, List

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.user import User
from app.pg_database import get_db

logger = logging.getLogger(__name__)

router = APIRouter()


async def _safe_query(
    db: AsyncSession,
    sql: str,
    label: str,
    *,
    fetch_all: bool = False,
):
    """Execute a query with rollback-on-error so one bad query
    does not poison the transaction for subsequent queries."""
    try:
        result = await db.execute(text(sql))
        return result.fetchall() if fetch_all else result.fetchone()
    except Exception as exc:
        logger.warning("Dashboard query [%s] failed: %s", label, exc)
        try:
            await db.rollback()
        except Exception:
            pass
        return [] if fetch_all else None


@router.get("/dashboard/trading")
async def get_trading_dashboard(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> Dict[str, Any]:
    """Return company-level trading metrics for the management dashboard.

    Grouped into logical sections:
      - today: deposits, FTDs, withdrawals, net, new accounts
      - portfolio: balances, funded clients, active traders
      - trends: deposits last 7 days, new registrations last 7 days
      - breakdowns: country, regulation, account status
      - retention_funnel: qualified -> funded -> with balance
      - top_lists: top depositors today, top affiliates
    """

    # ==================================================================
    # SECTION 1: TODAY'S ACTIVITY
    # ==================================================================

    # -- Today's deposits (Approved only, excludes bonuses) ------------
    row = await _safe_query(db, """
        SELECT COUNT(*), COALESCE(SUM(usdamount), 0)
        FROM vtiger_mttransactions
        WHERE transactiontype = 'Deposit'
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE
          AND (payment_method IS NULL OR payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
    """, "today_deposits")
    today_deposits_count = int(row[0]) if row else 0
    today_deposits_amount = round(float(row[1]), 2) if row else 0.0

    # -- Today's FTDs (first-time deposits from vtiger_mttransactions, excludes bonuses) --
    row = await _safe_query(db, """
        WITH first_dep AS (
            SELECT login, MIN(DATE(confirmation_time)) AS first_date
            FROM vtiger_mttransactions
            WHERE transactiontype = 'Deposit'
              AND transactionapproval = 'Approved'
              AND (payment_method IS NULL OR payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
            GROUP BY login
        )
        SELECT COUNT(DISTINCT t.login), COALESCE(SUM(t.usdamount), 0)
        FROM vtiger_mttransactions t
        JOIN first_dep fd ON t.login = fd.login
        WHERE fd.first_date = CURRENT_DATE
          AND t.transactiontype = 'Deposit'
          AND t.transactionapproval = 'Approved'
          AND t.confirmation_time >= CURRENT_DATE
          AND (t.payment_method IS NULL OR t.payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
    """, "today_ftds")
    today_ftds_count = int(row[0]) if row else 0
    today_ftds_amount = round(float(row[1]), 2) if row else 0.0

    # -- Today's FTDs from ant_acc (complementary: clients who got
    #    their first_deposit_date today, as synced from CRM) -----------
    # NOTE: ftd_amount in ant_acc is stored in integer cents, divide by 100
    row = await _safe_query(db, """
        SELECT COUNT(*), COALESCE(SUM(ftd_amount), 0)
        FROM ant_acc
        WHERE first_deposit_date >= CURRENT_DATE
          AND funded = 1
          AND (is_test_account = 0 OR is_test_account IS NULL)
    """, "today_ftds_antacc")
    today_ftds_antacc_count = int(row[0]) if row else 0
    today_ftds_antacc_amount = round(float(row[1]) / 100.0, 2) if row else 0.0

    # -- Today's withdrawals (Approved, excludes bonuses) ---------------
    row = await _safe_query(db, """
        SELECT COUNT(*), COALESCE(SUM(usdamount), 0)
        FROM vtiger_mttransactions
        WHERE transactiontype IN ('Withdrawal', 'Withdraw')
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE
          AND (payment_method IS NULL OR payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
    """, "today_withdrawals")
    today_withdrawals_count = int(row[0]) if row else 0
    today_withdrawals_amount = round(float(row[1]), 2) if row else 0.0

    # -- Net deposits today --------------------------------------------
    net_deposits_today = round(today_deposits_amount - today_withdrawals_amount, 2)

    # -- New accounts registered today (ant_acc.createdtime) -----------
    row = await _safe_query(db, """
        SELECT COUNT(*)
        FROM ant_acc
        WHERE createdtime >= CURRENT_DATE
          AND (is_test_account = 0 OR is_test_account IS NULL)
    """, "new_accounts_today")
    new_accounts_today = int(row[0]) if row else 0

    # ==================================================================
    # SECTION 2: PORTFOLIO OVERVIEW
    # ==================================================================

    # -- Total client balance + credit (positive-balance accounts) -----
    row = await _safe_query(db, """
        SELECT COALESCE(SUM(balance), 0),
               COALESCE(SUM(credit), 0),
               COUNT(*)
        FROM vtiger_trading_accounts
        WHERE balance > 0 AND balance < 10000000
    """, "total_balance")
    total_client_balance = round(float(row[0]), 2) if row else 0.0
    total_client_credit = round(float(row[1]), 2) if row else 0.0
    active_balance_accounts = int(row[2]) if row else 0

    # -- Active traders today: logins that opened at least 1 trade today
    #    Primary source: trades_mt4 (Dealio).
    #    Fallback: modifiedtime proxy if trades_mt4 is empty (still syncing).
    active_traders_today = 0
    active_traders_source = "trades_mt4"

    row = await _safe_query(db, """
        SELECT COUNT(*) FROM trades_mt4
    """, "trades_mt4_count")
    trades_mt4_count = int(row[0]) if row else 0

    if trades_mt4_count > 0:
        row = await _safe_query(db, """
            SELECT COUNT(DISTINCT login)
            FROM trades_mt4
            WHERE open_time >= CURRENT_DATE
        """, "active_traders_today")
        active_traders_today = int(row[0]) if row else 0
    else:
        # Fallback: modifiedtime proxy (unreliable — includes CRM batch updates)
        row = await _safe_query(db, """
            SELECT COUNT(DISTINCT login)
            FROM vtiger_trading_accounts
            WHERE modifiedtime >= CURRENT_DATE
              AND balance > 0
        """, "active_traders_today_fallback")
        active_traders_today = int(row[0]) if row else 0
        active_traders_source = "modifiedtime_fallback"

    # -- Funded clients: total + newly funded today --------------------
    row = await _safe_query(db, """
        SELECT COUNT(*) FILTER (WHERE funded = 1) AS funded_total,
               COUNT(*) FILTER (WHERE funded = 1 AND first_deposit_date >= CURRENT_DATE) AS funded_today,
               COUNT(*) AS total_clients
        FROM ant_acc
        WHERE is_test_account = 0 OR is_test_account IS NULL
    """, "funded_clients")
    funded_clients_total = int(row[0]) if row else 0
    funded_clients_today = int(row[1]) if row else 0
    total_registered_clients = int(row[2]) if row else 0

    # -- Aggregate deposit/withdrawal totals from ant_acc --------------
    # NOTE: total_deposit, total_withdrawal, net_deposit in ant_acc are stored
    #        in integer cents, divide by 100
    row = await _safe_query(db, """
        SELECT COALESCE(SUM(total_deposit), 0),
               COALESCE(SUM(total_withdrawal), 0),
               COALESCE(SUM(net_deposit), 0)
        FROM ant_acc
        WHERE funded = 1
          AND (is_test_account = 0 OR is_test_account IS NULL)
    """, "lifetime_totals")
    lifetime_total_deposits = round(int(row[0]) / 100.0, 2) if row else 0
    lifetime_total_withdrawals = round(int(row[1]) / 100.0, 2) if row else 0
    lifetime_net_deposits = round(int(row[2]) / 100.0, 2) if row else 0

    # ==================================================================
    # SECTION 3: TRENDS (last 7 days)
    # ==================================================================

    # -- Deposits last 7 days (excludes bonuses) -----------------------
    deposits_7d: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT DATE(confirmation_time) AS dt,
               COUNT(*)               AS cnt,
               COALESCE(SUM(usdamount), 0) AS amt
        FROM vtiger_mttransactions
        WHERE transactiontype = 'Deposit'
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE - INTERVAL '7 days'
          AND (payment_method IS NULL OR payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
        GROUP BY DATE(confirmation_time)
        ORDER BY dt ASC
    """, "deposits_7d", fetch_all=True)
    for r in (rows or []):
        deposits_7d.append({
            "date": str(r[0]) if r[0] else "",
            "count": int(r[1]) if r[1] else 0,
            "amount_usd": round(float(r[2]), 2) if r[2] else 0.0,
        })

    # -- Withdrawals last 7 days (excludes bonuses) --------------------
    withdrawals_7d: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT DATE(confirmation_time) AS dt,
               COUNT(*)               AS cnt,
               COALESCE(SUM(usdamount), 0) AS amt
        FROM vtiger_mttransactions
        WHERE transactiontype IN ('Withdrawal', 'Withdraw')
          AND transactionapproval = 'Approved'
          AND confirmation_time >= CURRENT_DATE - INTERVAL '7 days'
          AND (payment_method IS NULL OR payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
        GROUP BY DATE(confirmation_time)
        ORDER BY dt ASC
    """, "withdrawals_7d", fetch_all=True)
    for r in (rows or []):
        withdrawals_7d.append({
            "date": str(r[0]) if r[0] else "",
            "count": int(r[1]) if r[1] else 0,
            "amount_usd": round(float(r[2]), 2) if r[2] else 0.0,
        })

    # -- New registrations last 7 days ---------------------------------
    registrations_7d: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT DATE(createdtime) AS dt, COUNT(*) AS cnt
        FROM ant_acc
        WHERE createdtime >= CURRENT_DATE - INTERVAL '7 days'
          AND (is_test_account = 0 OR is_test_account IS NULL)
        GROUP BY DATE(createdtime)
        ORDER BY dt ASC
    """, "registrations_7d", fetch_all=True)
    for r in (rows or []):
        registrations_7d.append({
            "date": str(r[0]) if r[0] else "",
            "count": int(r[1]) if r[1] else 0,
        })

    # -- FTDs last 7 days (from ant_acc.first_deposit_date) ------------
    # NOTE: ftd_amount is in integer cents, divide by 100
    ftds_7d: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT DATE(first_deposit_date) AS dt,
               COUNT(*)                 AS cnt,
               COALESCE(SUM(ftd_amount), 0) AS amt
        FROM ant_acc
        WHERE first_deposit_date >= CURRENT_DATE - INTERVAL '7 days'
          AND funded = 1
          AND (is_test_account = 0 OR is_test_account IS NULL)
        GROUP BY DATE(first_deposit_date)
        ORDER BY dt ASC
    """, "ftds_7d", fetch_all=True)
    for r in (rows or []):
        ftds_7d.append({
            "date": str(r[0]) if r[0] else "",
            "count": int(r[1]) if r[1] else 0,
            "amount_usd": round(float(r[2]) / 100.0, 2) if r[2] else 0.0,
        })

    # ==================================================================
    # SECTION 4: CLIENT BREAKDOWNS
    # ==================================================================

    # -- Top 10 countries by funded-client count -----------------------
    country_breakdown: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT country_iso, COUNT(*) AS cnt,
               COALESCE(SUM(total_deposit), 0) AS total_dep
        FROM ant_acc
        WHERE funded = 1
          AND (is_test_account = 0 OR is_test_account IS NULL)
          AND country_iso IS NOT NULL AND country_iso != ''
        GROUP BY country_iso
        ORDER BY cnt DESC
        LIMIT 10
    """, "country_breakdown", fetch_all=True)
    for r in (rows or []):
        country_breakdown.append({
            "country_iso": str(r[0]),
            "funded_clients": int(r[1]),
            "total_deposit_usd": round(int(r[2]) / 100.0, 2),  # cents -> dollars
        })

    # -- Regulation breakdown ------------------------------------------
    regulation_breakdown: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT regulation,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE funded = 1) AS funded
        FROM ant_acc
        WHERE (is_test_account = 0 OR is_test_account IS NULL)
          AND regulation IS NOT NULL AND regulation != ''
        GROUP BY regulation
        ORDER BY total DESC
    """, "regulation_breakdown", fetch_all=True)
    for r in (rows or []):
        regulation_breakdown.append({
            "regulation": str(r[0]),
            "total_clients": int(r[1]),
            "funded_clients": int(r[2]),
        })

    # -- Account status breakdown (Sales vs Retention) -----------------
    status_breakdown: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT accountstatus,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE funded = 1) AS funded
        FROM ant_acc
        WHERE (is_test_account = 0 OR is_test_account IS NULL)
          AND accountstatus IS NOT NULL AND accountstatus != ''
        GROUP BY accountstatus
        ORDER BY total DESC
    """, "status_breakdown", fetch_all=True)
    for r in (rows or []):
        status_breakdown.append({
            "status": str(r[0]),
            "total_clients": int(r[1]),
            "funded_clients": int(r[2]),
        })

    # ==================================================================
    # SECTION 5: RETENTION FUNNEL
    # ==================================================================

    # Funnel: total registered -> qualified -> funded -> with balance
    # "Qualified" = has a client_qualification_date set
    row = await _safe_query(db, """
        SELECT COUNT(*) AS total,
               COUNT(client_qualification_date) AS qualified,
               COUNT(*) FILTER (WHERE funded = 1) AS funded
        FROM ant_acc
        WHERE is_test_account = 0 OR is_test_account IS NULL
    """, "funnel_antacc")
    funnel_total = int(row[0]) if row else 0
    funnel_qualified = int(row[1]) if row else 0
    funnel_funded = int(row[2]) if row else 0

    # Clients with active balance (from vtiger_trading_accounts)
    row = await _safe_query(db, """
        SELECT COUNT(DISTINCT vtigeraccountid)
        FROM vtiger_trading_accounts
        WHERE balance > 0 AND balance < 10000000
    """, "funnel_active_balance")
    funnel_with_balance = int(row[0]) if row else 0

    # Retention-assigned clients (from retention_mv, guarded)
    row = await _safe_query(db, """
        SELECT COUNT(*),
               COUNT(*) FILTER (WHERE total_balance > 0)
        FROM retention_mv
    """, "retention_mv")
    retention_total = int(row[0]) if row else 0
    retention_with_balance = int(row[1]) if row else 0

    # ==================================================================
    # SECTION 6: TOP LISTS
    # ==================================================================

    # -- Top 10 depositors today (excludes bonuses) --------------------
    top_depositors_today: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT t.login,
               a.full_name,
               a.country_iso,
               SUM(t.usdamount)      AS amount_usd,
               COUNT(*)              AS txn_count,
               MAX(t.payment_method) AS payment_method
        FROM vtiger_mttransactions t
        LEFT JOIN vtiger_trading_accounts vta
            ON vta.login::text = t.login::text
        LEFT JOIN ant_acc a
            ON a.accountid::text = vta.vtigeraccountid::text
        WHERE t.transactiontype = 'Deposit'
          AND t.transactionapproval = 'Approved'
          AND t.confirmation_time >= CURRENT_DATE
          AND (t.payment_method IS NULL OR t.payment_method NOT IN ('Bonus', 'BonusProtectedPositionCashback'))
        GROUP BY t.login, a.full_name, a.country_iso
        ORDER BY amount_usd DESC
        LIMIT 10
    """, "top_depositors", fetch_all=True)
    for r in (rows or []):
        top_depositors_today.append({
            "login": str(r[0]) if r[0] else "",
            "name": str(r[1]) if r[1] else "",
            "country": str(r[2]) if r[2] else "",
            "amount_usd": round(float(r[3]), 2) if r[3] else 0.0,
            "txn_count": int(r[4]) if r[4] else 0,
            "payment_method": str(r[5]) if r[5] else "",
        })

    # -- Top 10 affiliates by funded-client count (all time) -----------
    #    traders_from_qualified: qualified clients (any time) who traded this month
    top_affiliates: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        WITH traded_this_month AS (
            SELECT DISTINCT vta.vtigeraccountid
            FROM trades_mt4 t
            JOIN vtiger_trading_accounts vta ON vta.login = t.login
            WHERE t.open_time >= DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT a.original_affiliate,
               COUNT(*) AS ftd_count,
               COALESCE(SUM(a.ftd_amount), 0) AS ftd_total,
               COALESCE(SUM(a.total_deposit), 0) AS total_dep,
               COUNT(DISTINCT CASE
                   WHEN a.client_qualification_date IS NOT NULL
                        AND tm.vtigeraccountid IS NOT NULL
                   THEN a.accountid END) AS traders_from_qualified
        FROM ant_acc a
        LEFT JOIN traded_this_month tm
            ON tm.vtigeraccountid::text = a.accountid::text
        WHERE a.funded = 1
          AND (a.is_test_account = 0 OR a.is_test_account IS NULL)
          AND a.original_affiliate IS NOT NULL
          AND a.original_affiliate != ''
        GROUP BY a.original_affiliate
        ORDER BY ftd_count DESC
        LIMIT 10
    """, "top_affiliates", fetch_all=True)
    for r in (rows or []):
        top_affiliates.append({
            "affiliate": str(r[0]),
            "ftd_count": int(r[1]),
            "ftd_amount_usd": round(int(r[2]) / 100.0, 2),  # cents -> dollars
            "total_deposit_usd": round(int(r[3]) / 100.0, 2),  # cents -> dollars
            "traders_from_qualified": int(r[4]) if r[4] else 0,
        })

    # -- Top 10 affiliates by FTD count this month --------------------
    #    traders_from_qualified: qualified THIS MONTH who traded this month
    top_affiliates_month: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        WITH traded_this_month AS (
            SELECT DISTINCT vta.vtigeraccountid
            FROM trades_mt4 t
            JOIN vtiger_trading_accounts vta ON vta.login = t.login
            WHERE t.open_time >= DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT a.original_affiliate,
               COUNT(*) AS ftd_count,
               COALESCE(SUM(a.ftd_amount), 0) AS ftd_total,
               COUNT(DISTINCT CASE
                   WHEN a.client_qualification_date >= DATE_TRUNC('month', CURRENT_DATE)
                        AND tm.vtigeraccountid IS NOT NULL
                   THEN a.accountid END) AS traders_from_qualified
        FROM ant_acc a
        LEFT JOIN traded_this_month tm
            ON tm.vtigeraccountid::text = a.accountid::text
        WHERE a.funded = 1
          AND a.first_deposit_date >= DATE_TRUNC('month', CURRENT_DATE)
          AND (a.is_test_account = 0 OR a.is_test_account IS NULL)
          AND a.original_affiliate IS NOT NULL
          AND a.original_affiliate != ''
        GROUP BY a.original_affiliate
        ORDER BY ftd_count DESC
        LIMIT 10
    """, "top_affiliates_month", fetch_all=True)
    for r in (rows or []):
        top_affiliates_month.append({
            "affiliate": str(r[0]),
            "ftd_count": int(r[1]),
            "ftd_amount_usd": round(int(r[2]) / 100.0, 2),  # cents -> dollars
            "traders_from_qualified": int(r[3]) if r[3] else 0,
        })

    # -- Top 10 affiliates last month (ant_acc) ------------------------
    top_affiliates_last_month: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        WITH traded_last_month AS (
            SELECT DISTINCT vta.vtigeraccountid
            FROM trades_mt4 t
            JOIN vtiger_trading_accounts vta ON vta.login = t.login
            WHERE t.open_time >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
              AND t.open_time <  DATE_TRUNC('month', CURRENT_DATE)
        )
        SELECT a.original_affiliate,
               COUNT(*) AS ftd_count,
               COALESCE(SUM(a.ftd_amount), 0) AS ftd_total,
               COUNT(DISTINCT CASE
                   WHEN a.client_qualification_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
                        AND a.client_qualification_date < DATE_TRUNC('month', CURRENT_DATE)
                        AND tm.vtigeraccountid IS NOT NULL
                   THEN a.accountid END) AS traders_from_qualified
        FROM ant_acc a
        LEFT JOIN traded_last_month tm
            ON tm.vtigeraccountid::text = a.accountid::text
        WHERE a.funded = 1
          AND a.first_deposit_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND a.first_deposit_date <  DATE_TRUNC('month', CURRENT_DATE)
          AND (a.is_test_account = 0 OR a.is_test_account IS NULL)
          AND a.original_affiliate IS NOT NULL
          AND a.original_affiliate != ''
        GROUP BY a.original_affiliate
        ORDER BY ftd_count DESC
        LIMIT 10
    """, "top_affiliates_last_month", fetch_all=True)
    for r in (rows or []):
        top_affiliates_last_month.append({
            "affiliate": str(r[0]),
            "ftd_count": int(r[1]),
            "ftd_amount_usd": round(int(r[2]) / 100.0, 2),
            "traders_from_qualified": int(r[3]) if r[3] else 0,
        })

    # -- Top 10 Proline affiliates this month -------------------------
    top_affiliates_proline_month: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT affiliate_id,
               COUNT(*)                               AS ftd_count,
               COALESCE(SUM(performance_commission), 0) AS total_commission
        FROM proline_data
        WHERE qualified_ftd_date >= DATE_TRUNC('month', CURRENT_DATE)
          AND affiliate_id IS NOT NULL AND affiliate_id != ''
        GROUP BY affiliate_id
        ORDER BY ftd_count DESC
        LIMIT 10
    """, "top_affiliates_proline_month", fetch_all=True)
    for r in (rows or []):
        top_affiliates_proline_month.append({
            "affiliate_id": str(r[0]),
            "ftd_count": int(r[1]),
            "total_commission": round(float(r[2]), 2),
        })

    # -- Top 10 Proline affiliates last month -------------------------
    top_affiliates_proline_last_month: List[Dict[str, Any]] = []
    rows = await _safe_query(db, """
        SELECT affiliate_id,
               COUNT(*)                               AS ftd_count,
               COALESCE(SUM(performance_commission), 0) AS total_commission
        FROM proline_data
        WHERE qualified_ftd_date >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
          AND qualified_ftd_date <  DATE_TRUNC('month', CURRENT_DATE)
          AND affiliate_id IS NOT NULL AND affiliate_id != ''
        GROUP BY affiliate_id
        ORDER BY ftd_count DESC
        LIMIT 10
    """, "top_affiliates_proline_last_month", fetch_all=True)
    for r in (rows or []):
        top_affiliates_proline_last_month.append({
            "affiliate_id": str(r[0]),
            "ftd_count": int(r[1]),
            "total_commission": round(float(r[2]), 2),
        })

    # ==================================================================
    # ASSEMBLE RESPONSE
    # ==================================================================

    return {
        "today": {
            "deposits": {
                "count": today_deposits_count,
                "amount_usd": today_deposits_amount,
            },
            "ftds": {
                "count": today_ftds_count,
                "amount_usd": today_ftds_amount,
            },
            "ftds_crm": {
                "count": today_ftds_antacc_count,
                "amount_usd": today_ftds_antacc_amount,
            },
            "withdrawals": {
                "count": today_withdrawals_count,
                "amount_usd": today_withdrawals_amount,
            },
            "net_deposits_usd": net_deposits_today,
            "new_accounts": new_accounts_today,
        },
        "portfolio": {
            "total_registered_clients": total_registered_clients,
            "funded_clients_total": funded_clients_total,
            "funded_clients_today": funded_clients_today,
            "active_balance_accounts": active_balance_accounts,
            "active_traders_today": active_traders_today,
            "active_traders_source": active_traders_source,
            "total_client_balance_usd": total_client_balance,
            "total_client_credit_usd": total_client_credit,
            "lifetime_total_deposits_usd": lifetime_total_deposits,
            "lifetime_total_withdrawals_usd": lifetime_total_withdrawals,
            "lifetime_net_deposits_usd": lifetime_net_deposits,
        },
        "trends": {
            "deposits_7d": deposits_7d,
            "withdrawals_7d": withdrawals_7d,
            "registrations_7d": registrations_7d,
            "ftds_7d": ftds_7d,
        },
        "breakdowns": {
            "by_country": country_breakdown,
            "by_regulation": regulation_breakdown,
            "by_account_status": status_breakdown,
        },
        "retention_funnel": {
            "total_registered": funnel_total,
            "qualified": funnel_qualified,
            "funded": funnel_funded,
            "with_balance": funnel_with_balance,
            "retention_assigned": retention_total,
            "retention_with_balance": retention_with_balance,
        },
        "top_lists": {
            "top_depositors_today": top_depositors_today,
            "top_affiliates_alltime": top_affiliates,
            "top_affiliates_month": top_affiliates_month,
            "top_affiliates_last_month": top_affiliates_last_month,
            "top_affiliates_proline_month": top_affiliates_proline_month,
            "top_affiliates_proline_last_month": top_affiliates_proline_last_month,
        },
    }

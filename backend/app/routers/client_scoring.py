import asyncio
import logging
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth_deps import get_current_user
from app.models.scoring_rule import ScoringRule
from app.pg_database import AsyncSessionLocal, get_db
from app.rbac import make_page_guard

logger = logging.getLogger(__name__)

router = APIRouter()

# CLAUD-124: Server-side page guard for client-scoring page
_require_client_scoring = make_page_guard("client-scoring")


# ---------------------------------------------------------------------------
# Column map — same fields available in retention_mv as used by retention_tasks
# ---------------------------------------------------------------------------
# CLAUD-72: Expanded to include ALL retention grid fields so every column
# visible in the Retention Manager is also available as a scoring criterion.
# IMPORTANT (CLAUD-57): All numeric expressions MUST use COALESCE so that
# NULL field values evaluate to 0 instead of silently skipping the rule.
# Text expressions are excluded from COALESCE since NULL != 'value'
# is the correct semantic (no data means the rule should not match).
# ---------------------------------------------------------------------------

SCORING_COL_SQL: Dict[str, str] = {
    # --- Financial ---
    "balance":              "COALESCE(m.total_balance, 0)",
    "credit":               "COALESCE(m.total_credit, 0)",
    "equity":               "COALESCE(m.total_equity, 0)",
    "live_equity":          "ABS(COALESCE(m.total_balance, 0) + COALESCE(m.total_credit, 0))",
    "margin":               "COALESCE(m.total_balance, 0) - COALESCE(m.total_equity, 0)",
    "total_profit":         "COALESCE(m.total_profit, 0)",
    "total_deposit":        "COALESCE(m.total_deposit, 0)",
    "net_deposit":          "COALESCE((SELECT aa.net_deposit FROM ant_acc aa WHERE aa.accountid = m.accountid), 0) / 100.0",
    "total_withdrawal":     "COALESCE((SELECT aa.total_withdrawal FROM ant_acc aa WHERE aa.accountid = m.accountid), 0) / 100.0",
    "turnover":             "CASE WHEN (COALESCE(m.total_balance, 0) + COALESCE(m.total_credit, 0)) != 0 THEN COALESCE(m.max_volume, 0) / ABS(COALESCE(m.total_balance, 0) + COALESCE(m.total_credit, 0)) ELSE 0 END",
    "exposure_usd":         "COALESCE(aec.exposure_usd, 0)",
    "exposure_pct":         "COALESCE(aec.exposure_pct, 0)",
    "open_pnl":             "(SELECT COALESCE(SUM(opc.pnl), 0) FROM open_pnl_cache opc JOIN vtiger_trading_accounts vta ON vta.login = opc.login WHERE vta.vtigeraccountid = m.accountid)",
    # --- Trading Activity ---
    "trade_count":          "COALESCE(m.trade_count, 0)",
    "max_open_trade":       "COALESCE(m.max_open_trade, 0)",
    "max_volume":           "COALESCE(m.max_volume, 0)",
    "max_volume_usd":       "COALESCE(m.max_volume, 0)",
    "open_volume":          "COALESCE(m.max_volume, 0)",
    "avg_trade_size":       "CASE WHEN COALESCE(m.trade_count, 0) > 0 THEN COALESCE(m.max_volume, 0) / m.trade_count ELSE 0 END",
    "win_rate":             "COALESCE(m.win_rate, 0)",
    "days_from_last_trade": "COALESCE(CURRENT_DATE - m.last_close_time::date, 0)",
    "open_positions":       "(SELECT COUNT(*) FROM open_pnl_cache opc JOIN vtiger_trading_accounts vta ON vta.login = opc.login WHERE vta.vtigeraccountid = m.accountid AND opc.pnl IS NOT NULL)",
    "unique_symbols":       "COALESCE((SELECT COUNT(DISTINCT t.symbol) FROM trades_mt4 t JOIN vtiger_trading_accounts vta ON vta.login = t.login WHERE vta.vtigeraccountid = m.accountid AND t.cmd IN (0, 1) AND (t.symbol IS NULL OR LOWER(t.symbol) NOT IN ('inactivity','zeroingusd','spread'))), 0)",
    # --- Engagement ---
    "days_in_retention":    "COALESCE(CURRENT_DATE - m.client_qualification_date, 0)",
    "deposit_count":        "COALESCE(m.deposit_count, 0)",
    "withdrawal_count":     "COALESCE((SELECT COUNT(mtt.mttransactionsid) FROM vtiger_mttransactions mtt JOIN vtiger_trading_accounts vta ON vta.login = mtt.login WHERE vta.vtigeraccountid = m.accountid AND mtt.transactionapproval = 'Approved' AND mtt.transactiontype = 'Withdrawal'), 0)",
    "sales_potential":      "COALESCE(NULLIF(TRIM(m.sales_client_potential), '')::numeric, 0)",
    "days_since_last_communication": (
        "COALESCE("
        "  EXTRACT(day FROM NOW() - ("
        "    SELECT MAX(al.timestamp) FROM audit_log al"
        "    WHERE al.client_account_id = m.accountid"
        "    AND al.action_type IN ('status_change','note_added','call_initiated','whatsapp_opened')"
        "  ))::numeric,"
        "  9999"
        ")"
    ),
    # --- Profile ---
    "age":                  "COALESCE(EXTRACT(year FROM AGE(m.birth_date))::numeric, 0)",
    "score":                "COALESCE((SELECT cs.score FROM client_scores cs WHERE cs.accountid = m.accountid), 0)",
    "card_type":            "(SELECT cct.card_type FROM client_card_type cct WHERE cct.accountid = m.accountid LIMIT 1)",
    "accountid":            "m.accountid",
    "full_name":            "m.full_name",
    "sales_client_potential": "m.sales_client_potential",
    "agent_name":           "m.agent_name",
    "country":              "(SELECT aa.country_iso FROM ant_acc aa WHERE aa.accountid = m.accountid)",
    "desk":                 "(SELECT vu.department FROM vtiger_users vu WHERE vu.id::text = m.assigned_to LIMIT 1)",
    "is_favorite":          "CASE WHEN EXISTS (SELECT 1 FROM user_favorites uf WHERE uf.accountid = m.accountid) THEN 1 ELSE 0 END",
    "task_type":            "(SELECT string_agg(rt.name, ',') FROM client_task_assignments cta JOIN retention_tasks rt ON rt.id = cta.task_id WHERE cta.accountid = m.accountid)",
    # --- Date fields (numeric: days since) ---
    "ftd_date":             "COALESCE(CURRENT_DATE - (SELECT aa.first_deposit_date::date FROM ant_acc aa WHERE aa.accountid = m.accountid), 9999)",
    "reg_date":             "COALESCE(CURRENT_DATE - m.client_qualification_date, 0)",
}

SCORING_OP_MAP: Dict[str, str] = {
    "eq":       "=",
    "gt":       ">",
    "lt":       "<",
    "gte":      ">=",
    "lte":      "<=",
    "contains": "ILIKE",
    "between":  "BETWEEN",
}


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ScoringRuleCreate(BaseModel):
    field: str
    operator: str
    value: Optional[str] = None
    score: int
    value_min: Optional[float] = None
    value_max: Optional[float] = None


class ScoringRuleUpdate(BaseModel):
    field: Optional[str] = None
    operator: Optional[str] = None
    value: Optional[str] = None
    score: Optional[int] = None
    value_min: Optional[float] = None
    value_max: Optional[float] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _rule_out(rule: ScoringRule) -> Dict[str, Any]:
    return {
        "id": rule.id,
        "field": rule.field,
        "operator": rule.operator,
        "value": rule.value,
        "score": rule.score,
        "created_at": rule.created_at.isoformat() if rule.created_at else None,
        "value_min": float(rule.value_min) if rule.value_min is not None else None,
        "value_max": float(rule.value_max) if rule.value_max is not None else None,
    }


# ---------------------------------------------------------------------------
# Score recalculation (CLAUD-57)
# ---------------------------------------------------------------------------
# Extracted from etl.py rebuild_retention_mv so it can be triggered
# independently when scoring rules change, without waiting for the next
# hourly MV rebuild cycle.
# ---------------------------------------------------------------------------

async def recalculate_all_scores() -> None:
    """Recompute client_scores for ALL clients using current scoring_rules.

    This iterates over every active scoring rule, queries retention_mv for
    matching accounts, and upserts the summed score into client_scores.
    Clients matching zero rules get score = 0.
    """
    try:
        async with AsyncSessionLocal() as db:
            rules_result = await db.execute(select(ScoringRule).order_by(ScoringRule.id))
            rules = rules_result.scalars().all()

            all_accts_result = await db.execute(text("SELECT accountid FROM retention_mv"))
            all_accountids = [r[0] for r in all_accts_result.fetchall()]

            if not all_accountids:
                logger.info("recalculate_all_scores: no accounts in retention_mv — skipping")
                return

            score_map: Dict[str, int] = {aid: 0 for aid in all_accountids}

            for rule in rules:
                sql_expr = SCORING_COL_SQL.get(rule.field)
                sql_op = SCORING_OP_MAP.get(rule.operator)
                if not sql_expr or not sql_op:
                    logger.warning("recalculate_all_scores: skipping rule %d — unknown field=%s or op=%s", rule.id, rule.field, rule.operator)
                    continue

                # Build condition SQL and bind params based on operator
                if rule.operator == "between":
                    if rule.value_min is None or rule.value_max is None:
                        logger.warning("recalculate_all_scores: skipping rule %d — between requires value_min and value_max", rule.id)
                        continue
                    cond_sql = f"{sql_expr} >= :val_min AND {sql_expr} <= :val_max"
                    bind_params: Dict[str, Any] = {"val_min": float(rule.value_min), "val_max": float(rule.value_max)}
                elif rule.operator == "contains":
                    cond_sql = f"{sql_expr} {sql_op} :val"
                    bind_params = {"val": f"%{rule.value}%"}
                else:
                    try:
                        cast_value = float(rule.value)
                    except (ValueError, TypeError):
                        cast_value = rule.value
                    cond_sql = f"{sql_expr} {sql_op} :val"
                    bind_params = {"val": cast_value}

                q = text(f"SELECT m.accountid FROM retention_mv m LEFT JOIN account_exposure_cache aec ON aec.accountid = m.accountid WHERE {cond_sql}")
                try:
                    matched = await db.execute(q, bind_params)
                    for row in matched.fetchall():
                        if row[0] in score_map:
                            score_map[row[0]] += rule.score
                except Exception as rule_err:
                    logger.warning("recalculate_all_scores: rule %d query failed: %s", rule.id, rule_err)
                    continue

            if score_map:
                upsert_sql = text("""
                    INSERT INTO client_scores (accountid, score, computed_at)
                    VALUES (:accountid, :score, NOW())
                    ON CONFLICT (accountid) DO UPDATE SET score = EXCLUDED.score, computed_at = NOW()
                """)
                await db.execute(upsert_sql, [{"accountid": k, "score": v} for k, v in score_map.items()])
                await db.commit()
                logger.info("recalculate_all_scores: computed and stored scores for %d clients", len(score_map))

    except Exception as e:
        logger.error("recalculate_all_scores failed: %s", e)


def _trigger_score_recalculation() -> None:
    """Fire-and-forget: schedule score recalculation in the background.

    Uses the running event loop to launch the recalculation without
    blocking the HTTP response to the client.
    """
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(recalculate_all_scores())
    except RuntimeError:
        logger.warning("_trigger_score_recalculation: no running event loop — skipping")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@router.get("/retention/scoring-rules")
async def list_scoring_rules(
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
    _g: Any = Depends(_require_client_scoring),
) -> List[Dict[str, Any]]:
    result = await db.execute(
        select(ScoringRule).order_by(ScoringRule.id)
    )
    rules = result.scalars().all()
    return [_rule_out(r) for r in rules]


@router.post("/retention/scoring-rules", status_code=201)
async def create_scoring_rule(
    body: ScoringRuleCreate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    if body.field not in SCORING_COL_SQL:
        raise HTTPException(status_code=400, detail=f"Invalid field: {body.field}")
    if body.operator not in SCORING_OP_MAP:
        raise HTTPException(status_code=400, detail=f"Invalid operator: {body.operator}")
    if body.operator == "between":
        if body.value_min is None or body.value_max is None:
            raise HTTPException(status_code=400, detail="Between operator requires both value_min and value_max")
        if body.value_min >= body.value_max:
            raise HTTPException(status_code=400, detail="value_min must be less than value_max")
    rule = ScoringRule(
        field=body.field,
        operator=body.operator,
        value=body.value if body.value is not None else "",
        score=body.score,
        value_min=body.value_min if body.operator == "between" else None,
        value_max=body.value_max if body.operator == "between" else None,
    )
    db.add(rule)
    await db.commit()
    await db.refresh(rule)
    _trigger_score_recalculation()
    return _rule_out(rule)


@router.put("/retention/scoring-rules/{rule_id}")
async def update_scoring_rule(
    rule_id: int,
    body: ScoringRuleUpdate,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    rule = await db.get(ScoringRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Scoring rule not found")
    if body.field is not None:
        if body.field not in SCORING_COL_SQL:
            raise HTTPException(status_code=400, detail=f"Invalid field: {body.field}")
        rule.field = body.field
    if body.operator is not None:
        if body.operator not in SCORING_OP_MAP:
            raise HTTPException(status_code=400, detail=f"Invalid operator: {body.operator}")
        rule.operator = body.operator
    if body.value is not None:
        rule.value = body.value
    if body.score is not None:
        rule.score = body.score
    # Determine effective operator (might have been updated above or kept from DB)
    effective_op = body.operator if body.operator is not None else rule.operator
    if effective_op == "between":
        # Accept value_min/value_max from body, or keep existing
        if body.value_min is not None:
            rule.value_min = body.value_min
        if body.value_max is not None:
            rule.value_max = body.value_max
        if rule.value_min is None or rule.value_max is None:
            raise HTTPException(status_code=400, detail="Between operator requires both value_min and value_max")
        if float(rule.value_min) >= float(rule.value_max):
            raise HTTPException(status_code=400, detail="value_min must be less than value_max")
    else:
        # Clear between values when switching away from between operator
        rule.value_min = None
        rule.value_max = None
    await db.commit()
    await db.refresh(rule)
    _trigger_score_recalculation()
    return _rule_out(rule)


@router.delete("/retention/scoring-rules/{rule_id}", status_code=204)
async def delete_scoring_rule(
    rule_id: int,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> None:
    rule = await db.get(ScoringRule, rule_id)
    if not rule:
        raise HTTPException(status_code=404, detail="Scoring rule not found")
    await db.delete(rule)
    await db.commit()
    _trigger_score_recalculation()


@router.get("/retention/clients/{accountid}/score-breakdown")
async def get_score_breakdown(
    accountid: str,
    db: AsyncSession = Depends(get_db),
    _: Any = Depends(get_current_user),
) -> Dict[str, Any]:
    """Return per-rule score breakdown for a single client.

    CLAUD-36 perf fix: replaced N+1 per-rule DB queries with a single
    batched SELECT that evaluates all rule conditions in one round-trip
    using CASE WHEN expressions. Rules that cannot be compiled (unknown
    field/op or incomplete between) are returned with matched=False and
    an error message without hitting the DB.
    """
    # Verify client exists + fetch stored score in one query
    client_row = await db.execute(
        text("""
            SELECT 1 AS exists_flag,
                   (SELECT score FROM client_scores WHERE accountid = :aid) AS stored_score
            FROM retention_mv
            WHERE accountid = :aid
            LIMIT 1
        """),
        {"aid": accountid},
    )
    row = client_row.first()
    if not row:
        raise HTTPException(status_code=404, detail="Client not found")
    total_score = int(row[1]) if row[1] is not None else 0

    # Fetch all rules in one query
    rules_result = await db.execute(select(ScoringRule).order_by(ScoringRule.id))
    rules = rules_result.scalars().all()

    if not rules:
        return {
            "accountid": accountid,
            "total_score": total_score,
            "computed_total": 0,
            "breakdown": [],
        }

    # ---------------------------------------------------------------------------
    # Build a single SELECT with one CASE WHEN column per rule.
    # Each column evaluates to 1 if the rule matches, 0 otherwise.
    # Rules that cannot be compiled are tracked separately and returned as errors.
    # ---------------------------------------------------------------------------
    case_cols: List[str] = []
    bind_params: Dict[str, Any] = {"aid": accountid}
    # Index within case_cols for rules that were successfully compiled
    compiled_indices: List[int] = []
    error_rules: Dict[int, str] = {}  # rule index -> error message
    display_values: Dict[int, str] = {}  # rule index -> display_value

    for idx, rule in enumerate(rules):
        sql_expr = SCORING_COL_SQL.get(rule.field)
        sql_op = SCORING_OP_MAP.get(rule.operator)

        if not sql_expr or not sql_op:
            error_rules[idx] = "Unknown field or operator"
            display_values[idx] = rule.value or ""
            continue

        if rule.operator == "between":
            if rule.value_min is None or rule.value_max is None:
                error_rules[idx] = "Between requires value_min and value_max"
                display_values[idx] = rule.value or ""
                continue
            p_min = f"r{idx}_min"
            p_max = f"r{idx}_max"
            bind_params[p_min] = float(rule.value_min)
            bind_params[p_max] = float(rule.value_max)
            cond = f"({sql_expr} >= :{p_min} AND {sql_expr} <= :{p_max})"
            display_values[idx] = f"{rule.value_min} \u2013 {rule.value_max}"
        elif rule.operator == "contains":
            p_val = f"r{idx}_val"
            bind_params[p_val] = f"%{rule.value}%"
            cond = f"({sql_expr} {sql_op} :{p_val})"
            display_values[idx] = rule.value or ""
        else:
            p_val = f"r{idx}_val"
            try:
                bind_params[p_val] = float(rule.value)
            except (ValueError, TypeError):
                bind_params[p_val] = rule.value
            cond = f"({sql_expr} {sql_op} :{p_val})"
            display_values[idx] = rule.value or ""

        case_cols.append(f"CASE WHEN {cond} THEN 1 ELSE 0 END AS r{idx}")
        compiled_indices.append(idx)

    # Execute the single batched query for all compiled rules
    matched_flags: Dict[int, bool] = {}
    if compiled_indices:
        batch_sql = (
            "SELECT " + ", ".join(case_cols)
            + " FROM retention_mv m"
            + " LEFT JOIN account_exposure_cache aec ON aec.accountid = m.accountid"
            + " WHERE m.accountid = :aid"
        )
        try:
            batch_result = await db.execute(text(batch_sql), bind_params)
            batch_row = batch_result.first()
            if batch_row:
                for pos, rule_idx in enumerate(compiled_indices):
                    matched_flags[rule_idx] = bool(batch_row[pos])
        except Exception as batch_err:
            logger.warning("score-breakdown: batch query failed for %s: %s", accountid, batch_err)
            # Fall back: mark all compiled rules as query_error
            for rule_idx in compiled_indices:
                error_rules[rule_idx] = str(batch_err)

    # Assemble breakdown response
    breakdown: List[Dict[str, Any]] = []
    computed_total = 0

    for idx, rule in enumerate(rules):
        base = {
            "rule_id": rule.id,
            "field": rule.field,
            "operator": rule.operator,
            "value": display_values.get(idx, rule.value or ""),
            "score": rule.score,
            "value_min": float(rule.value_min) if rule.value_min is not None else None,
            "value_max": float(rule.value_max) if rule.value_max is not None else None,
        }
        if idx in error_rules:
            base["matched"] = False
            base["error"] = error_rules[idx]
        else:
            is_matched = matched_flags.get(idx, False)
            base["matched"] = is_matched
            if is_matched:
                computed_total += rule.score

        breakdown.append(base)

    return {
        "accountid": accountid,
        "total_score": total_score,
        "computed_total": computed_total,
        "breakdown": breakdown,
    }

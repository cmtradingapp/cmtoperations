from fastapi import APIRouter

from app import database

router = APIRouter()

_COUNTRIES = [
    "South Africa", "Kenya", "Ghana", "Botswana",
    "Namibia", "Rwanda", "Tanzania", "Lesotho", "Uganda",
]

_PLACEHOLDERS = ",".join("?" * len(_COUNTRIES))


@router.get("/filters/statuses")
async def list_statuses() -> list[dict]:
    rows = await database.execute_query(
        "SELECT status_key, value FROM report.ant_sales_status ORDER BY status_key",
        (),
    )
    return [{"id": row["status_key"], "value": row["value"]} for row in rows]


@router.get("/filters/countries")
async def list_countries() -> list[dict]:
    rows = await database.execute_query(
        f"SELECT name, iso2code FROM report.countries "
        f"WHERE name IN ({_PLACEHOLDERS}) ORDER BY name",
        tuple(_COUNTRIES),
    )
    return [{"name": row["name"], "iso2code": row["iso2code"]} for row in rows]

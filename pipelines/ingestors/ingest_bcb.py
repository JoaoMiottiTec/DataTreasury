import os
import json
import datetime as dt
import requests
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv
import time
import logging

log = logging.getLogger(__name__)

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not defined.")

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=4, timeout=10)

BCB_BASE = "https://api.bcb.gov.br/dados/serie"
SERIES = {
    "bcb_selic": 432,
    # "bcb_ipca": 433,
    # "bcb_usdbrl": 1,
}


def to_br_date(d: dt.date) -> str:
    return f"{d.day:02d}/{d.month:02d}/{d.year}"


def parse_brazilian_date(d: str) -> dt.date:
    day, month, year = map(int, d.split("/"))
    return dt.date(year, month, day)


def parse_value_br(v: str):
    try:
        return float(v.replace(".", "").replace(",", "."))
    except Exception:
        return None


GET_LAST_DATE_SQL = """
select date
from bronze.raw_macro_daily
where source = %s
order by date desc
limit 1;
"""


def get_last_ingested_date(source_name: str):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_LAST_DATE_SQL, (source_name,))
            row = cur.fetchone()
            return row[0] if row and row[0] else None


def fetch_series(
    series_id: int,
    start_date: dt.date = None,
    end_date: dt.date = None,
    retries: int = 3,
    backoff: float = 1.5,
    timeout: int = 15,
):
    url = f"{BCB_BASE}/bcdata.sgs.{series_id}/dados"
    params = {"formato": "json"}

    today = dt.date.today()
    if not end_date:
        end_date = today
    if not start_date:
        start_date = today.replace(year=today.year - 10)

    params["dataInicial"] = f"{start_date.day:02d}/{start_date.month:02d}/{start_date.year}"
    params["dataFinal"] = f"{end_date.day:02d}/{end_date.month:02d}/{end_date.year}"

    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                raise ValueError(
                    f"SGS series {series_id} not found (404). Check the series ID."
                )
            log.warning(
                "BCB SGS HTTP %s (attempt %s/%s) - url=%s params=%s",
                resp.status_code,
                attempt,
                retries,
                url,
                params,
            )
        except (requests.Timeout, requests.ConnectionError) as e:
            log.warning(
                "BCB SGS network error (%s) attempt %s/%s", e, attempt, retries
            )

        time.sleep(backoff**attempt)

    raise RuntimeError(f"Failed to fetch series {series_id} after {retries} attempts.")


UPSERT_SQL = """
insert into bronze.raw_macro_daily (source, date, value, raw)
values(%s,%s,%s,%s::jsonb)
on conflict (source, date)
do update set value = excluded.value, raw = excluded.raw;
"""


def build_rows(source_name: str, records: list[dict]):
    rows = []
    for r in records:
        rows.append(
            (
                source_name,
                parse_brazilian_date(r["data"]),
                parse_value_br(r["valor"]),
                json.dumps(r, ensure_ascii=False),
            )
        )
    return rows


def upsert_records(source_name: str, records: list[dict]):
    rows = build_rows(source_name, records)
    if not rows:
        print(f"[WARN] {source_name}: nothing to upsert (0 rows).")
        return
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
            conn.commit()
        print(f"[OK] {source_name}: upserted {len(rows)} rows.")
    except Exception as e:
        sample = rows[0] if rows else None
        print(
            f"[ERROR] upsert_records({source_name}) -> {e}\nSample row: {sample}"
        )
        raise


def run():
    today = dt.date.today()
    ten_years_ago = today.replace(year=today.year - 10)

    for source_name, sgs_id in SERIES.items():
        last = get_last_ingested_date(source_name)
        if last:
            start = max(last + dt.timedelta(days=1), ten_years_ago)
        else:
            start = ten_years_ago

        end = today - dt.timedelta(days=1)

        if start > end:
            print(f"[SKIP] {source_name}: nothing to fetch (start={start} > end={end}).")
            continue

        data = fetch_series(sgs_id, start, end)
        upsert_records(source_name, data)
        print(f"[OK] {source_name}: {len(data)} rows ({start}..{end})")


if __name__ == "__main__":
    run()
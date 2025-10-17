import os, json, datetime as dt
import requests
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("Isn't defined the variable")

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=4, timeout=10)

BCB_BASE = 'https://api.bcb.gov.br/dados/serie'
SERIES = {
    "bcb_selic": 11,  # DAY
    # "bcb_ipca": 433,  # MONTH
    # "bcb_usdbrl": 1,  # DAY
}

def fetch_series(series_id: int):
    url = f"{BCB_BASE}/bcdata.sgs.{series_id}/dados?formato=json"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()

def parse_brazilian_date(d: str) -> dt.date:
    day, month, year = map(int, d.split("/"))
    return dt.date( year, month, day)

def parse_value_br(v: str):
    try:
        return float (v.replace(",", "."))
    except Exception:
        return None
    
UPSERT_SQL = """
insert into bronze.raw_macro_daily (source, date, value, raw)
values(%s,%s,%s,%s)
on conflict (source, date)
do update set value = excluded.value, raw = excluded.raw;
"""

def build_rows(source_name: str, records: list[dict]):
    rows = []
    for r in records:
        rows.append((
            source_name,
            parse_brazilian_date(r["data"]),
            parse_value_br(r["valor"]),
            json.dumps(r, ensure_ascii=False)
        ))
    return rows

def upsert_records(source_name: str, records: list[dict]):
    rows = build_rows(source_name, records)
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
        conn.commit()

def run():
    for source_name, sgs_id in SERIES.items():
        data = fetch_series(sgs_id)
        upsert_records(source_name, data)
        print("Ingestion successfull")

if __name__=="__main__":
    run()
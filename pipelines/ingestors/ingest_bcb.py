import os, json, datetime as dt
import requests
from psycopg_pool import ConnectionPool
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL isn't defined the variable")

pool = ConnectionPool(conninfo=DATABASE_URL, min_size=1, max_size=4, timeout=10)

BCB_BASE = 'https://api.bcb.gov.br/dados/serie'
SERIES = {
    "bcb_selic": 11,  # DAY
    # "bcb_ipca": 433,  # MONTH
    # "bcb_usdbrl": 1,  # DAY
}

def to_br_date(d: dt.date) -> str:
    return f"{d.day:02d}/{d.month:02d}/{d.year}"

def parse_brazilian_date(d: str) -> dt.date:
    day, month, year = map(int, d.split("/"))
    return dt.date(year, month, day)

def parse_value_br(v: str):
    try:
        return float(v.replace(",", "."))
    except Exception:
        return None

GET_LAST_DATE_SQL = "select max(date) from bronze.raw_macro_daily where source = %s;"

def get_last_ingested_date(source_name: str):
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute(GET_LAST_DATE_SQL, (source_name,))
            row = cur.fetchone()
            return row[0] if row and row[0] else None

def fetch_series(series_id: int, data_inicial: dt.date = None, data_final: dt.date = None):
    url = f"{BCB_BASE}/bcdata.sgs.{series_id}/dados"
    params = {"formato": "json"}

    today = dt.date.today()
    if not data_final:
        data_final = today

    if not data_inicial:
        data_inicial = today.replace(year=today.year - 10)

    params["dataInicial"] = f"{data_inicial.day:02d}/{data_inicial.month:02d}/{data_inicial.year}"
    params["dataFinal"] = f"{data_final.day:02d}/{data_final.month:02d}/{data_final.year}"

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

UPSERT_SQL = """
insert into bronze.raw_macro_daily (source, date, value, raw)
values(%s,%s,%s,%s::jsonb)
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
    if not rows:
        print(f"[WARN] {source_name}: nada para upsertar (0 linhas).")
        return
    try:
        with pool.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(UPSERT_SQL, rows)
            conn.commit()
        print(f"[OK] {source_name}: upsert de {len(rows)} linhas")
    except Exception as e:
        sample = rows[0] if rows else None
        print(f"[ERROR] upsert_records({source_name}) -> {e}\nExemplo de linha: {sample}")
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
        end = today

        if start > end:
            print(f"[SKIP] {source_name}: nada para buscar.")
            continue

        data = fetch_series(sgs_id, start, end)
        upsert_records(source_name, data)
        print(f"[OK] {source_name}: Ingestion successfull")

if __name__=="__main__":
    run()
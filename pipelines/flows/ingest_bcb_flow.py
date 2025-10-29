from prefect import flow, task
import datetime as dt
from pipelines.ingestors.ingest_bcb import fetch_series, get_last_ingested_date, upsert_records, SERIES

@task(name="fetch BCB series", retries=3, retry_delay_seconds=15, log_prints=True)
def t_fetch_series(source_name: str, series_id: int):
    today = dt.date.today()
    ten_years_ago = today.replace(year=today.year - 10)

    last = get_last_ingested_date(source_name)
    if last:
        start = max(last + dt.timedelta(days=1), ten_years_ago)
    else:
        start = ten_years_ago
    end = today - dt.timedelta(days=1)

    if start > end:
        print(f"[SKIP] {source_name}: nada para buscar (start={start} > end={end}).")
        return []

    print(f"[RUN] {source_name} ({series_id}) — janela {start}..{end}")
    return fetch_series(series_id, start, end)

@task(name="upsert records", log_prints=True)
def  t_upsert_records(source_name: str, records: list[dict]):
    upsert_records(source_name, records)

@flow(name="BCB Ingestion Flow")
def ingest_bcb_flow(days_back: int = None, full_refresh: bool = False):
    for source_name, sgs_id in SERIES.items():
        data = t_fetch_series.with_options(name=f"fetch_{source_name}")(
            source_name, sgs_id
        )
        t_upsert_records.with_options(name=f"upsert_{source_name}")(
            source_name, data
        )

if __name__=="__main__":
    ingest_bcb_flow()
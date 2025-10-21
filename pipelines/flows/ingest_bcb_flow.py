from prefect import flow, task
from pipelines.ingestors.ingest_bcb import fetch_series, upsert_records, SERIES

@task(name="fetch BCB series", retries=3, retry_delay_seconds=15, log_prints=True)
def t_fetch_series(series_id: int):
    return fetch_series(series_id)

@task(name="upsert records", log_prints=True)
def  t_upsert_records(source_name: str, records: list[dict]):
    upsert_records(source_name, records)

@flow(name="BCB Ingestion Flow")
def ingest_bcb_flow():
    for source_name, sgs_id in SERIES.items():
        data= t_fetch_series(sgs_id)
        t_upsert_records(source_name, data)

if __name__=="__main__":
    ingest_bcb_flow()
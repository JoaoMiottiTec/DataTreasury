from prefect import flow, get_run_logger
from pipelines.flows.ingest_bcb_flow import ingest_bcb_flow
from pipelines.flows.gold_update_flow import gold_update_flow


@flow(name="datatreasury_daily_flow")
def datatreasury_daily_flow():
    logger = get_run_logger()
    logger.info(" Starting DataTreasury Daily Pipeline")

    logger.info(" Step 1 — Ingestão BCB")
    ingest_bcb_flow()

    logger.info(" Step 2 — Atualização GOLD com dbt")
    gold_update_flow()

    logger.info(" Daily Pipeline Completed Successfully!")

if __name__ == "__main__":
    datatreasury_daily_flow()
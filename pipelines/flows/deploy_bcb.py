from prefect import serve
from prefect.client.schemas.schedules import CronSchedule
from pipelines.flows.ingest_bcb_flow import ingest_bcb_flow

if __name__ == "__main__":
    serve(
        ingest_bcb_flow.to_deployment(
            name="BCB Daily",
            work_pool_name="default",
            schedules=[CronSchedule(
                cron="0 8 * * *", 
                timezone="America/Sao_Paulo"
            )],
            tags=["bcb", "daily"],
        )
    )

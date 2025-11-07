from pathlib import Path
import subprocess
from typing import Sequence

from prefect import flow, task, get_run_logger


@task
def run_sql_with_runner(sql_path: str) -> None:
    logger = get_run_logger()
    path = Path(sql_path)
    if not path.exists():
        raise FileNotFoundError(f"SQL não encontrado: {path}")

    cmd = ["python", "ops/sql_runner.py", str(path)]
    logger.info(f"Executando: {' '.join(cmd)}")

    subprocess.run(cmd, check=True)
    logger.info(f"✅ OK: {path.name}")


@flow(name="gold_update_flow")
def gold_update_flow(
    sql_files: Sequence[str] = (
        "warehouse/migrations/0004_gold_populate.sql",
        "warehouse/migrations/0005_gold_views.sql",
    ),
):
    logger = get_run_logger()
    logger.info("Iniciando atualização do GOLD...")

    for sql in sql_files:
        run_sql_with_runner.submit(sql)

    logger.info("Flow GOLD finalizado.")

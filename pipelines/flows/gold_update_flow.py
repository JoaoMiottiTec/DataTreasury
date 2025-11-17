from pathlib import Path
import subprocess

from prefect import flow, task, get_run_logger

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_ROOT / "dbt"


@task
def run_dbt():
    logger = get_run_logger()
    logger.info("Running `dbt run`...")
    result = subprocess.run(
        ["dbt", "run"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt run failed with code {result.returncode}")

    return result.stdout


@task
def test_dbt():
    logger = get_run_logger()
    logger.info("Running `dbt test`...")
    result = subprocess.run(
        ["dbt", "test"],
        cwd=DBT_DIR,
        capture_output=True,
        text=True,
        check=False,
    )

    logger.info(result.stdout)
    if result.returncode != 0:
        logger.error(result.stderr)
        raise RuntimeError(f"dbt test failed with code {result.returncode}")

    return result.stdout


@flow(name="gold_update_flow")
def gold_update_flow():
    logger = get_run_logger()
    logger.info("Starting GOLD update with dbt...")

    run_dbt_result = run_dbt()
    test_dbt_result = test_dbt()

    logger.info("GOLD flow (dbt) completed.")
    return {
        "dbt_run": run_dbt_result[-500:],
        "dbt_test": test_dbt_result[-500:],
    }

if __name__ == "__main__":
    gold_update_flow()

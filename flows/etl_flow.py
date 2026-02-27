from prefect import flow, task
from prefect.logging import get_run_logger
from datetime import datetime, timezone
from src.pipeline.bronze import to_bronze
from src.pipeline.silver import to_silver
from src.pipeline.gold import to_gold


@task(name="ingest-to-bronze", retries=3, retry_delay_seconds=10)
def bronze_task(year: int, month: int, day: int, hour: int):
    logger = get_run_logger()
    logger.info(f"Starting bronze ingestion for {year}-{month:02d}-{day:02d} hour {hour}")
    df = to_bronze(year, month, day, hour)
    logger.info(f"Bronze complete: {len(df)} rows")
    return len(df)


@task(name="transform-to-silver", retries=2, retry_delay_seconds=5)
def silver_task(year: int, month: int, day: int, hour: int):
    logger = get_run_logger()
    logger.info(f"Starting silver transformation")
    df = to_silver(year, month, day, hour)
    logger.info(f"Silver complete: {len(df)} rows")
    return len(df)


@task(name="aggregate-to-gold")
def gold_task(year: int, month: int, day: int, hour: int):
    logger = get_run_logger()
    logger.info(f"Starting gold aggregation")
    gold = to_gold(year, month, day, hour)
    total = sum(len(v) for v in gold.values())
    logger.info(f"Gold complete: {total} total rows across {len(gold)} tables")
    return {k: len(v) for k, v in gold.items()}


@flow(name="dataflow-etl", log_prints=True)
def etl_flow(year: int, month: int, day: int, hour: int):
    print(f"Starting ETL flow for {year}-{month:02d}-{day:02d} hour {hour}")

    bronze_count = bronze_task(year, month, day, hour)
    silver_count = silver_task(year, month, day, hour)
    gold_counts = gold_task(year, month, day, hour)

    print(f"Pipeline complete:")
    print(f"  Bronze: {bronze_count} rows")
    print(f"  Silver: {silver_count} rows")
    print(f"  Gold tables: {gold_counts}")
    return {"bronze": bronze_count, "silver": silver_count, "gold": gold_counts}


if __name__ == "__main__":
    result = etl_flow(year=2024, month=1, day=1, hour=1)
    print(f"Result: {result}")
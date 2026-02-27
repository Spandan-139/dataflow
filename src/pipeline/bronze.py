import polars as pl
from pathlib import Path
from loguru import logger
from datetime import timezone
from src.ingestion.gharchive import ingest_hour

BRONZE_PATH = Path("data/bronze")


def to_bronze(year: int, month: int, day: int, hour: int) -> pl.DataFrame:
    logger.info(f"Building bronze layer for {year}-{month:02d}-{day:02d} hour {hour}...")
    
    events = ingest_hour(year, month, day, hour)
    
    df = pl.DataFrame(events).with_columns([
        pl.col("created_at").str.to_datetime(format="%Y-%m-%dT%H:%M:%SZ", time_unit="us").alias("created_at"),
        pl.col("id").cast(pl.Utf8),
        pl.col("public").cast(pl.Boolean),
        pl.lit(f"{year}-{month:02d}-{day:02d}").alias("date"),
        pl.lit(hour).cast(pl.Int32).alias("hour"),
    ])

    BRONZE_PATH.mkdir(parents=True, exist_ok=True)
    output_path = BRONZE_PATH / f"{year}-{month:02d}-{day:02d}-{hour}.parquet"
    df.write_parquet(output_path)

    logger.info(f"Bronze: {len(df)} rows → {output_path}")
    return df


if __name__ == "__main__":
    df = to_bronze(2024, 1, 1, 0)
    print(df.schema)
    print(df.head(3))
import polars as pl
from pathlib import Path
from loguru import logger

BRONZE_PATH = Path("data/bronze")
SILVER_PATH = Path("data/silver")


def to_silver(year: int, month: int, day: int, hour: int) -> pl.DataFrame:
    logger.info(f"Building silver layer for {year}-{month:02d}-{day:02d} hour {hour}...")

    bronze_path = BRONZE_PATH / f"{year}-{month:02d}-{day:02d}-{hour}.parquet"
    df = pl.read_parquet(bronze_path)

    # Clean and enrich
    df = (
        df
        # Drop nulls on critical fields
        .filter(
            pl.col("actor_login").is_not_null() &
            pl.col("repo_name").is_not_null() &
            pl.col("type").is_not_null()
        )
        # Extract repo owner and name
        .with_columns([
            pl.col("repo_name").str.split("/").list.get(0).alias("repo_owner"),
            pl.col("repo_name").str.split("/").list.get(1).alias("repo_short_name"),
        ])
        # Extract language hint from repo name (simple heuristic)
        .with_columns([
            pl.col("created_at").dt.hour().alias("hour_of_day"),
            pl.col("created_at").dt.weekday().alias("day_of_week"),
            # Classify event category
            pl.when(pl.col("type").is_in(["PushEvent", "CreateEvent", "DeleteEvent"]))
              .then(pl.lit("code"))
            .when(pl.col("type").is_in(["PullRequestEvent", "PullRequestReviewEvent", "PullRequestReviewCommentEvent"]))
              .then(pl.lit("review"))
            .when(pl.col("type").is_in(["IssuesEvent", "IssueCommentEvent"]))
              .then(pl.lit("issues"))
            .when(pl.col("type").is_in(["WatchEvent", "ForkEvent", "PublicEvent"]))
              .then(pl.lit("social"))
            .otherwise(pl.lit("other"))
            .alias("event_category"),
            # Is org event
            pl.col("org").is_not_null().alias("is_org_event"),
        ])
    )

    SILVER_PATH.mkdir(parents=True, exist_ok=True)
    output_path = SILVER_PATH / f"{year}-{month:02d}-{day:02d}-{hour}.parquet"
    df.write_parquet(output_path)

    logger.info(f"Silver: {len(df)} rows → {output_path}")
    return df


if __name__ == "__main__":
    df = to_silver(2024, 1, 1, 0)
    print(df.schema)
    print(df.head(3))
    print(f"\nEvent categories:\n{df['event_category'].value_counts()}")
    print(f"\nOrg events: {df['is_org_event'].sum()} / {len(df)}")
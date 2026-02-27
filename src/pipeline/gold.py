import polars as pl
import duckdb
from pathlib import Path
from loguru import logger

SILVER_PATH = Path("data/silver")
GOLD_PATH = Path("data/gold")


def to_gold(year: int, month: int, day: int, hour: int) -> dict[str, pl.DataFrame]:
    logger.info(f"Building gold layer for {year}-{month:02d}-{day:02d} hour {hour}...")

    silver_path = SILVER_PATH / f"{year}-{month:02d}-{day:02d}-{hour}.parquet"
    df = pl.read_parquet(silver_path)

    GOLD_PATH.mkdir(parents=True, exist_ok=True)

    # Gold 1 — Top repos by activity
    top_repos = (
        df.group_by("repo_name")
        .agg([
            pl.len().alias("total_events"),
            pl.col("type").filter(pl.col("type") == "PushEvent").len().alias("push_count"),
            pl.col("type").filter(pl.col("type") == "WatchEvent").len().alias("star_count"),
            pl.col("type").filter(pl.col("type") == "ForkEvent").len().alias("fork_count"),
            pl.col("type").filter(pl.col("type") == "PullRequestEvent").len().alias("pr_count"),
            pl.col("actor_login").n_unique().alias("unique_contributors"),
        ])
        .sort("total_events", descending=True)
        .head(100)
    )
    top_repos.write_parquet(GOLD_PATH / "top_repos.parquet")
    logger.info(f"Gold top_repos: {len(top_repos)} rows")

    # Gold 2 — Event type distribution
    event_distribution = (
        df.group_by(["type", "event_category"])
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
    )
    event_distribution.write_parquet(GOLD_PATH / "event_distribution.parquet")
    logger.info(f"Gold event_distribution: {len(event_distribution)} rows")

    # Gold 3 — Activity by hour of day
    hourly_activity = (
        df.group_by("hour_of_day")
        .agg([
            pl.len().alias("total_events"),
            pl.col("type").filter(pl.col("type") == "PushEvent").len().alias("push_count"),
            pl.col("actor_login").n_unique().alias("unique_actors"),
        ])
        .sort("hour_of_day")
    )
    hourly_activity.write_parquet(GOLD_PATH / "hourly_activity.parquet")
    logger.info(f"Gold hourly_activity: {len(hourly_activity)} rows")

    # Gold 4 — Top contributors
    top_contributors = (
        df.group_by("actor_login")
        .agg([
            pl.len().alias("total_events"),
            pl.col("repo_name").n_unique().alias("unique_repos"),
            pl.col("type").filter(pl.col("type") == "PushEvent").len().alias("push_count"),
            pl.col("is_org_event").sum().alias("org_events"),
        ])
        .sort("total_events", descending=True)
        .head(100)
    )
    top_contributors.write_parquet(GOLD_PATH / "top_contributors.parquet")
    logger.info(f"Gold top_contributors: {len(top_contributors)} rows")

    # Gold 5 — Org vs personal activity
    org_summary = (
        df.group_by("is_org_event")
        .agg([
            pl.len().alias("total_events"),
            pl.col("actor_login").n_unique().alias("unique_actors"),
            pl.col("repo_name").n_unique().alias("unique_repos"),
        ])
    )
    org_summary.write_parquet(GOLD_PATH / "org_summary.parquet")
    logger.info(f"Gold org_summary: {len(org_summary)} rows")

    return {
        "top_repos": top_repos,
        "event_distribution": event_distribution,
        "hourly_activity": hourly_activity,
        "top_contributors": top_contributors,
        "org_summary": org_summary,
    }


if __name__ == "__main__":
    gold = to_gold(2024, 1, 1, 0)
    print("\n--- Top 5 Repos ---")
    print(gold["top_repos"].head(5))
    print("\n--- Event Distribution ---")
    print(gold["event_distribution"])
    print("\n--- Hourly Activity ---")
    print(gold["hourly_activity"].head(5))
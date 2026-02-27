import duckdb
from pathlib import Path
from loguru import logger

GOLD_PATH = Path("data/gold")
DB_PATH = Path("data/warehouse.db")


def get_connection() -> duckdb.DuckDBPyConnection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def build_warehouse():
    logger.info("Building DuckDB warehouse from gold layer...")
    conn = get_connection()

    tables = {
        "top_repos": GOLD_PATH / "top_repos.parquet",
        "event_distribution": GOLD_PATH / "event_distribution.parquet",
        "hourly_activity": GOLD_PATH / "hourly_activity.parquet",
        "top_contributors": GOLD_PATH / "top_contributors.parquet",
        "org_summary": GOLD_PATH / "org_summary.parquet",
    }

    for table_name, parquet_path in tables.items():
        if not parquet_path.exists():
            logger.warning(f"Skipping {table_name} — file not found")
            continue
        conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_parquet('{parquet_path}')
        """)
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        logger.info(f"Loaded {table_name}: {count} rows")

    conn.close()
    logger.info(f"Warehouse built at {DB_PATH}")


def query(sql: str) -> list[dict]:
    conn = get_connection()
    result = conn.execute(sql).fetchdf()
    conn.close()
    return result.to_dict(orient="records")


def get_top_repos(limit: int = 10) -> list[dict]:
    return query(f"""
        SELECT repo_name, total_events, push_count, star_count,
               fork_count, pr_count, unique_contributors
        FROM top_repos
        ORDER BY total_events DESC
        LIMIT {limit}
    """)


def get_event_distribution() -> list[dict]:
    return query("""
        SELECT type, event_category, count
        FROM event_distribution
        ORDER BY count DESC
    """)


def get_hourly_activity() -> list[dict]:
    return query("""
        SELECT hour_of_day, total_events, push_count, unique_actors
        FROM hourly_activity
        ORDER BY hour_of_day
    """)


def get_top_contributors(limit: int = 10) -> list[dict]:
    return query(f"""
        SELECT actor_login, total_events, unique_repos, push_count, org_events
        FROM top_contributors
        ORDER BY total_events DESC
        LIMIT {limit}
    """)


def get_summary_stats() -> dict:
    conn = get_connection()
    total_events = conn.execute("SELECT SUM(count) FROM event_distribution").fetchone()[0]
    total_repos = conn.execute("SELECT COUNT(*) FROM top_repos").fetchone()[0]
    total_contributors = conn.execute("SELECT COUNT(*) FROM top_contributors").fetchone()[0]
    top_event = conn.execute("SELECT type FROM event_distribution ORDER BY count DESC LIMIT 1").fetchone()[0]
    conn.close()
    return {
        "total_events": int(total_events),
        "total_repos_tracked": int(total_repos),
        "total_contributors_tracked": int(total_contributors),
        "most_common_event": top_event,
    }


if __name__ == "__main__":
    build_warehouse()
    print("\n--- Summary Stats ---")
    print(get_summary_stats())
    print("\n--- Top 5 Repos ---")
    for r in get_top_repos(5):
        print(r)
    print("\n--- Event Distribution ---")
    for r in get_event_distribution():
        print(r)
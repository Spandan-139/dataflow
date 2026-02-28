import pytest
import polars as pl
from pathlib import Path
from unittest.mock import patch


# ── Bronze Tests ──────────────────────────────────────────────────────────────
def test_bronze_schema():
    from src.pipeline.bronze import to_bronze
    sample_events = [
        {"id": "1", "type": "PushEvent", "actor_login": "user1",
         "repo_name": "user1/repo1", "created_at": "2024-01-01T00:00:00Z",
         "public": True, "org": None},
        {"id": "2", "type": "WatchEvent", "actor_login": "user2",
         "repo_name": "user2/repo2", "created_at": "2024-01-01T00:00:01Z",
         "public": True, "org": "myorg"},
    ]
    with patch("src.pipeline.bronze.ingest_hour", return_value=sample_events), \
         patch("src.pipeline.bronze.BRONZE_PATH", Path("tests/tmp")):
        Path("tests/tmp").mkdir(exist_ok=True)
        df = to_bronze(2024, 1, 1, 0)
        assert "id" in df.columns
        assert "type" in df.columns
        assert "created_at" in df.columns
        assert "date" in df.columns
        assert "hour" in df.columns
        assert len(df) == 2


def test_bronze_types():
    from src.pipeline.bronze import to_bronze
    sample_events = [
        {"id": "123", "type": "PushEvent", "actor_login": "user1",
         "repo_name": "user1/repo", "created_at": "2024-01-01T00:00:00Z",
         "public": True, "org": None},
    ]
    with patch("src.pipeline.bronze.ingest_hour", return_value=sample_events), \
         patch("src.pipeline.bronze.BRONZE_PATH", Path("tests/tmp")):
        df = to_bronze(2024, 1, 1, 0)
        assert df["public"].dtype == pl.Boolean
        assert df["hour"].dtype == pl.Int32


# ── Silver Tests ──────────────────────────────────────────────────────────────
def make_bronze_df():
    return pl.DataFrame({
        "id": ["1", "2", "3", "4"],
        "type": ["PushEvent", "WatchEvent", "PullRequestEvent", "IssuesEvent"],
        "actor_login": ["user1", "user2", "user3", "user4"],
        "repo_name": ["owner1/repo1", "owner2/repo2", "owner3/repo3", "owner4/repo4"],
        "created_at": pl.Series([
            "2024-01-01T00:00:00", "2024-01-01T01:00:00",
            "2024-01-01T02:00:00", "2024-01-01T03:00:00"
        ]).str.to_datetime(),
        "public": [True, True, True, True],
        "org": [None, "myorg", None, None],
        "date": ["2024-01-01"] * 4,
        "hour": [0, 1, 2, 3],
    })


def test_silver_event_categories():
    from src.pipeline.silver import to_silver
    bronze_df = make_bronze_df()
    with patch("src.pipeline.silver.pl.read_parquet", return_value=bronze_df), \
         patch("src.pipeline.silver.SILVER_PATH", Path("tests/tmp")):
        df = to_silver(2024, 1, 1, 0)
        categories = df["event_category"].to_list()
        assert "code" in categories
        assert "social" in categories
        assert "review" in categories
        assert "issues" in categories


def test_silver_repo_parsing():
    from src.pipeline.silver import to_silver
    bronze_df = make_bronze_df()
    with patch("src.pipeline.silver.pl.read_parquet", return_value=bronze_df), \
         patch("src.pipeline.silver.SILVER_PATH", Path("tests/tmp")):
        df = to_silver(2024, 1, 1, 0)
        assert "repo_owner" in df.columns
        assert "repo_short_name" in df.columns
        row = df.filter(pl.col("repo_name") == "owner1/repo1")
        assert row["repo_owner"][0] == "owner1"
        assert row["repo_short_name"][0] == "repo1"


def test_silver_null_filtering():
    from src.pipeline.silver import to_silver
    bronze_df = make_bronze_df()
    with patch("src.pipeline.silver.pl.read_parquet", return_value=bronze_df), \
         patch("src.pipeline.silver.SILVER_PATH", Path("tests/tmp")):
        df = to_silver(2024, 1, 1, 0)
        assert df["actor_login"].null_count() == 0


def test_silver_org_flag():
    from src.pipeline.silver import to_silver
    bronze_df = make_bronze_df()
    with patch("src.pipeline.silver.pl.read_parquet", return_value=bronze_df), \
         patch("src.pipeline.silver.SILVER_PATH", Path("tests/tmp")):
        df = to_silver(2024, 1, 1, 0)
        assert "is_org_event" in df.columns
        org_row = df.filter(pl.col("org") == "myorg")
        assert org_row["is_org_event"][0] == True


# ── Gold Tests ────────────────────────────────────────────────────────────────
def make_silver_df():
    return pl.DataFrame({
        "id": ["1", "2", "3", "4", "5"],
        "type": ["PushEvent", "PushEvent", "WatchEvent", "PullRequestEvent", "IssuesEvent"],
        "actor_login": ["user1", "user2", "user3", "user1", "user2"],
        "repo_name": ["owner1/repo1", "owner1/repo1", "owner2/repo2", "owner1/repo1", "owner3/repo3"],
        "created_at": pl.Series([
            "2024-01-01T00:00:00", "2024-01-01T01:00:00", "2024-01-01T02:00:00",
            "2024-01-01T03:00:00", "2024-01-01T04:00:00"
        ]).str.to_datetime(),
        "public": [True] * 5,
        "org": [None, None, "myorg", None, None],
        "date": ["2024-01-01"] * 5,
        "hour": [0, 1, 2, 3, 4],
        "repo_owner": ["owner1", "owner1", "owner2", "owner1", "owner3"],
        "repo_short_name": ["repo1", "repo1", "repo2", "repo1", "repo3"],
        "hour_of_day": [0, 1, 2, 3, 4],
        "day_of_week": [1, 1, 1, 1, 1],
        "event_category": ["code", "code", "social", "review", "issues"],
        "is_org_event": [False, False, True, False, False],
    })


def test_gold_top_repos():
    from src.pipeline.gold import to_gold
    silver_df = make_silver_df()
    with patch("src.pipeline.gold.pl.read_parquet", return_value=silver_df), \
         patch("src.pipeline.gold.GOLD_PATH", Path("tests/tmp")):
        gold = to_gold(2024, 1, 1, 0)
        assert "top_repos" in gold
        top = gold["top_repos"]
        assert "repo_name" in top.columns
        assert "total_events" in top.columns


def test_gold_event_distribution():
    from src.pipeline.gold import to_gold
    silver_df = make_silver_df()
    with patch("src.pipeline.gold.pl.read_parquet", return_value=silver_df), \
         patch("src.pipeline.gold.GOLD_PATH", Path("tests/tmp")):
        gold = to_gold(2024, 1, 1, 0)
        assert "event_distribution" in gold
        dist = gold["event_distribution"]
        types = dist["type"].to_list()
        assert "PushEvent" in types


def test_gold_returns_all_tables():
    from src.pipeline.gold import to_gold
    silver_df = make_silver_df()
    with patch("src.pipeline.gold.pl.read_parquet", return_value=silver_df), \
         patch("src.pipeline.gold.GOLD_PATH", Path("tests/tmp")):
        gold = to_gold(2024, 1, 1, 0)
        assert set(gold.keys()) == {
            "top_repos", "event_distribution", "hourly_activity",
            "top_contributors", "org_summary"
        }
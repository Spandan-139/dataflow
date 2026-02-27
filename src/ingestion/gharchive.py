import httpx
import gzip
import json
from pathlib import Path
from datetime import datetime, timezone
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_fixed

RAW_DATA_PATH = Path("data/raw")


def get_gharchive_url(year: int, month: int, day: int, hour: int) -> str:
    return f"https://data.gharchive.org/{year}-{month:02d}-{day:02d}-{hour}.json.gz"


@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def download_hour(year: int, month: int, day: int, hour: int) -> Path:
    url = get_gharchive_url(year, month, day, hour)
    filename = f"{year}-{month:02d}-{day:02d}-{hour}.json.gz"
    output_path = RAW_DATA_PATH / filename

    if output_path.exists():
        logger.info(f"Already downloaded: {filename}")
        return output_path

    RAW_DATA_PATH.mkdir(parents=True, exist_ok=True)
    logger.info(f"Downloading {url}...")

    with httpx.Client(timeout=60) as client:
        response = client.get(url)
        response.raise_for_status()
        output_path.write_bytes(response.content)

    logger.info(f"Downloaded {filename} ({output_path.stat().st_size / 1024 / 1024:.1f} MB)")
    return output_path


def parse_events(file_path: Path) -> list[dict]:
    events = []
    with gzip.open(file_path, "rt", encoding="utf-8") as f:
        for line in f:
            try:
                event = json.loads(line.strip())
                events.append({
                    "id": event.get("id"),
                    "type": event.get("type"),
                    "actor_login": event.get("actor", {}).get("login"),
                    "repo_name": event.get("repo", {}).get("name"),
                    "created_at": event.get("created_at"),
                    "public": event.get("public", True),
                    "org": event.get("org", {}).get("login") if event.get("org") else None,
                })
            except json.JSONDecodeError:
                continue
    logger.info(f"Parsed {len(events)} events from {file_path.name}")
    return events


def ingest_hour(year: int, month: int, day: int, hour: int) -> list[dict]:
    file_path = download_hour(year, month, day, hour)
    return parse_events(file_path)


if __name__ == "__main__":
    # Test ingestion with one hour of data
    now = datetime.now(timezone.utc)
    events = ingest_hour(2024, 1, 1, 0)
    print(f"Total events: {len(events)}")
    print(f"Sample: {events[0]}")
    event_types = {}
    for e in events:
        event_types[e["type"]] = event_types.get(e["type"], 0) + 1
    print(f"Event types: {event_types}")
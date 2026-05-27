"""
main.py
=======
Orchestrator for the Live Data Analyst Job Dashboard pipeline.

Usage
-----
    # Pull fresh jobs from APIs and load to BigQuery (default)
    python main.py
    python main.py --mode live

    # Replay emergency_backup.json through the new enrichment engine
    python main.py --mode backfill

    # Both -- backfill first (so live overrides if same URL), then live
    python main.py --mode both

    # Restrict to a subset of APIs
    python main.py --apis adzuna,remotive

    # Restrict to a subset of roles
    python main.py --roles "Data Analyst,Data Scientist"

Environment variables expected
------------------------------
    GOOGLE_APPLICATION_CREDENTIALS  -- path to credentials.json
    GCP_PROJECT_ID                  -- e.g. my-first-project-27273
    BQ_DATASET                      -- e.g. data_job_market
    BQ_TABLE                        -- e.g. us_job_data

    ADZUNA_APP_ID, ADZUNA_APP_KEY
    USAJOBS_USER_AGENT, USAJOBS_AUTH_KEY
    JOOBLE_API_KEY
    THE_MUSE_API_KEY               (optional)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

from etl_engine import BigQueryLoader, JobEnricher
from extractors import API_REGISTRY, TARGET_ROLES, fetch_all


# ---------------------------------------------------------------------------
# Logging -- write to BOTH console and etl_pipeline.log
# ---------------------------------------------------------------------------
def _configure_logging() -> None:
    fmt = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    handlers = [
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("etl_pipeline.log", mode="a", encoding="utf-8"),
    ]
    logging.basicConfig(level=logging.INFO, format=fmt, handlers=handlers)


log = logging.getLogger("main")


# ---------------------------------------------------------------------------
# Modes
# ---------------------------------------------------------------------------
def run_live(loader: BigQueryLoader, enricher: JobEnricher,
             roles: list[str], apis: list[str], workers: int) -> int:
    raw = fetch_all(roles=roles, apis=apis, max_workers=workers)
    if not raw:
        log.warning("No raw rows fetched. Check API credentials / quota.")
        return 0
    df = enricher.enrich_many(raw)
    log.info("Enriched DataFrame: %d unique rows, %d columns",
             len(df), len(df.columns))
    return loader.upsert(df)


def run_backfill(loader: BigQueryLoader, enricher: JobEnricher,
                 backup_path: Path) -> int:
    if not backup_path.exists():
        log.error("Backup file %s not found.", backup_path)
        return 0
    log.info("Loading historical jobs from %s...", backup_path)
    with backup_path.open("r", encoding="utf-8") as f:
        raw = json.load(f)
    if not isinstance(raw, list):
        # Sometimes backups are wrapped: {"jobs": [...]} or {"data": [...]}
        for key in ("jobs", "data", "results", "items"):
            if isinstance(raw, dict) and isinstance(raw.get(key), list):
                raw = raw[key]
                break
    if not isinstance(raw, list):
        log.error("emergency_backup.json is not a list of jobs; got %s",
                  type(raw).__name__)
        return 0

    # Normalise legacy field names so the enricher accepts them.
    normalised: list[dict] = []
    for j in raw:
        if not isinstance(j, dict):
            continue
        n = dict(j)  # shallow copy
        # Common legacy aliases
        n.setdefault("job_title", n.get("title"))
        n.setdefault("job_url",   n.get("url") or n.get("link"))
        n.setdefault("source_api", n.get("source") or "historical_backup")
        normalised.append(n)
    log.info("Replaying %d historical rows through enricher...", len(normalised))
    df = enricher.enrich_many(normalised)
    log.info("Enriched DataFrame: %d unique rows", len(df))
    return loader.upsert(df)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Job-listings ETL orchestrator")
    p.add_argument("--mode", choices=["live", "backfill", "both"], default="live")
    p.add_argument("--apis", default="",
                   help="Comma-separated subset of: " + ",".join(API_REGISTRY))
    p.add_argument("--roles", default="",
                   help="Comma-separated subset of TARGET_ROLES")
    p.add_argument("--workers", type=int, default=12,
                   help="ThreadPoolExecutor size (default 12)")
    p.add_argument("--backup", default="emergency_backup.json")
    return p.parse_args()


def main() -> int:
    _configure_logging()
    load_dotenv()
    args = parse_args()

    project_id = os.getenv("GCP_PROJECT_ID")
    dataset    = os.getenv("BQ_DATASET")
    table      = os.getenv("BQ_TABLE")
    if not (project_id and dataset and table):
        log.error("Missing one of GCP_PROJECT_ID / BQ_DATASET / BQ_TABLE in .env")
        return 1

    creds = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or "credentials.json"
    if not Path(creds).exists():
        log.error("Google credentials file not found at %s", creds)
        return 1

    apis = [a.strip() for a in args.apis.split(",") if a.strip()] or list(API_REGISTRY)
    roles = [r.strip() for r in args.roles.split(",") if r.strip()] or TARGET_ROLES
    bad = [a for a in apis if a not in API_REGISTRY]
    if bad:
        log.error("Unknown APIs: %s", bad)
        return 1

    loader   = BigQueryLoader(project_id, dataset, table, credentials_path=creds)
    enricher = JobEnricher()

    log.info("Mode = %s | APIs = %s | Roles = %s",
             args.mode, apis, roles)

    total = 0
    if args.mode in ("backfill", "both"):
        total += run_backfill(loader, enricher, Path(args.backup))
    if args.mode in ("live", "both"):
        total += run_live(loader, enricher, roles, apis, args.workers)

    enricher.stats.log_report()
    log.info("Pipeline finished. Total DML-affected rows: %d", total)
    return 0


if __name__ == "__main__":
    sys.exit(main())
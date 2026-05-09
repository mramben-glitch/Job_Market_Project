import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

import pandas as pd
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "my-first-project-27273"
DATASET = "data_job_market"
TABLE = "us_job_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET}.{TABLE}"

ROLES = [
    "Data Analyst",
    "Product Analyst",
    "Marketing Analyst",
    "Business Analyst",
    "Business Intelligence Analyst",
    "Financial Analyst",
    "Operation Analyst",
    "Data Scientist",
]


def load_environment() -> None:
    """Load environment variables from .env and set Google credentials."""
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path=dotenv_path)

    credentials_path = os.path.join(os.path.dirname(__file__), "credentials.json")
    if os.path.isfile(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


def get_bigquery_client() -> bigquery.Client:
    """Get BigQuery client using service account credentials from env or secret."""
    credentials_json = os.getenv("GOOGLE_CREDENTIALS")
    if credentials_json:
        try:
            creds_dict = json.loads(credentials_json)
            credentials = service_account.Credentials.from_service_account_info(creds_dict)
            return bigquery.Client(project=PROJECT_ID, credentials=credentials)
        except Exception as e:
            print(f"[BigQuery] Failed to load credentials from GOOGLE_CREDENTIALS: {e}")
    return bigquery.Client(project=PROJECT_ID)


def strict_inclusion_filter(job_title: Optional[str]) -> bool:
    """Check if job_title contains one of the target roles (case-insensitive)."""
    if not job_title:
        return False
    job_title_lower = job_title.lower()
    return any(role.lower() in job_title_lower for role in ROLES)


def strip_html(text: Optional[str]) -> Optional[str]:
    """Strip HTML tags from description text."""
    if not text:
        return text
    try:
        return BeautifulSoup(text, "html.parser").get_text(separator=" ").strip()
    except Exception:
        return re.sub(r"<[^>]+>", "", text).strip()


def format_salary_raw(value: Any) -> Optional[str]:
    """Format salary as JSON string or keep as-is."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return str(value)


def get_date_retrieved() -> str:
    """Return current UTC timestamp."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def safe_company_name(company: Any) -> Optional[str]:
    """Extract company name from various formats."""
    if isinstance(company, dict):
        return company.get("display_name") or company.get("name") or company.get("company_name")
    if isinstance(company, str):
        return company
    return None


def safe_date_posted(date_value: Any) -> Optional[str]:
    """Convert date to DATE string (YYYY-MM-DD) for BigQuery. Return None if invalid."""
    if date_value is None or date_value == "":
        return None
    try:
        parsed_date = pd.to_datetime(date_value)
        return parsed_date.strftime("%Y-%m-%d")
    except Exception:
        return None


def build_row(
    job_title: Optional[str],
    company: Any,
    location: Any,
    description: Optional[str],
    salary_raw: Any,
    job_url: Optional[str],
    date_posted: Any = None,
) -> Optional[Dict[str, Any]]:
    """Build a standardized job row for BigQuery. Return None if filtered out."""
    if not strict_inclusion_filter(job_title):
        return None

    return {
        "job_title": job_title,
        "company": safe_company_name(company),
        "location": location,
        "description": strip_html(description),
        "salary_raw": format_salary_raw(salary_raw),
        "job_url": job_url,
        "date_retrieved": get_date_retrieved(),
        "industry": None,
        "skills": None,
        "education": None,
        "benefits": None,
        "remote_status": None,
        "date_posted": safe_date_posted(date_posted),
    }


def get_existing_urls(client: bigquery.Client) -> Set[str]:
    """Fetch all existing job_url values from BigQuery to avoid duplicates."""
    try:
        query = f"SELECT DISTINCT job_url FROM `{TABLE_ID}` WHERE job_url IS NOT NULL"
        results = client.query(query).result()
        return {row.job_url for row in results}
    except Exception as e:
        print(f"[Dedup] Error fetching existing URLs: {e}")
        return set()


def fetch_adzuna(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from Adzuna API for a specific role with pagination."""
    try:
        app_id = os.getenv("ADZUNA_APP_ID")
        app_key = os.getenv("ADZUNA_APP_KEY")
        if not app_id or not app_key:
            return []

        rows = []
        for page in range(1, 11):
            response = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/us/search/{page}",
                params={
                    "app_id": app_id,
                    "app_key": app_key,
                    "results_per_page": 100,
                    "what": role,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            page_results = data.get("results", [])
            if not page_results:
                break

            for item in page_results:
                location_value = None
                if isinstance(item.get("location"), dict):
                    location_value = item.get("location", {}).get("display_name")
                else:
                    location_value = item.get("location")

                job_url = item.get("redirect_url") or item.get("url")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=item.get("title"),
                        company=item.get("company"),
                        location=location_value,
                        description=item.get("description"),
                        salary_raw={
                            "salary_min": item.get("salary_min"),
                            "salary_max": item.get("salary_max"),
                            "salary_currency": item.get("salary_currency"),
                        },
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            time.sleep(1)

        print(f"[Adzuna] {role}: {len(rows)} new jobs (pages 1-{min(10, page)}).")
        return rows
    except Exception as e:
        print(f"[Adzuna] {role}: Error: {e}")
        return []


def fetch_jooble(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from Jooble API for a specific role with pagination."""
    try:
        api_key = os.getenv("JOOBLE_API_KEY")
        if not api_key:
            return []

        rows = []
        for page in range(1, 11):
            response = requests.post(
                f"https://jooble.org/api/{api_key}",
                json={
                    "keywords": role,
                    "location": "United States",
                    "page": page,
                },
                timeout=30,
            )

            if response.status_code in [400, 403]:
                break

            response.raise_for_status()
            data = response.json()
            page_results = data.get("jobs", [])
            if not page_results:
                break

            for item in page_results:
                job_url = item.get("url")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=item.get("title"),
                        company=item.get("company"),
                        location=item.get("location"),
                        description=item.get("description"),
                        salary_raw=item.get("salary"),
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            time.sleep(1)

        print(f"[Jooble] {role}: {len(rows)} new jobs (pages 1-{min(10, page)}).")
        return rows
    except Exception as e:
        print(f"[Jooble] {role}: Error: {e}")
        return []


def fetch_usajobs(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from USA Jobs API for a specific role with pagination."""
    try:
        api_key = os.getenv("USAJOBS_API_KEY")
        email = os.getenv("USAJOBS_EMAIL")
        if not api_key or not email:
            return []

        rows = []
        for page in range(1, 11):
            response = requests.get(
                "https://data.usajobs.gov/api/search",
                params={
                    "ResultsPerPage": 100,
                    "Page": page,
                    "Keyword": role,
                },
                headers={
                    "User-Agent": email,
                    "Authorization-Key": api_key,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            page_results = data.get("SearchResult", {}).get("SearchResultItems", [])
            if not page_results:
                break

            for item in page_results:
                descriptor = item.get("MatchedObjectDescriptor", {})
                location_values = []
                for location in descriptor.get("PositionLocation", []):
                    location_name = location.get("LocationName")
                    if location_name:
                        location_values.append(location_name)

                job_url = descriptor.get("PositionURI")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=descriptor.get("PositionTitle"),
                        company=descriptor.get("OrganizationName"),
                        location=", ".join(location_values) if location_values else None,
                        description=(
                            descriptor.get("UserArea", {})
                            .get("Details", {})
                            .get("JobSummary")
                            or descriptor.get("QualificationSummary")
                            or descriptor.get("PositionSummary")
                        ),
                        salary_raw=descriptor.get("PositionRemuneration"),
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            time.sleep(1)

        print(f"[USAJobs] {role}: {len(rows)} new jobs (pages 1-{min(10, page)}).")
        return rows
    except Exception as e:
        print(f"[USAJobs] {role}: Error: {e}")
        return []


def fetch_themuse(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from The Muse API for a specific role with pagination."""
    try:
        rows = []
        for page in range(0, 10):
            response = requests.get(
                "https://www.themuse.com/api/public/jobs",
                params={
                    "page": page,
                    "search_query": role,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            page_results = data.get("results", [])
            if not page_results:
                break

            for item in page_results:
                locations = [
                    loc.get("name")
                    for loc in item.get("locations", [])
                    if loc.get("name")
                ]
                job_url = item.get("refs", {}).get("landing_page")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=item.get("name"),
                        company=item.get("company", {}).get("name"),
                        location=", ".join(locations) if locations else None,
                        description=item.get("contents"),
                        salary_raw=item.get("salary"),
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            time.sleep(1)

        print(f"[The Muse] {role}: {len(rows)} new jobs (pages 0-{min(10, page)}).")
        return rows
    except Exception as e:
        print(f"[The Muse] {role}: Error: {e}")
        return []


def fetch_remotive(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from Remotive API for a specific role with pagination."""
    try:
        rows = []
        offset = 0
        for page in range(10):
            response = requests.get(
                "https://remotive.com/api/remote-jobs",
                params={
                    "search": role,
                    "limit": 100,
                    "offset": offset,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            page_results = data.get("jobs", [])
            if not page_results:
                break

            for item in page_results:
                job_url = item.get("url")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=item.get("title"),
                        company=item.get("company_name"),
                        location=item.get("candidate_required_location"),
                        description=item.get("description"),
                        salary_raw=item.get("salary"),
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            offset += 100
            time.sleep(1)

        print(f"[Remotive] {role}: {len(rows)} new jobs (offsets 0-{offset - 100}).")
        return rows
    except Exception as e:
        print(f"[Remotive] {role}: Error: {e}")
        return []


def fetch_arbeitnow(role: str, existing_urls: Set[str]) -> List[Dict[str, Any]]:
    """Fetch jobs from Arbeitnow API for a specific role with pagination."""
    try:
        rows = []
        for page in range(1, 11):
            response = requests.get(
                "https://www.arbeitnow.com/api/job-board-api",
                params={
                    "search": role,
                    "page": page,
                    "limit": 100,
                },
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            page_results = data.get("data", [])
            if not page_results:
                break

            for item in page_results:
                job_url = item.get("url")
                if job_url and job_url not in existing_urls:
                    row = build_row(
                        job_title=item.get("title"),
                        company=item.get("company_name"),
                        location=item.get("location"),
                        description=item.get("description"),
                        salary_raw=item.get("salary"),
                        job_url=job_url,
                    )
                    if row:
                        rows.append(row)
                        existing_urls.add(job_url)

            time.sleep(1)

        print(f"[Arbeitnow] {role}: {len(rows)} new jobs (pages 1-{min(10, page)}).")
        return rows
    except Exception as e:
        print(f"[Arbeitnow] {role}: Error: {e}")
        return []


def ensure_table_exists(client: bigquery.Client) -> None:
    """Create BigQuery table if it does not exist."""
    schema = [
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("company", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("salary_raw", "STRING"),
        bigquery.SchemaField("job_url", "STRING"),
        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("skills", "STRING"),
        bigquery.SchemaField("education", "STRING"),
        bigquery.SchemaField("benefits", "STRING"),
        bigquery.SchemaField("remote_status", "STRING"),
        bigquery.SchemaField("date_posted", "DATE"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"[BigQuery] Table {TABLE_ID} ensured.")


def load_rows_to_bigquery(rows: List[Dict[str, Any]], client: bigquery.Client) -> None:
    """Load job rows into BigQuery."""
    if not rows:
        print("[BigQuery] No new rows to load.")
        return

    try:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("company", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("salary_raw", "STRING"),
                bigquery.SchemaField("job_url", "STRING"),
                bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
                bigquery.SchemaField("industry", "STRING"),
                bigquery.SchemaField("skills", "STRING"),
                bigquery.SchemaField("education", "STRING"),
                bigquery.SchemaField("benefits", "STRING"),
                bigquery.SchemaField("remote_status", "STRING"),
                bigquery.SchemaField("date_posted", "DATE"),
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        load_job = client.load_table_from_json(rows, TABLE_ID, job_config=job_config)
        load_job.result()
        print(f"[BigQuery] Loaded {len(rows)} rows into {TABLE_ID}.")
    except Exception as e:
        print(f"[BigQuery] Error loading data: {e}")


def main() -> None:
    """Main pipeline: fetch from all sources for all roles and load to BigQuery."""
    load_environment()
    client = get_bigquery_client()
    ensure_table_exists(client)

    print("Starting production job data pipeline...")
    print(f"Target roles: {', '.join(ROLES)}")

    existing_urls = get_existing_urls(client)
    print(f"[Dedup] Found {len(existing_urls)} existing URLs in BigQuery.")

    all_rows: List[Dict[str, Any]] = []

    for role in ROLES:
        print(f"\n--- Fetching '{role}' ---")
        all_rows.extend(fetch_adzuna(role, existing_urls))
        all_rows.extend(fetch_jooble(role, existing_urls))
        all_rows.extend(fetch_usajobs(role, existing_urls))
        all_rows.extend(fetch_themuse(role, existing_urls))
        all_rows.extend(fetch_remotive(role, existing_urls))
        all_rows.extend(fetch_arbeitnow(role, existing_urls))

    print(f"\n=== Total new rows collected: {len(all_rows)} ===")
    load_rows_to_bigquery(all_rows, client)
    print("Pipeline complete.")


if __name__ == "__main__":
    main()

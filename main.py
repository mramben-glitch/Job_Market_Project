import json
import os
import re
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import requests
import spacy
from bs4 import BeautifulSoup
from dateutil import parser as date_parser
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

# Initialize spaCy model globally
try:
    NLP = spacy.load("en_core_web_sm")
except OSError:
    print("[spaCy] Model not found. Install with: python -m spacy download en_core_web_sm")
    NLP = None

# Skills keyword mapping (supercharged with word boundaries)
SKILLS_KEYWORDS = {
    "R": r"\bR\b",
    "Python": r"\bpython\b",
    "SQL": r"\bsql\b",
    "Excel": r"\bexcel\b",
    "Power BI": r"\bpower\s*bi\b|\bpowerbi\b",
    "Tableau": r"\btableau\b",
    "AWS": r"\baws\b|\bamazon\s*web\s*services\b",
    "Azure": r"\bazure\b|\bmicrosoft\s*azure\b",
    "Spark": r"\bspark\b|\bapache\s*spark\b",
    "Snowflake": r"\bsnowflake\b",
    "Looker": r"\blooker\b",
    "GCP": r"\bgcp\b|\bgoogle\s*cloud\b|\bgoogle\s*cloud\s*platform\b",
    "Pandas": r"\bpandas\b",
}

# Education keywords
EDUCATION_KEYWORDS = {
    "PhD": r"\bphd\b|\bdoctorate\b|\bdoctoral\s*degree\b",
    "Master's": r"\bmaster['s]*\b|\bm\.?a\.?\b|\bm\.?s\.?\b|\bm\.?b\.?a\.?\b",
    "Bachelor's": r"\bbachelor['s]*\b|\bb\.?a\.?\b|\bb\.?s\.?\b|\bbachelor\s*degree\b",
}

# Remote status keywords (aggressive matching)
REMOTE_KEYWORDS = {
    "Remote": r"\bremote\b|\bwork\s*from\s*home\b|\bwfh\b|\bfully?\s*remote\b|\btelecommute\b|\bvirtual\b",
    "Hybrid": r"\bhybrid\b|\bdays\s*on\s*site\b|\bflexible\b|\bmixed\b",
    "On-site": r"\bon[\s-]*site\b|\bonsite\b|\bin\s*office\b|\bin[\s-]*person\b|\boffice\b",
}

# Benefits keywords (aggressive matching)
BENEFITS_KEYWORDS = {
    "401(k)": r"\b401\(?k\)?\b|\bretirement\b|\b401k\b",
    "Health Insurance": r"\bhealth\s*insurance\b|\bmedical\b|\bdental\b|\bvision\b|\bhealthcare\b",
    "PTO": r"\bpto\b|\bpaid\s*time\s*off\b|\bvacation\b|\bpaid\s*leave\b",
    "Bonus": r"\bbonus\b|\bsigning\s*bonus\b",
    "Stock": r"\bstock\s*options\b|\bequity\b|\brsu\b",
}

# All US state abbreviations
US_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
    "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
    "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
    "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
    "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY", "DC"
}

# Global counter for debugging
JOBS_PROCESSED = 0


def load_environment() -> None:
    """Load environment variables from .env and set Google credentials."""
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path=dotenv_path)

    credentials_path = os.path.join(os.path.dirname(__file__), "credentials.json")
    if os.path.isfile(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path


def extract_city_state(location: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Smart location extraction with fallbacks:
    1. Try spaCy NER for city/state
    2. Fallback: scan raw string for US state abbreviations
    3. Fallback: use first word as city guess
    """
    if not location:
        return None, None
    
    location = location.strip()
    city = None
    state = None
    
    # Try spaCy NER first
    if NLP:
        try:
            doc = NLP(location)
            for ent in doc.ents:
                if ent.label_ == "GPE":
                    # Check if it's a state code
                    if len(ent.text) == 2 and ent.text.upper() in US_STATES:
                        state = ent.text.upper()
                    else:
                        if not city:
                            city = ent.text
        except Exception:
            pass
    
    # Fallback: scan raw string for state abbreviations
    if not state:
        words = location.split(",")
        for word in words:
            word_clean = word.strip().upper()
            if len(word_clean) == 2 and word_clean in US_STATES:
                state = word_clean
                break
    
    # Fallback: use first word as city if we don't have one
    if not city:
        words = location.split(",")
        first_word = words[0].strip()
        if first_word and len(first_word) > 0:
            city = first_word
    
    return city, state


def extract_skills(text: Optional[str]) -> Optional[str]:
    """Extract skills from job description. Returns comma-separated skills."""
    if not text:
        return None
    
    text_lower = text.lower()
    found_skills = []
    
    for skill, pattern in SKILLS_KEYWORDS.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            found_skills.append(skill)
    
    return ", ".join(found_skills) if found_skills else None


def extract_education(text: Optional[str]) -> Optional[str]:
    """Extract education requirements from job description."""
    if not text:
        return None
    
    text_lower = text.lower()
    
    # Check in order: PhD, Master's, Bachelor's
    for edu, pattern in EDUCATION_KEYWORDS.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            return edu
    
    return None


def extract_remote_status(description: Optional[str], location: Optional[str]) -> Optional[str]:
    """Extract remote status from description and location fields. Returns 'Remote', 'Hybrid', 'On-site', or 'Not Specified'."""
    combined_text = f"{description or ''} {location or ''}".lower()
    
    # Check in order: Remote, Hybrid, On-site
    for status, pattern in REMOTE_KEYWORDS.items():
        if re.search(pattern, combined_text, re.IGNORECASE):
            return status
    
    # Default to "Not Specified" instead of None
    return "Not Specified"


def extract_benefits(text: Optional[str]) -> Optional[str]:
    """Extract benefits from job description. Returns comma-separated benefits."""
    if not text:
        return None
    
    text_lower = text.lower()
    found_benefits = []
    
    for benefit, pattern in BENEFITS_KEYWORDS.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            found_benefits.append(benefit)
    
    return ", ".join(found_benefits) if found_benefits else None


def parse_salary_range(salary_raw: Any) -> Tuple[Optional[float], Optional[float]]:
    """
    Professional-grade salary parser handling:
    - Commas in numbers (100,000)
    - 'k' or 'K' suffix (multiply by 1000)
    - Hourly rates (/hr or per hour, multiply by 2080 for annual)
    - Ranges (80k-100k)
    - Various formats
    """
    if not salary_raw:
        return None, None
    
    try:
        salary_text = str(salary_raw).lower().strip()
        
        # Handle dictionary format (from APIs)
        if isinstance(salary_raw, dict):
            salary_min = salary_raw.get("salary_min")
            salary_max = salary_raw.get("salary_max")
            
            min_val = None
            max_val = None
            
            if salary_min:
                try:
                    min_val = float(str(salary_min).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            
            if salary_max:
                try:
                    max_val = float(str(salary_max).replace(",", ""))
                except (ValueError, TypeError):
                    pass
            
            # Apply k multiplier if needed
            if min_val and min_val < 1000 and "k" in str(salary_raw).lower():
                min_val *= 1000
            if max_val and max_val < 1000 and "k" in str(salary_raw).lower():
                max_val *= 1000
            
            return min_val, max_val
        
        # Check if hourly rate
        is_hourly = "/hr" in salary_text or "per hour" in salary_text or "hourly" in salary_text
        
        # Extract all numbers (with optional commas and decimals)
        numbers_raw = re.findall(r"[\d,]+\.?\d*", salary_text.replace(",", ""))
        
        if not numbers_raw:
            return None, None
        
        # Convert to floats
        numbers = []
        for num_str in numbers_raw:
            try:
                num = float(num_str)
                # Apply 'k' multiplier if number is small and followed by 'k' in original text
                if num < 10000 and "k" in salary_text:
                    # Check if this specific number had a 'k' after it
                    if re.search(rf"{re.escape(num_str)}\s*k", salary_text):
                        num *= 1000
                numbers.append(num)
            except ValueError:
                continue
        
        if not numbers:
            return None, None
        
        # Handle hourly: multiply by 2080 (standard work year: 40 hrs/week * 52 weeks)
        if is_hourly:
            numbers = [n * 2080 for n in numbers]
        
        # Sort and extract min/max
        numbers.sort()
        salary_min = numbers[0] if len(numbers) >= 1 else None
        salary_max = numbers[-1] if len(numbers) >= 2 else None
        
        return salary_min, salary_max
    
    except Exception:
        pass
    
    return None, None


def parse_relative_date(date_string: Optional[str]) -> Optional[str]:
    """
    Convert relative date strings or any date string to YYYY-MM-DD format.
    Handles: '3 days ago', 'today', 'yesterday', '30+', relative dates, absolute dates.
    """
    if not date_string or date_string == "":
        return None
    
    date_string = str(date_string).lower().strip()
    
    try:
        # Handle immediate/current dates
        if "today" in date_string or "just now" in date_string or "just posted" in date_string:
            return datetime.now(timezone.utc).strftime("%Y-%m-%d")
        
        if "yesterday" in date_string:
            return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # Handle relative date formats with "ago"
        if "ago" in date_string:
            parts = date_string.split()
            # Look for patterns like "3 days ago" or "30+ days ago"
            for i, part in enumerate(parts):
                # Extract number (handle "30+" format)
                num_str = part.rstrip("+")
                if num_str.isdigit():
                    num = int(num_str)
                    # Look for time unit in next part
                    if i + 1 < len(parts):
                        unit = parts[i + 1]
                        if "day" in unit:
                            target_date = datetime.now(timezone.utc) - timedelta(days=num)
                        elif "week" in unit:
                            target_date = datetime.now(timezone.utc) - timedelta(weeks=num)
                        elif "month" in unit:
                            target_date = datetime.now(timezone.utc) - timedelta(days=num * 30)
                        elif "hour" in unit:
                            target_date = datetime.now(timezone.utc) - timedelta(hours=num)
                        else:
                            continue
                        return target_date.strftime("%Y-%m-%d")
        
        # Handle patterns like "30+" without "ago"
        if "+" in date_string:
            num_str = date_string.split("+")[0].strip()
            if num_str.isdigit():
                num = int(num_str)
                # Default to days
                target_date = datetime.now(timezone.utc) - timedelta(days=num)
                return target_date.strftime("%Y-%m-%d")
        
        # Try to parse as a standard date (without fuzzy first)
        try:
            parsed_date = date_parser.parse(date_string, fuzzy=False, ignoretz=True)
            return parsed_date.strftime("%Y-%m-%d")
        except Exception:
            # Try with fuzzy parsing
            parsed_date = date_parser.parse(date_string, fuzzy=True, ignoretz=True)
            return parsed_date.strftime("%Y-%m-%d")
    
    except Exception:
        return None


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
    """Build a standardized job row for BigQuery with all 15 columns. Return None if filtered out."""
    global JOBS_PROCESSED
    
    if not strict_inclusion_filter(job_title):
        return None

    JOBS_PROCESSED += 1

    location_str = location
    if isinstance(location, dict):
        location_str = location.get("display_name") or location.get("name")
    elif not isinstance(location, str):
        location_str = None

    # Extract city and state from location
    city, state = extract_city_state(location_str)
    
    # Clean description
    cleaned_description = strip_html(description)
    
    # Extract skills, education, remote status, benefits
    skills = extract_skills(cleaned_description)
    education = extract_education(cleaned_description)
    remote_status = extract_remote_status(cleaned_description, location_str)
    benefits = extract_benefits(cleaned_description)
    
    # Parse salary
    salary_min, salary_max = parse_salary_range(salary_raw)
    
    # Parse date_posted
    date_posted_str = parse_relative_date(date_posted)

    # Debugging: print every 10th job
    if JOBS_PROCESSED % 10 == 0:
        print(f"[DEBUG] Job #{JOBS_PROCESSED}: ${salary_min or 'N/A'} - ${salary_max or 'N/A'} | State: {state or 'N/A'} | Remote: {remote_status}")

    return {
        "job_title": job_title,
        "company": safe_company_name(company),
        "location": location_str,
        "city": city,
        "state": state,
        "description": cleaned_description,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "job_url": job_url,
        "skills": skills,
        "education": education,
        "remote_status": remote_status,
        "benefits": benefits,
        "date_retrieved": get_date_retrieved(),
        "date_posted": date_posted_str,
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
                        date_posted=item.get("posted_date"),
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
                        date_posted=item.get("update_date") or item.get("posted_date"),
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
                        date_posted=descriptor.get("PublicationStartDate"),
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
                        date_posted=item.get("published_at"),
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
                        date_posted=item.get("publication_date"),
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
                        date_posted=item.get("publication_date") or item.get("posted_date"),
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
    """Create BigQuery table with the 15-column Smart ETL schema if it does not exist."""
    schema = [
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("company", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("salary_min", "FLOAT64"),
        bigquery.SchemaField("salary_max", "FLOAT64"),
        bigquery.SchemaField("job_url", "STRING"),
        bigquery.SchemaField("skills", "STRING"),
        bigquery.SchemaField("education", "STRING"),
        bigquery.SchemaField("remote_status", "STRING"),
        bigquery.SchemaField("benefits", "STRING"),
        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
        bigquery.SchemaField("date_posted", "DATE"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"[BigQuery] Table {TABLE_ID} ensured with Smart ETL schema (15 columns).")


def load_rows_to_bigquery(rows: List[Dict[str, Any]], client: bigquery.Client) -> None:
    """Load job rows into BigQuery with the 15-column Smart ETL schema."""
    if not rows:
        print("[BigQuery] No new rows to load.")
        return

    try:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("company", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("salary_min", "FLOAT64"),
                bigquery.SchemaField("salary_max", "FLOAT64"),
                bigquery.SchemaField("job_url", "STRING"),
                bigquery.SchemaField("skills", "STRING"),
                bigquery.SchemaField("education", "STRING"),
                bigquery.SchemaField("remote_status", "STRING"),
                bigquery.SchemaField("benefits", "STRING"),
                bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
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
    """Main pipeline: Smart ETL job data pipeline with NLP extraction."""
    global JOBS_PROCESSED
    JOBS_PROCESSED = 0  # Reset counter for this run
    
    load_environment()
    client = get_bigquery_client()
    ensure_table_exists(client)

    print("=" * 70)
    print("Starting Smart ETL Job Data Pipeline")
    print("=" * 70)
    print(f"Target roles: {', '.join(ROLES)}")
    print("\nSmart ETL Features:")
    print("  • Salary parsing: min/max extraction with k-suffix & hourly conversion")
    print("  • Location parsing: city/state extraction with state code fallback")
    print("  • Skills detection: R, Python, SQL, Excel, Power BI, Tableau, AWS, Azure, Spark, Snowflake, Looker")
    print("  • Education detection: Bachelor's, Master's, PhD")
    print("  • Remote status detection: Remote, Hybrid, On-site, Not Specified")
    print("  • Benefits detection: 401(k), Health Insurance, PTO, Bonus, Stock")
    print("  • Date parsing: 'today', 'yesterday', '3 days ago', '30+' → YYYY-MM-DD")
    print("  • Debugging: Sample debug output every 10 jobs")
    print()

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

    print(f"\n{'=' * 70}")
    print(f"Total new rows collected: {len(all_rows)}")
    print(f"{'=' * 70}")
    load_rows_to_bigquery(all_rows, client)
    print("Smart ETL Pipeline complete.")


if __name__ == "__main__":
    main()

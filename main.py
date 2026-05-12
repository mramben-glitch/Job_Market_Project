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

# Comprehensive Skills Mapping with Variants (Fuzzy Tolerant)
SKILLS_MAPPING = {
    "Python": ["python", "py"],
    "SQL": ["sql", "tsql", "plsql", "sqlite", "mysql", "postgresql", "oracle sql"],
    "Excel": ["excel", "spreadsheet", "vba"],
    "Power BI": ["power bi", "powerbi", "power-bi", "pbi"],
    "Tableau": ["tableau", "tabpy"],
    "AWS": ["aws", "amazon web services", "amazon aws"],
    "Azure": ["azure", "microsoft azure", "az"],
    "GCP": ["gcp", "google cloud", "google cloud platform", "bigquery"],
    "Spark": ["spark", "apache spark", "pyspark"],
    "Snowflake": ["snowflake", "sf"],
    "Looker": ["looker", "look", "lml"],
    "Pandas": ["pandas", "pd"],
    "R": [],  # R handled separately with strict word boundary
}
# Education keywords mapping
EDUCATION_KEYWORDS = {
    "Bachelor's": r"\bbachelor['s]*\b|\bb\.?a\.?\b|\bb\.?s\.?\b|\bundergraduate\b",
    "Master's": r"\bmaster['s]*\b|\bm\.?s\.?\b|\bm\.?a\.?\b|\bmba\b",
    "PhD": r"\bph\.?d\b|\bdoctorate\b"
}
# State Mapping (Both abbreviations and full names)
STATE_MAPPING = {
    "AL": ["alabama", "al"],
    "AK": ["alaska", "ak"],
    "AZ": ["arizona", "az"],
    "AR": ["arkansas", "ar"],
    "CA": ["california", "ca"],
    "CO": ["colorado", "co"],
    "CT": ["connecticut", "ct"],
    "DE": ["delaware", "de"],
    "FL": ["florida", "fl"],
    "GA": ["georgia", "ga"],
    "HI": ["hawaii", "hi"],
    "ID": ["idaho", "id"],
    "IL": ["illinois", "il"],
    "IN": ["indiana", "in"],
    "IA": ["iowa", "ia"],
    "KS": ["kansas", "ks"],
    "KY": ["kentucky", "ky"],
    "LA": ["louisiana", "la"],
    "ME": ["maine", "me"],
    "MD": ["maryland", "md"],
    "MA": ["massachusetts", "mass", "ma"],
    "MI": ["michigan", "mi"],
    "MN": ["minnesota", "mn"],
    "MS": ["mississippi", "ms"],
    "MO": ["missouri", "mo"],
    "MT": ["montana", "mt"],
    "NE": ["nebraska", "ne"],
    "NV": ["nevada", "nv"],
    "NH": ["new hampshire", "nh"],
    "NJ": ["new jersey", "nj"],
    "NM": ["new mexico", "nm"],
    "NY": ["new york", "ny"],
    "NC": ["north carolina", "nc"],
    "ND": ["north dakota", "nd"],
    "OH": ["ohio", "oh"],
    "OK": ["oklahoma", "ok"],
    "OR": ["oregon", "or"],
    "PA": ["pennsylvania", "penn", "pa"],
    "RI": ["rhode island", "ri"],
    "SC": ["south carolina", "sc"],
    "SD": ["south dakota", "sd"],
    "TN": ["tennessee", "tn"],
    "TX": ["texas", "tx"],
    "UT": ["utah", "ut"],
    "VT": ["vermont", "vt"],
    "VA": ["virginia", "va"],
    "WA": ["washington", "wa"],
    "WV": ["west virginia", "wv"],
    "WI": ["wisconsin", "wi"],
    "WY": ["wyoming", "wy"],
    "DC": ["district of columbia", "washington dc", "dc"],
}

# Comprehensive Benefits Dictionary (Aggressive Semantic Matching)
BENEFITS_MAPPING = {
    "401(k)": [
        "401k", "401(k)", "401 k", "retirement", "401 (k)",
        "retirement plan", "retirement savings", "roth", "matching"
    ],
    "Health Insurance": [
        "health", "health insurance", "medical", "healthcare", "health coverage",
        "dental", "vision", "health plan", "comprehensive health", "medical plan",
        "health benefits", "dental coverage", "vision coverage", "eye care"
    ],
    "PTO": [
        "pto", "paid time off", "vacation", "paid leave", "paid vacation",
        "time off", "holiday", "holidays", "paid holiday", "personal days",
        "days off", "leave", "annual leave", "sick leave", "paid sick"
    ],
    "Bonus": [
        "bonus", "signing bonus", "sign on bonus", "performance bonus",
        "annual bonus", "incentive", "bounty", "reward"
    ],
    "Stock": [
        "stock", "stock option", "stock options", "equity", "rsu",
        "restricted stock", "profit sharing", "shares"
    ],
}

# Create reverse lookup for benefits (for faster matching)
BENEFITS_PATTERNS = {}
for benefit_name, phrases in BENEFITS_MAPPING.items():
    for phrase in phrases:
        BENEFITS_PATTERNS[phrase] = benefit_name

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
    Robust location extraction with comprehensive fallback chain:
    1. spaCy NER for GPE entities
    2. Full state names and abbreviations (both directions)
    3. Remote/non-geographic handling (US/National defaults)
    4. First word fallback for city
    """
    if not location:
        return None, None
    
    location = location.strip()
    city = None
    state = None
    
    # Check for remote/non-geographic locations
    location_lower = location.lower()
    if any(term in location_lower for term in ["remote", "anywhere", "virtual", "distributed", "online", "work from home", "wfh"]):
        # For remote positions without specific state, try to extract if mentioned
        # e.g., "Remote - Texas" or "Remote, CA"
        pass  # Fall through to extraction below
    
    # If explicitly "United States" or "USA" without state, default to "US"
    if re.search(r"\bunited\s*states\b|\busa\b|^us$", location_lower):
        return "USA", "US"
    
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
    
    # Fallback: Comprehensive state matching (abbreviations and full names)
    if not state:
        location_normalized = location.lower()
        # Split by common delimiters
        parts = re.split(r"[,\-|]", location_normalized)
        for part in parts:
            part = part.strip()
            # Check each state mapping
            for state_code, names in STATE_MAPPING.items():
                for name_variant in names:
                    # Exact word match
                    if re.search(rf"\b{re.escape(name_variant)}\b", part):
                        state = state_code
                        break
                if state:
                    break
            if state:
                break
    
    # Fallback: Extract first meaningful word as city
    if not city:
        # Remove state from location if we found it
        location_for_city = location
        if state:
            # Try to remove the state from location string
            for state_code, names in STATE_MAPPING.items():
                if state_code == state:
                    for name_variant in names:
                        location_for_city = re.sub(rf"\b{re.escape(name_variant)}\b", "", location_for_city, flags=re.IGNORECASE)
        
        # Get first non-empty, non-delimiter part
        parts = re.split(r"[,\-|]", location_for_city)
        for part in parts:
            part = part.strip()
            if part and len(part) > 0 and not re.match(r"^\d+$", part):  # Skip numbers
                city = part
                break
    
    # If still no city but we have state, construct a default
    if not city and state:
        city = f"Multiple Cities, {state}"
    
    return city, state


def extract_skills(text: Optional[str]) -> Optional[str]:
    """
    Bulletproof skills extraction with fuzzy matching and punctuation tolerance.
    Handles variations like PowerBI, Power-BI, Node.js, Node JS, etc.
    Exception: R uses strict word boundary matching.
    """
    if not text:
        return None
    
    text_lower = text.lower()
    # Normalize punctuation/spacing for matching (but preserve for strict R)
    text_normalized = re.sub(r"[_\-\s.]+", " ", text_lower)
    
    found_skills = set()
    
    # Check R with strict word boundary (case-insensitive match but keep original case)
    if re.search(r"\br\b", text_lower, re.IGNORECASE):
        found_skills.add("R")
    
    # Check all other skills using fuzzy variant matching
    for skill_name, variants in SKILLS_MAPPING.items():
        for variant in variants:
            # Create flexible pattern: handles punctuation and spacing
            pattern = re.sub(r"[_\-\s.]+", r"[\\s._-]*", re.escape(variant))
            if re.search(rf"\b{pattern}\b", text_normalized, re.IGNORECASE):
                found_skills.add(skill_name)
                break  # Found this skill, move to next
    
    return ", ".join(sorted(found_skills)) if found_skills else None


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
    """
    Aggressive semantic benefits matching using comprehensive phrase library.
    Handles dozens of variations for each benefit category.
    Returns comma-separated benefits or None if none found.
    """
    if not text:
        return None
    
    text_lower = text.lower()
    # Normalize punctuation and spacing
    text_normalized = re.sub(r"[_\-\s.]+", " ", text_lower)
    
    found_benefits = set()
    
    # Check each benefit against its comprehensive phrase list
    for benefit_name, phrases in BENEFITS_MAPPING.items():
        for phrase in phrases:
            # Create flexible pattern for punctuation/spacing
            pattern = re.sub(r"[_\-\s.]+", r"[\\s._-]*", re.escape(phrase))
            if re.search(rf"\b{pattern}\b", text_normalized, re.IGNORECASE):
                found_benefits.add(benefit_name)
                break  # Found this benefit, move to next
    
    return ", ".join(sorted(found_benefits)) if found_benefits else None


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


def parse_relative_date(date_string: Optional[str], api_dict: Optional[Dict[str, Any]] = None, fallback_date: Optional[str] = None) -> str:
    """
    Ironclad date parsing with FORCED FALLBACK to today's date.
    
    Steps:
    1. Check api_dict for standard date keys (created_at, date, publication_date, etc.)
    2. Parse relative formats ("3 days ago", "today", "yesterday", "30+")
    3. Parse absolute date formats
    4. FORCED FALLBACK: If nothing works, return fallback_date (today's date)
    
    NEVER returns None. Always returns YYYY-MM-DD.
    """
    # First, try to extract from API dict if provided
    if api_dict and isinstance(api_dict, dict):
        date_candidates = [
            "created_at", "created", "date", "publication_date", "posted_date",
            "update_date", "published_at", "posted", "posted_on", "date_posted"
        ]
        for key in date_candidates:
            if key in api_dict and api_dict[key]:
                try:
                    parsed = parse_relative_date(str(api_dict[key]), None, fallback_date)
                    if parsed:
                        return parsed
                except Exception:
                    continue
    
    # If no date_string provided, use fallback immediately
    if not date_string or date_string == "":
        return fallback_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
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
            for i, part in enumerate(parts):
                # Extract number (handle "30+" format)
                num_str = part.rstrip("+").rstrip(".")
                if num_str.replace("-", "").isdigit():
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
            num_str = date_string.split("+")[0].strip().rstrip(".")
            if num_str.isdigit():
                num = int(num_str)
                # Default to days
                target_date = datetime.now(timezone.utc) - timedelta(days=num)
                return target_date.strftime("%Y-%m-%d")
        
        # Try to parse as a standard date (without fuzzy first for accuracy)
        try:
            parsed_date = date_parser.parse(date_string, fuzzy=False, ignoretz=True)
            return parsed_date.strftime("%Y-%m-%d")
        except Exception:
            # Try with fuzzy parsing
            parsed_date = date_parser.parse(date_string, fuzzy=True, ignoretz=True)
            return parsed_date.strftime("%Y-%m-%d")
    
    except Exception:
        pass
    
    # FORCED FALLBACK: If we reach here, return today's date
    return fallback_date or datetime.now(timezone.utc).strftime("%Y-%m-%d")


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
    api_dict: Optional[Dict[str, Any]] = None,
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
    
    # Get today's date for fallback
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Parse date_posted with ironclad fallback to today's date
    date_posted_str = parse_relative_date(date_posted, api_dict, today_date)

    # Debugging: print every 10th job
    if JOBS_PROCESSED % 10 == 0:
        print(f"[DEBUG] Job #{JOBS_PROCESSED}: ${salary_min or 'N/A'} - ${salary_max or 'N/A'} | State: {state or 'N/A'} | Remote: {remote_status} | Skills: {skills or 'N/A'}")

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
        "date_posted": date_posted_str,  # GUARANTEED non-null YYYY-MM-DD
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
                        api_dict=item,
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
                        api_dict=item,
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
                        api_dict=descriptor,
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
                        api_dict=item,
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
                        api_dict=item,
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
                        api_dict=item,
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

"""
cloud_pipeline.py - PRODUCTION WEEKLY DEEP HARVEST (FORTIFIED CLOUD EDITION)
"""
import os
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, Literal
import httpx
from dotenv import load_dotenv
from google import genai
from google.cloud import bigquery
from google.oauth2 import service_account
from pydantic import BaseModel

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("cloud_pipeline")

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "my-first-project-27273")
TABLE_ID = f"{PROJECT_ID}.data_job_market.us_job_data"

sa_json_string = os.getenv("GCP_SA_KEY_JSON")
if sa_json_string:
    try:
        credentials_info = json.loads(sa_json_string)
        bq_client = bigquery.Client(
            project=PROJECT_ID, 
            credentials=service_account.Credentials.from_service_account_info(credentials_info)
        )
        log.info("Authenticated BigQuery via GitHub Secret Token.")
    except Exception as auth_err:
        log.error(f"Credentials parse error: {auth_err}")
        bq_client = bigquery.Client(project=PROJECT_ID)
else:
    bq_client = bigquery.Client(project=PROJECT_ID)

gemini_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

class JobExtracted(BaseModel):
    job_title: Optional[str] = None
    company: Optional[str] = None
    industry: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    location_evidence: Optional[str] = None  
    description: Optional[str] = None
    salary_min_annual: Optional[float] = None
    salary_max_annual: Optional[float] = None
    salary_evidence: Optional[str] = None    
    tools: list[str] = []
    hard_skills: list[str] = []
    soft_skills: list[str] = []
    remote_status: Literal["Remote", "Hybrid", "On-site", "Unspecified"] = "Unspecified"
    education: Literal["High School", "Associate", "Bachelor", "Master", "PhD", "Unspecified"] = "Unspecified"
    employment_type: Literal["Full-time", "Part-time", "Contract", "Internship", "Freelance", "Unspecified"] = "Unspecified"
    benefits: list[str] = []
    date_posted: Optional[str] = None

JSONLD_RE = re.compile(
    r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)

def clean_date_string(val):
    if not val:
        return None
    match = re.search(r'(\d{4})-(\d{2})-(\d{2})', str(val))
    return match.group(0) if match else None

def safe_float(val) -> Optional[float]:
    if val is None:
        return None
    try:
        if isinstance(val, (int, float)):
            return float(val)
        cleaned = re.sub(r'[^\d\.]', '', str(val))
        return float(cleaned) if cleaned else None
    except Exception:
        return None

def extract_jsonld_metadata(html: str) -> Optional[dict]:
    for m in JSONLD_RE.finditer(html):
        try:
            data = json.loads(m.group(1).strip())
            if isinstance(data, list):
                for item in data:
                    if item.get("@type") == "JobPosting":
                        return item
            elif isinstance(data, dict):
                if data.get("@type") == "JobPosting":
                    return data
                elif "@graph" in data and isinstance(data["@graph"], list):
                    for item in data["@graph"]:
                        if item.get("@type") == "JobPosting":
                            return item
        except Exception:
            continue
    return None

def fetch_live_page_context(url: str) -> str:
    if not url or not url.startswith("http"):
        return ""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    try:
        with httpx.Client(follow_redirects=True, timeout=12.0) as client:
            resp = client.get(url, headers=headers)
            if resp.status_code == 200:
                raw_html = resp.text
                jsonld_data = extract_jsonld_metadata(raw_html)
                if jsonld_data:
                    return f"--- FOUND JSON-LD STRUCTURED METADATA ---\n{json.dumps(jsonld_data)}\n"
                
                html_cleaned = re.sub(r'<(script|style).*?>.*?</\1>', '', raw_html, flags=re.DOTALL)
                body_match = re.search(r'<body.*?>(.*?)</body>', html_cleaned, flags=re.DOTALL | re.IGNORECASE)
                text_corpus = body_match.group(1) if body_match else html_cleaned
                clean_text = re.sub(r'<[^>]+>', ' ', text_corpus)
                return " ".join(clean_text.split())[:5000]
    except Exception:
        pass
    return ""

def build_raw_fallback_row(raw_job: dict) -> dict:
    return {
        "date_posted": clean_date_string(raw_job.get("job_posted_at_datetime_utc")),
        "job_title": raw_job.get("job_title") or "N/A",
        "company": raw_job.get("employer_name") or "N/A",
        "industry": raw_job.get("industry") or "N/A",
        "city": raw_job.get("job_city"),
        "state": raw_job.get("job_state"),
        "description": raw_job.get("job_description"),
        "job_url": raw_job.get("job_apply_link") or "Missing Link",
        "salary_min": safe_float(raw_job.get("job_min_salary")),
        "salary_max": safe_float(raw_job.get("job_max_salary")),
        "tools": None, "hard_skills": None, "soft_skills": None,
        "remote_status": "Remote" if raw_job.get("job_is_remote") is True else "On-site" if raw_job.get("job_is_remote") is False else "Unspecified",
        "employment_type": "Unspecified", "benefits": None, "education": "Unspecified",
        "date_retrieved": datetime.now(timezone.utc).strftime('%Y-%m-%d')
    }

def process_single_job_with_retry(raw_job: dict, max_retries: int = 5) -> tuple[dict, str]:
    highlights = raw_job.get("job_highlights", {})
    nested_benefits = ". ".join(highlights.get("Benefits", [])) if isinstance(highlights.get("Benefits", []), list) else ""
    nested_quals = ". ".join(highlights.get("Qualifications", [])) if isinstance(highlights.get("Qualifications", []), list) else ""
    nested_responsibilities = ". ".join(highlights.get("Responsibilities", [])) if isinstance(highlights.get("Responsibilities", []), list) else ""

    scraped_supplemental_context = ""
    job_url = raw_job.get("job_apply_link")
    if (not raw_job.get("job_city") or not raw_job.get("job_min_salary")) and job_url:
        scraped_supplemental_context = fetch_live_page_context(job_url)

    metadata_context = (
        f"--- UPSTREAM API STRUCTURED DATA ---\n"
        f"Provided Min Salary: {raw_job.get('job_min_salary')}\n"
        f"Provided Max Salary: {raw_job.get('job_max_salary')}\n"
        f"Provided City: {raw_job.get('job_city')}\n"
        f"Provided State: {raw_job.get('job_state')}\n"
        f"Provided Is Remote Flag: {raw_job.get('job_is_remote')}\n"
        f"-------------------------------------\n"
    )

    prompt = (
        "You are an expert Data Ingestion Agent. Output a clean JSON object following the schema restrictions.\n\n"
        "STRICT RETAINMENT DIRECTIVES:\n"
        "1. If Upstream Structured Data fields are populated, map them exactly. Never overwrite valid upstream information.\n"
        "2. If Upstream Structured Data is null, locate structural mentions inside the RAW DESCRIPTION, HIDDEN CONTEXT, and SCRAPED SUPPLEMENTAL TEXT.\n"
        "3. FOR SALARY & LOCATION: If you fill a missing value from the text, you MUST populate 'salary_evidence' or 'location_evidence' with the exact matching snippet. If absent, leave null. Do not guess.\n"
        "4. Output array fields like tools, hard_skills, and benefits as arrays of plain strings.\n\n"
        "--- FEW-SHOT POSITIVE RECOVERY EXAMPLE ---\n"
        "Input Metadata: Provided City: null, Provided State: null\n"
        "Input Description: 'We are hiring an analyst for our operations group in Austin, Texas.'\n"
        "Output Schema Mapping: city='Austin', state='Texas', location_evidence='Austin, Texas'\n\n"
        "--- FEW-SHOT METADATA PRESERVATION EXAMPLE ---\n"
        "Input Metadata: Provided City: Chicago, Provided State: IL\n"
        "Input Description: 'Join our national analytics workforce. Remote optional.'\n"
        "Output Schema Mapping: city='Chicago', state='IL', location_evidence=null\n\n"
        "--- FEW-SHOT JSON-LD SCHEMA SOURCE EXAMPLE ---\n"
        "Input Context: '--- FOUND JSON-LD STRUCTURED METADATA ---\\n{\"jobLocation\": {\"address\": {\"addressLocality\": \"Denver\", \"addressRegion\": \"CO\"}}}'\n"
        "Output Schema Mapping: city='Denver', state='CO', location_evidence='JSON-LD Entry'\n\n"
        f"{metadata_context}\n"
        f"--- RAW DESCRIPTION ---\n{raw_job.get('job_description', '')}\n\n"
        f"--- HIDDEN HIGH-YIELD CONTEXT ---\n"
        f"Qualifications: {nested_quals}\nBenefits: {nested_benefits}\nResponsibilities: {nested_responsibilities}\n\n"
        f"--- SCRAPED SUPPLEMENTAL CONTEXT ---\n{scraped_supplemental_context}"
    )

    backoff_delay = 4.0
    for attempt in range(1, max_retries + 1):
        try:
            response = gemini_client.models.generate_content(
                model='models/gemini-2.5-flash', contents=prompt,
                config={'response_mime_type': 'application/json', 'response_schema': JobExtracted}
            )
            r = response.parsed
            
            time.sleep(4.5)
            
            return {
                "date_posted": clean_date_string(raw_job.get("job_posted_at_datetime_utc")),
                "job_title": r.job_title or raw_job.get("job_title") or "N/A",
                "company": r.company or raw_job.get("employer_name") or "N/A",
                "industry": r.industry or raw_job.get("industry") or "N/A",
                "city": r.city or raw_job.get("job_city"),
                "state": r.state or raw_job.get("job_state"),
                "description": r.description or raw_job.get("job_description"),
                "job_url": job_url or "Missing Link",
                "salary_min": safe_float(r.salary_min_annual) if r.salary_min_annual is not None else safe_float(raw_job.get("job_min_salary")),
                "salary_max": safe_float(r.salary_max_annual) if r.salary_max_annual is not None else safe_float(raw_job.get("job_max_salary")),
                "tools": ", ".join(sorted(list(set(r.tools)))) if r.tools else None,
                "hard_skills": ", ".join(sorted(list(set(r.hard_skills)))) if r.hard_skills else None,
                "soft_skills": ", ".join(sorted(list(set(r.soft_skills)))) if r.soft_skills else None,
                "remote_status": r.remote_status if r.remote_status != "Unspecified" else ("Remote" if raw_job.get("job_is_remote") is True else "On-site" if raw_job.get("job_is_remote") is False else "Unspecified"),
                "employment_type": r.employment_type if r.employment_type != "Unspecified" else "Unspecified",
                "benefits": ", ".join(sorted(list(set(r.benefits)))) if r.benefits else None,
                "education": r.education,
                "date_retrieved": datetime.now(timezone.utc).strftime('%Y-%m-%d')
            }, "enriched"
        except Exception as err:
            err_msg = str(err)
            if "429" in err_msg or "RESOURCE_EXHAUSTED" in err_msg:
                log.warning(f"Rate barrier flagged (429) on attempt {attempt}/{max_retries}. Pausing {backoff_delay}s...")
                time.sleep(backoff_delay)
                backoff_delay *= 2
            else:
                log.error(f"GenAI extraction failure, falling back to base metrics: {err}")
                return build_raw_fallback_row(raw_job), "fallback_raw"
                
    return build_raw_fallback_row(raw_job), "exhausted_raw"

def calculate_completeness(job: dict) -> int:
    return sum(1 for v in job.values() if v is not None and v != "" and v != "N/A" and v != "Unspecified")

def main():
    start_time = time.time()
    queries = [
        "Data Analyst or Product Analyst in USA",
        "Business Analyst or BI Analyst in USA",
        "Data Scientist in USA",
        "Marketing Analyst or Finance Analyst in USA",
        "Operations Analyst or Healthcare Analyst in USA"
    ]
    
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }

    deduplicated_jobs = {}
    consecutive_exhaustions = 0
    metrics = {"harvested": 0, "processed": 0, "enriched": 0, "fallback_raw": 0, "exhausted_raw": 0, "loaded": 0}

    log.info("Launching production-grade fortified weekly cloud harvest pipeline...")

    with httpx.Client() as client:
        for q in queries:
            try:
                params = {
                    "query": q, "page": "1", "num_pages": "10", "date_posted": "week", "country": "us", "remote_jobs_only": "false"
                }
                response = client.get("https://jsearch.p.rapidapi.com/search", headers=headers, params=params, timeout=30.0)
                if response.status_code == 200:
                    for job in response.json().get("data", []):
                        metrics["harvested"] += 1
                        
                        company_clean = str(job.get("employer_name", "")).strip().lower()
                        title_clean = str(job.get("job_title", "")).strip().lower()
                        city_clean = str(job.get("job_city", "")).strip().lower()
                        dedup_tuple = (company_clean, title_clean, city_clean)

                        if dedup_tuple not in deduplicated_jobs:
                            deduplicated_jobs[dedup_tuple] = job
                        else:
                            current_score = calculate_completeness(deduplicated_jobs[dedup_tuple])
                            incoming_score = calculate_completeness(job)
                            if incoming_score > current_score:
                                deduplicated_jobs[dedup_tuple] = job
                time.sleep(1.2)
            except Exception as e:
                log.warning(f"API download path anomaly on track '{q}': {e}")

    job_list = list(deduplicated_jobs.values())
    total_to_process = len(job_list)
    log.info(f"Deduplication step completed. Processing {total_to_process} unique items sequentially...")

    buffer = []
    for job in job_list:
        metrics["processed"] += 1
        processed_row, status = process_single_job_with_retry(job)
        
        metrics[status] += 1
        if processed_row:
            buffer.append(processed_row)
            
        if status == "exhausted_raw":
            consecutive_exhaustions += 1
        else:
            consecutive_exhaustions = 0

        if consecutive_exhaustions >= 3:
            log.critical("Consecutive rate limits exhausted. Aborting loop processing early to safeguard data logs.")
            break

    if buffer:
        chunk_size = 50
        for i in range(0, len(buffer), chunk_size):
            chunk = buffer[i:i + chunk_size]
            try:
                job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
                load_job = bq_client.load_table_from_json(chunk, TABLE_ID, job_config=job_config)
                load_job.result()
                metrics["loaded"] += len(chunk)
            except Exception as bq_err:
                log.error(f"BigQuery validation rejection on batch window {i}-{i+chunk_size}: {bq_err}")

    elapsed = int(time.time() - start_time)
    summary_string = f"METRICS_SUMMARY: harvested={metrics['harvested']}, unique={total_to_process}, enriched={metrics['enriched']}, fallback={metrics['fallback_raw']}, exhausted={metrics['exhausted_raw']}, loaded={metrics['loaded']}, runtime_seconds={elapsed}"
    print(summary_string)
    log.info(summary_string)

if __name__ == "__main__":
    main()
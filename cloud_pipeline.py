import os
import time
import logging
import httpx
import json  # Securely parses the service account JSON secret
from google.cloud import bigquery
from google.oauth2 import service_account  # Converts parsed JSON into active Google Credentials
from google import genai
from google.genai import types
from dotenv import load_dotenv

# Initialize Environment and Logging
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
log = logging.getLogger("cloud_pipeline")

# Infrastructure Constants
PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_ID = "data_job_market"
TABLE_NAME = "us_job_data"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

# Initialize Google Cloud Clients with explicit Service Account verification
try:
    sa_key_env = os.getenv("GCP_SA_KEY_JSON")
    if sa_key_env:
        # Running in GitHub Actions: parse the secret JSON string directly
        info = json.loads(sa_key_env)
        credentials = service_account.Credentials.from_service_account_info(info)
        bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    else:
        # Local Desktop Testing: fallback to default environmental tokens (ADC)
        bq_client = bigquery.Client(project=PROJECT_ID)
        
    log.info("Authenticated BigQuery via Secure Token.")
except Exception as e:
    log.error(f"Failed to initialize BigQuery client: {e}")
    raise

# Initialize Gemini Client
ai_client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

def calculate_completeness(job):
    """Calculates a primitive score based on field presence to assist deduplication."""
    score = 0
    fields_to_check = ["job_description", "job_salary", "job_highlights", "job_required_skills"]
    for field in fields_to_check:
        if job.get(field):
            score += 1
    return score

def process_single_job_with_retry(job):
    """
    Sends the job data to Gemini 2.5 Flash for data enrichment.
    Maintains a 4.5s delay to stay entirely inside the 15 RPM Free Tier.
    """
    # Hard throttling shield for Gemini Free Tier (Max 15 Requests Per Minute)
    time.sleep(4.5)
    
    raw_description = job.get("job_description", "")
    if not raw_description:
        return None, "fallback_raw"

    prompt = f"""
    You are a data analyst assistant. Analyze the following job posting description and extract the data fields exactly in JSON format.
    Fields to extract:
    - job_title (string)
    - company (string)
    - city (string)
    - state (string)
    - tools (list of strings, e.g., ['Python', 'SQL', 'Power BI'])
    - hard_skills (list of strings, e.g., ['Data Modeling', 'ETL'])
    - salary_min (float or null)
    - salary_max (float or null)

    Job Description:
    {{raw_description}}
    """

    try:
        response = ai_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
            config=types.GenerateContentConfig(
                response_mime_type="application/json"
            )
        )
        
        # Parse output data directly from the structured schema JSON
        structured_data = json.loads(response.text)
        
        # Inject tracking metadata from original payload
        structured_data["job_id"] = job.get("job_id")
        structured_data["date_retrieved"] = time.strftime("%Y-%m-%d")
        structured_data["job_apply_link"] = job.get("job_apply_link")
        
        return structured_data, "enriched"
        
    except Exception as gemini_err:
        if "429" in str(gemini_err) or "Quota" in str(gemini_err):
            log.warning("Gemini API Free Rate Limit spiked. Shifting row into raw fallback state.")
            return None, "exhausted_raw"
        
        log.warning(f"Failed to enrich job {{job.get('job_id')}}: {{gemini_err}}")
        return None, "fallback_raw"

def main():
    start_time = time.time()
    
    # BUDGET TRACKS: Exactly 5 targeted tracks to maximize our free RapidAPI tier metrics
    queries = [
        "Data Analyst or Product Analyst in USA",
        "Business Intelligence Analyst in USA",
        "Data Scientist in USA",
        "Marketing Analyst or Finance Analyst in USA",
        "Operations Analyst or Healthcare Analyst in USA"
    ]
    
    headers = {
        "x-rapidapi-key": os.getenv("RAPIDAPI_KEY"),
        "x-rapidapi-host": "jsearch.p.rapidapi.com"
    }

    deduplicated_jobs = {}
    metrics = {"harvested": 0, "processed": 0, "enriched": 0, "fallback_raw": 0, "exhausted_raw": 0, "loaded": 0}

    log.info("Launching production-grade fortified weekly cloud harvest pipeline...")

    with httpx.Client() as client:
        for q in queries:
            # REAL PAGINATION LOOP: Iterates from page 1 to page 5 sequentially (25 total API hits)
            for page_num in range(1, 6):
                try:
                    params = {
                        "query": q, 
                        "page": str(page_num), 
                        "date_posted": "week", 
                        "country": "us", 
                        "remote_jobs_only": "false"
                    }
                    response = client.get("https://jsearch.p.rapidapi.com/search", headers=headers, params=params, timeout=30.0)
                    
                    if response.status_code == 200:
                        job_data_list = response.json().get("data", [])
                        
                        # Break early if a deep pagination tier naturally runs completely empty
                        if not job_data_list:
                            break
                            
                        for job in job_data_list:
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
                    
                    # Prevent rapid hammering of JSearch page tiers
                    time.sleep(1.2)
                    
                except Exception as e:
                    log.warning(f"API download path anomaly on track '{{q}}' Page {{page_num}}: {{e}}")
                    # Escape current track if timeouts or server exceptions interrupt connection
                    break

    job_list = list(deduplicated_jobs.values())
    total_to_process = len(job_list)
    log.info(f"Deduplication step completed. Processing {{total_to_process}} unique items sequentially...")

    buffer = []
    consecutive_exhaustions = 0
    
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

        # Safety loop breakout if Gemini tier errors repeatedly cascade
        if consecutive_exhaustions >= 3:
            log.critical("Consecutive rate limits exhausted. Aborting loop processing early to safeguard data logs.")
            break

    # Bulk load chunking strategy to cleanly ingest rows into BigQuery tables
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
                log.error(f"BigQuery validation rejection on batch window {{i}}-{{i+chunk_size}}: {{bq_err}}")

    elapsed = int(time.time() - start_time)
    summary_string = f"METRICS_SUMMARY: harvested={{metrics['harvested']}}, unique={{total_to_process}}, enriched={{metrics['enriched']}}, fallback={{metrics['fallback_raw']}}, exhausted={{metrics['exhausted_raw']}}, loaded={{metrics['loaded']}}, runtime_seconds={{elapsed}}"
    print(summary_string)
    log.info(summary_string)

if __name__ == "__main__":
    main()
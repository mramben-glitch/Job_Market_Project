import os
import json
import time
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from google import genai
from google.genai import types

load_dotenv()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID", "my-first-project-27273")
BQ_DATASET = os.getenv("BQ_DATASET", "data_job_market")
BQ_TABLE = os.getenv("BQ_TABLE", "us_job_data")
CREDENTIALS_PATH = "credentials.json"
LOCAL_BACKUP_FILE = "enriched_master_list.json"

ai_client = genai.Client(api_key=GEMINI_API_KEY)
credentials = service_account.Credentials.from_service_account_file(
    CREDENTIALS_PATH, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)
table_id = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

def execute_local_enrichment():
    # STEP 1: Download the data if we haven't already
    if not os.path.exists(LOCAL_BACKUP_FILE):
        print("📥 Downloading current BigQuery table to local backup...")
        query = f"SELECT * FROM `{table_id}`"
        results = bq_client.query(query).result()
        
        jobs = [dict(row) for row in results]
        
        for job in jobs:
            if job.get('date_retrieved'): job['date_retrieved'] = str(job['date_retrieved'])
            if job.get('date_posted'): job['date_posted'] = str(job['date_posted'])

        with open(LOCAL_BACKUP_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=4)
        print(f"✅ Downloaded {len(jobs)} jobs to {LOCAL_BACKUP_FILE}")

    # STEP 2: Load local data and find jobs that need enrichment
    with open(LOCAL_BACKUP_FILE, "r", encoding="utf-8") as f:
        jobs = json.load(f)

    jobs_to_enrich = [
        job for job in jobs 
        if job.get("description") and (not job.get("skills") or not job.get("soft_skills"))
    ]

    if not jobs_to_enrich:
        print("🎉 All jobs are enriched! Initiating BigQuery Overwrite...")
        upload_to_bigquery(jobs)
        return

    print(f"🔍 Found {len(jobs_to_enrich)} jobs needing AI enrichment. Starting marathon run...")

    # STEP 3: Enrich with Gemini (Continuous Loop)
    for job in jobs_to_enrich:
        prompt = f"""
        You are an expert technical recruiter. Extract data from this job description into a strict JSON object with exactly three keys:
        "skills": [hard technical skills like SQL, Python, Tableau],
        "soft_skills": [soft skills like Leadership, Communication],
        "benefits": [benefits like 401k, Remote, Health Insurance].
        If none are found, return an empty array [].
        Job Description: {job['description']}
        """
        try:
            response = ai_client.models.generate_content(
                model='gemini-3.5-flash',
                contents=prompt,
                config=types.GenerateContentConfig(response_mime_type="application/json")
            )
            extracted_data = json.loads(response.text)

            # Update the local dictionary
            job['skills'] = ", ".join(extracted_data.get("skills", [])) or None
            job['soft_skills'] = ", ".join(extracted_data.get("soft_skills", [])) or None
            job['benefits'] = ", ".join(extracted_data.get("benefits", [])) or None

            # Save progress to hard drive instantly
            with open(LOCAL_BACKUP_FILE, "w", encoding="utf-8") as f:
                json.dump(jobs, f, indent=4)

            print(f"✅ Enriched: {job.get('job_title')} | Skills: {job['skills']}")
            
        except Exception as e:
            print(f"❌ Gemini Error: {e}")

        # Sleep to respect free tier
        time.sleep(4.1) 

    # STEP 4: When the loop finally finishes all jobs, push to the cloud
    print("🌟 Marathon complete! Preparing final cloud upload...")
    upload_to_bigquery(jobs)

def upload_to_bigquery(local_jobs_data):
    print("🚀 Overwriting BigQuery table with enriched data...")
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )
    
    load_job = bq_client.load_table_from_json(local_jobs_data, table_id, job_config=job_config)
    load_job.result()
    print("✅ BigQuery Table successfully overwritten with enriched data!")

if __name__ == "__main__":
    execute_local_enrichment()
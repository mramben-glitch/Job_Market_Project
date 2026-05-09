# Production Job Market Pipeline - Deployment Guide

## Files Provided

1. **main.py** - Production-ready pipeline with:
   - Strict inclusion filter for 8 target roles
   - Pagination (5-10 pages per role per API)
   - Smart deduplication (checks existing URLs before inserting)
   - Support for GOOGLE_CREDENTIALS secret from GitHub Actions
   - 1-second sleep between requests to avoid rate-limiting

2. **requirements.txt** - All Python dependencies

3. **.github/workflows/job_sync.yml** - GitHub Actions workflow that:
   - Runs every 6 hours (0 */6 * * *)
   - Uses GitHub Secrets for all credentials
   - Cleans up credentials after execution

## GitHub Secrets to Configure

Add the following secrets to your GitHub repository settings (Settings → Secrets and variables → Actions):

### API Keys
- **ADZUNA_APP_ID** - Your Adzuna App ID
- **ADZUNA_APP_KEY** - Your Adzuna App Key
- **JOOBLE_API_KEY** - Your Jooble API Key (format: xxxx-xxxx-xxxx)
- **USAJOBS_API_KEY** - Your USA Jobs API Key
- **USAJOBS_EMAIL** - Your email for USA Jobs User-Agent header
- **THE_MUSE_API_KEY** - Your Muse API Key (if needed)
- **REMOTIVE_API_KEY** - Your Remotive API Key (if needed)
- **ARBEITNOW_API_KEY** - Your Arbeitnow API Key (if needed)

### BigQuery Credentials
- **GOOGLE_CREDENTIALS** - Your service account JSON as a single-line string

To convert your `credentials.json` to a single-line string for the secret:
```powershell
# Windows PowerShell
(Get-Content credentials.json -Raw) -replace '\s+', ' ' | Set-Clipboard
```

Then paste it into the GitHub Secret.

## Target Roles (Strict Inclusion Filter)

The pipeline ONLY fetches and inserts jobs that match these 8 roles (case-insensitive):
1. Data Analyst
2. Product Analyst
3. Marketing Analyst
4. Business Analyst
5. Business Intelligence Analyst
6. Financial Analyst
7. Operation Analyst
8. Data Scientist

Any job title not containing one of these strings is automatically filtered out.

## Pipeline Features

### Strict Inclusion Filter
- Checks if job_title contains any target role (case-insensitive)
- Immediately discards non-matching roles
- Zero noise, 100% relevancy

### Pagination & Throughput
- Fetches 5-10 pages per role per API
- Sets results_per_page to 100 (maximum)
- Total potential: ~50-60 jobs per role per API
- For 8 roles × 6 APIs = up to 2,400-2,880 jobs per run

### Smart Deduplication
- Queries existing `job_url` values from BigQuery at start
- Checks each fetched URL before inserting
- Updates dedup set in-memory during run
- Only appends new URLs to BigQuery

### Schema
**BigQuery Table: `my-first-project-27273.data_job_market.us_job_data`**

| Column | Type | Notes |
|--------|------|-------|
| job_title | STRING | Filtered to target roles |
| company | STRING | Extracted from API response |
| location | STRING | Raw location value |
| description | STRING | HTML-stripped only |
| salary_raw | STRING | JSON-serialized salary data |
| job_url | STRING | Unique identifier for deduplication |
| date_retrieved | TIMESTAMP | UTC timestamp of fetch |
| industry | STRING | NULL (reserved for later cleaning) |
| skills | STRING | NULL (reserved for later cleaning) |
| education | STRING | NULL (reserved for later cleaning) |
| benefits | STRING | NULL (reserved for later cleaning) |
| remote_status | STRING | NULL (reserved for later cleaning) |
| date_posted | DATE | Parsed to YYYY-MM-DD or NULL |

### Rate-Limiting
- 1-second sleep between paginated requests to each API
- Prevents throttling and 429 errors

## Deployment Steps

1. **Push files to GitHub:**
   ```bash
   git add main.py requirements.txt .github/workflows/job_sync.yml
   git commit -m "Add production job market pipeline"
   git push origin main
   ```

2. **Add GitHub Secrets:**
   - Go to your repository → Settings → Secrets and variables → Actions
   - Click "New repository secret" for each of the 9 secrets listed above

3. **Verify workflow:**
   - Go to Actions tab
   - Manual trigger with "Run workflow" button (the workflow_dispatch trigger)
   - Check logs to confirm pipeline runs successfully

4. **Monitor scheduled runs:**
   - The workflow will automatically run every 6 hours (0:00 UTC, 6:00 UTC, 12:00 UTC, 18:00 UTC)

## Local Testing (Before Deployment)

```bash
# Install dependencies
pip install -r requirements.txt

# Create .env with your credentials
# (Copy from GitHub Secrets or use local credentials.json)

# Test the pipeline
python main.py
```

## Troubleshooting

- **400/403 from Jooble:** The API returns an error for some keywords; the pipeline gracefully continues to other sources
- **Duplicate inserts:** BigQuery `WRITE_APPEND` mode handles duplicates; dedup logic prevents them at the script level
- **Missing date_posted:** Automatically converted to NULL for BigQuery DATE columns
- **Rate limiting:** 1-second sleep prevents most throttling; increase if needed

## Next Steps in Power BI/SQL

Your data will land in BigQuery with these columns raw and unprocessed:
- Use SQL to standardize locations (city normalization)
- Extract skills from descriptions
- Determine remote status (keyword search in description/location)
- Normalize job titles and clean salary_raw
- The 8-role filter ensures you only clean job market data, not medical/retail noise

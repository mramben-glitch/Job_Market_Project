"""
================================================================================
 ULTIMATE US JOB DATA ETL PIPELINE  —  v1.1
================================================================================
 Changelog v1.1:
   • Fix: USAJobs MajorDuties now safely handles str / list / dict / nested
   • Fix: Arbeitnow guard against malformed (non-dict) JSON items
   • Fix: GeoValidator now whitelists 'worldwide / anywhere / global'
   • New: Universal SalaryParser (hourly→annual, k/m suffix, multi-currency veto)
   • Fix: Loosened regex word boundaries (\w) with full IGNORECASE
   • New: Dual-pronged skills/benefits extraction (regex + API tags merge)
================================================================================
"""

import os
import re
import json
import hashlib
import logging
import time
from datetime import datetime, timezone
from typing import Optional, Iterable
from dataclasses import dataclass, field, asdict
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import cloudscraper
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from ratelimit import limits, sleep_and_retry
from google.cloud import bigquery
from google.oauth2 import service_account
from tqdm import tqdm

# ============================================================================
# 1. ENVIRONMENT & LOGGING
# ============================================================================

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-7s | %(name)-20s | %(message)s',
    handlers=[logging.FileHandler('etl_pipeline.log'), logging.StreamHandler()]
)
log = logging.getLogger('job_etl')

# ============================================================================
# 2. CONFIGURATION
# ============================================================================

CREDENTIALS_PATH = "credentials.json"
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET", "jobs_data")
BQ_TABLE = os.getenv("BQ_TABLE", "us_analyst_jobs")

ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")
JOOBLE_API_KEY = os.getenv("JOOBLE_API_KEY")
USAJOBS_API_KEY = os.getenv("USAJOBS_API_KEY")
USAJOBS_EMAIL = os.getenv("USAJOBS_EMAIL")

TARGET_ROLES = [
    "Data Analyst", "Product Analyst", "Marketing Analyst", "Business Analyst",
    "Business Intelligence Analyst", "Financial Analyst", "Operation Analyst",
    "Data Scientist",
]

RESULTS_PER_ROLE_PER_API = 1000

# ---- US Geographic Authority ------------------------------------------------
US_STATES = {
    'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
    'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
    'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
    'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
    'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
    'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
    'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
    'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
    'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
    'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
    'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
    'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
    'WI': 'Wisconsin', 'WY': 'Wyoming', 'DC': 'District of Columbia',
    'PR': 'Puerto Rico'
}
STATE_NAME_TO_CODE = {v.lower(): k for k, v in US_STATES.items()}
VALID_STATE_CODES = set(US_STATES.keys())

US_CITIES = {
    "new york": "NY", "nyc": "NY", "manhattan": "NY", "brooklyn": "NY", "queens": "NY",
    "buffalo": "NY", "rochester": "NY", "albany": "NY", "syracuse": "NY",
    "los angeles": "CA", "la": "CA", "san francisco": "CA", "sf": "CA",
    "san diego": "CA", "san jose": "CA", "sacramento": "CA", "oakland": "CA",
    "fresno": "CA", "long beach": "CA", "anaheim": "CA", "santa monica": "CA",
    "palo alto": "CA", "mountain view": "CA", "sunnyvale": "CA", "berkeley": "CA",
    "chicago": "IL", "naperville": "IL", "aurora": "IL", "rockford": "IL",
    "houston": "TX", "dallas": "TX", "austin": "TX", "san antonio": "TX",
    "fort worth": "TX", "el paso": "TX", "arlington": "TX", "plano": "TX",
    "phoenix": "AZ", "tucson": "AZ", "mesa": "AZ", "scottsdale": "AZ",
    "philadelphia": "PA", "pittsburgh": "PA", "harrisburg": "PA",
    "jacksonville": "FL", "miami": "FL", "tampa": "FL", "orlando": "FL",
    "fort lauderdale": "FL", "st petersburg": "FL", "tallahassee": "FL",
    "columbus": "OH", "cleveland": "OH", "cincinnati": "OH", "toledo": "OH",
    "indianapolis": "IN", "fort wayne": "IN",
    "charlotte": "NC", "raleigh": "NC", "durham": "NC", "greensboro": "NC",
    "seattle": "WA", "spokane": "WA", "tacoma": "WA", "bellevue": "WA", "redmond": "WA",
    "denver": "CO", "colorado springs": "CO", "boulder": "CO",
    "washington": "DC", "dc": "DC",
    "boston": "MA", "cambridge": "MA", "worcester": "MA",
    "nashville": "TN", "memphis": "TN", "knoxville": "TN", "chattanooga": "TN",
    "detroit": "MI", "grand rapids": "MI", "ann arbor": "MI",
    "portland": "OR", "eugene": "OR", "salem": "OR",
    "las vegas": "NV", "reno": "NV", "henderson": "NV",
    "atlanta": "GA", "savannah": "GA", "augusta": "GA",
    "minneapolis": "MN", "st paul": "MN", "saint paul": "MN",
    "milwaukee": "WI", "madison": "WI",
    "baltimore": "MD", "annapolis": "MD", "bethesda": "MD", "silver spring": "MD",
    "albuquerque": "NM", "santa fe": "NM",
    "tulsa": "OK", "oklahoma city": "OK",
    "louisville": "KY", "lexington": "KY",
    "new orleans": "LA", "baton rouge": "LA",
    "honolulu": "HI", "anchorage": "AK",
    "richmond": "VA", "virginia beach": "VA", "norfolk": "VA",
    "alexandria": "VA", "reston": "VA", "mclean": "VA",
    "salt lake city": "UT", "provo": "UT",
    "kansas city": "MO", "st louis": "MO", "saint louis": "MO", "springfield": "MO",
    "omaha": "NE", "lincoln": "NE",
    "des moines": "IA", "cedar rapids": "IA",
    "wichita": "KS", "topeka": "KS",
    "boise": "ID",
    "billings": "MT", "missoula": "MT",
    "fargo": "ND", "bismarck": "ND",
    "sioux falls": "SD", "rapid city": "SD",
    "burlington": "VT",
    "manchester": "NH", "concord": "NH",
    "portland": "ME",
    "providence": "RI",
    "hartford": "CT", "new haven": "CT", "stamford": "CT",
    "newark": "NJ", "jersey city": "NJ", "trenton": "NJ", "princeton": "NJ",
    "wilmington": "DE", "dover": "DE",
    "charleston": "SC", "columbia": "SC",
    "birmingham": "AL", "montgomery": "AL", "huntsville": "AL", "mobile": "AL",
    "jackson": "MS", "little rock": "AR",
    "morgantown": "WV",
    "cheyenne": "WY",
    "san juan": "PR",
}

NON_US_VETO_KEYWORDS = {
    'germany', 'deutschland', 'united kingdom', 'great britain', 'england',
    'scotland', 'wales', 'ireland', 'france', 'spain', 'italy', 'netherlands',
    'belgium', 'austria', 'switzerland', 'sweden', 'norway', 'denmark', 'finland',
    'poland', 'czech', 'portugal', 'greece', 'turkey', 'russia', 'ukraine',
    'china', 'japan', 'india', 'singapore', 'australia', 'new zealand', 'canada',
    'mexico', 'brazil', 'argentina', 'chile', 'colombia', 'south africa',
    'israel', 'uae', 'dubai', 'saudi arabia', 'philippines', 'vietnam', 'thailand',
    'indonesia', 'malaysia', 'south korea', 'taiwan', 'hong kong',
    'london', 'paris', 'berlin', 'munich', 'frankfurt', 'hamburg', 'cologne',
    'rostock', 'stuttgart', 'düsseldorf', 'dusseldorf', 'leipzig', 'dresden',
    'madrid', 'barcelona', 'rome', 'milan', 'amsterdam', 'rotterdam', 'brussels',
    'vienna', 'zurich', 'geneva', 'stockholm', 'oslo', 'copenhagen', 'helsinki',
    'warsaw', 'prague', 'lisbon', 'athens', 'istanbul', 'moscow', 'kyiv',
    'tokyo', 'osaka', 'beijing', 'shanghai', 'mumbai', 'delhi', 'bangalore',
    'bengaluru', 'hyderabad', 'sydney', 'melbourne', 'toronto', 'vancouver',
    'montreal', 'mexico city', 'são paulo', 'sao paulo', 'rio', 'buenos aires',
    'gmbh', 'sarl', 'b.v.', 'bv ', 'eur ', '€', '£', 'gbp', 'inr', 'rupees',
    'yen', '¥', 'rmb', 'bundesland', 'kreis',
}

# NEW v1.1: country aliases that should NOT trigger Layer-2 rejection
REMOTE_ELIGIBLE_ALIASES = {'worldwide', 'anywhere', 'global', 'remote', ''}
US_COUNTRY_ALIASES = {'us', 'usa', 'u.s.', 'u.s.a.', 'united states',
                      'united states of america', 'america'}

# ---- Taxonomies -------------------------------------------------------------
SKILLS_TAXONOMY = [
    'python', 'r', 'sql', 'java', 'scala', 'javascript', 'typescript', 'c++', 'c#',
    'julia', 'go', 'rust', 'matlab', 'sas', 'stata', 'spss', 'vba',
    'pandas', 'numpy', 'scikit-learn', 'sklearn', 'tensorflow', 'pytorch', 'keras',
    'spark', 'pyspark', 'hadoop', 'kafka', 'airflow', 'dbt', 'snowflake', 'databricks',
    'redshift', 'bigquery', 'synapse', 'fivetran', 'stitch', 'segment',
    'postgresql', 'postgres', 'mysql', 'mssql', 'sql server', 'oracle', 'mongodb',
    'cassandra', 'redis', 'dynamodb', 'cosmos db', 'elasticsearch',
    'tableau', 'power bi', 'powerbi', 'looker', 'looker studio', 'qlik', 'qlikview',
    'qliksense', 'mode analytics', 'sisense', 'domo', 'metabase', 'superset',
    'thoughtspot', 'sigma', 'hex', 'd3.js', 'plotly', 'matplotlib', 'seaborn',
    'ggplot', 'highcharts', 'google data studio',
    'aws', 'amazon web services', 'azure', 'gcp', 'google cloud', 's3', 'ec2',
    'lambda', 'glue', 'athena', 'sagemaker', 'vertex ai', 'cloud functions',
    'cloud run', 'kubernetes', 'docker', 'terraform',
    'machine learning', 'deep learning', 'nlp', 'computer vision', 'time series',
    'regression', 'classification', 'clustering', 'a/b testing', 'ab testing',
    'hypothesis testing', 'statistical modeling', 'bayesian', 'forecasting',
    'optimization', 'reinforcement learning', 'mlops',
    'excel', 'powerpoint', 'word', 'google sheets', 'financial modeling',
    'budgeting', 'variance analysis', 'p&l', 'pnl', 'kpi',
    'okr', 'salesforce', 'hubspot', 'sap', 'oracle erp', 'netsuite', 'workday',
    'quickbooks', 'jira', 'confluence', 'asana', 'trello', 'notion',
    'google analytics', 'ga4', 'google ads', 'facebook ads', 'meta ads',
    'adobe analytics', 'mixpanel', 'amplitude', 'heap', 'pendo',
    'marketo', 'pardot', 'mailchimp', 'klaviyo', 'attribution modeling',
    'seo', 'sem', 'ppc',
    'agile', 'scrum', 'kanban', 'waterfall', 'lean', 'six sigma', 'design thinking',
    'product analytics', 'cohort analysis', 'funnel analysis', 'churn analysis',
    'ltv', 'cac', 'mrr', 'arr',
    'rest api', 'graphql', 'json', 'xml', 'git', 'github', 'gitlab', 'bitbucket',
    'linux', 'bash', 'shell scripting', 'etl', 'elt', 'data warehousing',
    'data modeling', 'dimensional modeling', 'star schema', 'snowflake schema',
]

SOFT_SKILLS_TAXONOMY = [
    'communication', 'leadership', 'teamwork', 'collaboration', 'problem solving',
    'problem-solving', 'critical thinking', 'analytical thinking', 'attention to detail',
    'time management', 'project management', 'stakeholder management', 'presentation',
    'storytelling', 'data storytelling', 'mentoring', 'cross-functional',
    'self-starter', 'self-motivated', 'proactive', 'adaptability', 'creativity',
    'negotiation', 'decision making', 'decision-making', 'strategic thinking',
    'business acumen', 'curiosity', 'ownership', 'accountability',
]

BENEFITS_PATTERNS = {
    'Health Insurance': r'(health\s*insurance|medical\s*(insurance|coverage|benefits)|dental|vision)',
    '401(k)': r'(401\s*\(?k\)?|retirement\s*(plan|savings)|roth\s*ira)',
    'Paid Time Off': r'(pto|paid\s*time\s*off|vacation\s*days|paid\s*vacation|unlimited\s*pto)',
    'Parental Leave': r'(parental\s*leave|maternity\s*leave|paternity\s*leave|family\s*leave)',
    'Equity/Stock': r'(equity|stock\s*options|rsu|restricted\s*stock|esop|stock\s*purchase)',
    'Bonus': r'(annual\s*bonus|performance\s*bonus|signing\s*bonus|bonus\s*program)',
    'Remote Stipend': r'(home\s*office\s*stipend|remote\s*stipend|wfh\s*stipend|wellness\s*stipend)',
    'Learning Budget': r'(learning\s*budget|education\s*reimbursement|tuition\s*(reimbursement|assistance)|professional\s*development)',
    'Gym/Wellness': r'(gym\s*membership|wellness\s*program|fitness\s*reimbursement|mental\s*health)',
    'Flexible Hours': r'(flexible\s*(hours|schedule)|flex\s*time|work-life\s*balance)',
    'FSA/HSA': r'(fsa|hsa|flexible\s*spending|health\s*savings)',
    'Life Insurance': r'(life\s*insurance|disability\s*insurance|ad&d)',
    'Commuter Benefits': r'(commuter\s*benefits|transit\s*benefits|parking\s*reimbursement)',
    'Free Meals': r'(free\s*(lunch|meals|food|snacks)|catered\s*meals)',
    'Pet-Friendly': r'(pet\s*friendly|dog\s*friendly|pet\s*insurance)',
}

# Map common API tag strings → canonical benefit names
BENEFIT_TAG_ALIASES = {
    'health': 'Health Insurance', 'medical': 'Health Insurance',
    'dental': 'Health Insurance', 'vision': 'Health Insurance',
    '401k': '401(k)', '401(k)': '401(k)', 'retirement': '401(k)',
    'pto': 'Paid Time Off', 'unlimited pto': 'Paid Time Off',
    'vacation': 'Paid Time Off', 'paid time off': 'Paid Time Off',
    'parental leave': 'Parental Leave', 'maternity': 'Parental Leave',
    'paternity': 'Parental Leave',
    'equity': 'Equity/Stock', 'stock options': 'Equity/Stock', 'rsu': 'Equity/Stock',
    'bonus': 'Bonus', 'signing bonus': 'Bonus',
    'remote stipend': 'Remote Stipend', 'wfh stipend': 'Remote Stipend',
    'learning budget': 'Learning Budget', 'tuition reimbursement': 'Learning Budget',
    'professional development': 'Learning Budget',
    'gym': 'Gym/Wellness', 'wellness': 'Gym/Wellness',
    'flexible hours': 'Flexible Hours', 'flex time': 'Flexible Hours',
    'work-life balance': 'Flexible Hours',
    'fsa': 'FSA/HSA', 'hsa': 'FSA/HSA',
    'life insurance': 'Life Insurance', 'disability': 'Life Insurance',
    'commuter': 'Commuter Benefits',
    'free lunch': 'Free Meals', 'meals': 'Free Meals',
    'pet friendly': 'Pet-Friendly', 'pet-friendly': 'Pet-Friendly',
}

INDUSTRY_KEYWORDS = {
    'Technology / SaaS': ['saas', 'software', 'tech company', 'platform', 'b2b software', 'cloud computing', 'developer tools'],
    'Finance / Banking': ['bank', 'banking', 'investment', 'hedge fund', 'private equity', 'asset management', 'wealth management', 'capital markets'],
    'Fintech': ['fintech', 'payments', 'lending', 'cryptocurrency', 'crypto', 'blockchain', 'neobank'],
    'Healthcare': ['healthcare', 'hospital', 'clinic', 'pharma', 'pharmaceutical', 'biotech', 'medical device', 'health system'],
    'Insurance': ['insurance', 'insurtech', 'underwriting', 'claims', 'actuarial'],
    'E-commerce / Retail': ['ecommerce', 'e-commerce', 'retail', 'marketplace', 'consumer goods', 'cpg', 'dtc'],
    'Media / Entertainment': ['media', 'entertainment', 'streaming', 'gaming', 'publishing', 'film', 'television'],
    'Consulting': ['consulting', 'advisory', 'professional services', 'big four', 'big 4'],
    'Government / Public Sector': ['government', 'federal', 'state agency', 'public sector', 'department of', 'dod', 'dhs', 'va '],
    'Education': ['edtech', 'university', 'college', 'school district', 'higher education', 'k-12'],
    'Manufacturing': ['manufacturing', 'industrial', 'automotive', 'aerospace', 'semiconductor'],
    'Energy / Utilities': ['energy', 'oil and gas', 'renewable', 'utilities', 'solar', 'wind power'],
    'Real Estate / Proptech': ['real estate', 'proptech', 'reit', 'property management'],
    'Logistics / Supply Chain': ['logistics', 'supply chain', 'shipping', 'freight', 'warehousing', '3pl'],
    'Telecommunications': ['telecom', 'telecommunications', '5g', 'wireless carrier'],
    'Non-Profit': ['non-profit', 'nonprofit', 'ngo', '501(c)'],
    'Travel / Hospitality': ['travel', 'hospitality', 'airline', 'hotel', 'tourism'],
    'Marketing / Advertising': ['advertising agency', 'marketing agency', 'ad tech', 'adtech', 'martech'],
}

EDUCATION_PATTERNS = [
    (r'\bph\.?d\.?\b|\bdoctorate\b', 'PhD'),
    (r'\bm\.?b\.?a\.?\b', 'MBA'),
    (r"\bmaster'?s?\b|\bm\.?s\.?\b|\bm\.?a\.?\b|\bgraduate degree\b", "Master's"),
    (r"\bbachelor'?s?\b|\bb\.?s\.?\b|\bb\.?a\.?\b|\bundergraduate degree\b", "Bachelor's"),
    (r"\bassociate'?s?\b degree\b|\ba\.?a\.?\b|\ba\.?s\.?\b", "Associate's"),
    (r'\bhigh school\b|\bged\b|\bdiploma\b', 'High School'),
]

# ============================================================================
# 3. UTILITIES — Safe text coercion (used everywhere)
# ============================================================================

def coerce_text(value) -> str:
    """
    NEW v1.1: Universal coercion to handle APIs returning str / list / dict / None.
    Used heavily for USAJobs (MajorDuties, Benefits, etc.) and Arbeitnow tags.
    """
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, (int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return " ".join(coerce_text(v) for v in value.values() if v)
    if isinstance(value, list):
        return " ".join(coerce_text(v) for v in value if v)
    return str(value)

def coerce_list(value) -> list:
    """Coerce API field to a list of clean strings."""
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, dict):
        return [coerce_text(v) for v in value.values() if v]
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(item.strip())
            elif isinstance(item, dict):
                out.append(coerce_text(item))
            elif item is not None:
                out.append(str(item))
        return out
    return [str(value)]

# ============================================================================
# 4. SALARY PARSER (NEW v1.1)
# ============================================================================

class SalaryParser:
    HOURLY_HINTS = ('/hr', 'per hour', 'hourly', '/hour', 'an hour', 'p/h', '/h ', ' hr')
    MONTHLY_HINTS = ('/month', 'per month', 'monthly', '/mo')
    ANNUAL_MULT_HOURLY = 2080  # 40h * 52w

    @classmethod
    def parse(cls, raw, reject_non_usd=True) -> tuple:
        """
        Returns (salary_min, salary_max) annualized in USD.
        Handles:
          '$80k - $120k'         → (80000, 120000)
          '$40 - $50 per hour'   → (83200, 104000)
          '83200'                → (83200, 83200)
          '80,000 - 120,000 USD' → (80000, 120000)
          40.0 (number)          → (40, 40)  # caller must indicate hourly via separate path
        """
        if raw is None:
            return None, None

        # Numeric input — already a clean annual figure
        if isinstance(raw, (int, float)):
            n = float(raw)
            if 10000 <= n <= 1_000_000:
                return n, n
            return None, None

        s = str(raw).strip()
        if not s:
            return None, None

        s_low = s.lower()

        # Reject non-USD if explicitly other currency
        if reject_non_usd and re.search(r'\b(eur|gbp|cad|aud|inr|jpy|cny|rub)\b|[€£¥]', s_low):
            return None, None

        is_hourly = any(h in s_low for h in cls.HOURLY_HINTS)
        is_monthly = any(h in s_low for h in cls.MONTHLY_HINTS)

        # Extract numeric tokens with optional k/m suffix
        tokens = re.findall(r'(\d[\d,]*\.?\d*)\s*([kKmM]?)', s)
        values = []
        for num_str, suffix in tokens:
            num_str = num_str.replace(',', '')
            if not num_str:
                continue
            try:
                n = float(num_str)
            except ValueError:
                continue
            if suffix.lower() == 'k':
                n *= 1000
            elif suffix.lower() == 'm':
                n *= 1_000_000
            values.append(n)

        # Drop tokens that look like years
        values = [v for v in values if not (1900 <= v <= 2100 and v == int(v))]
        if not values:
            return None, None

        if is_hourly:
            values = [v * cls.ANNUAL_MULT_HOURLY for v in values]
        elif is_monthly:
            values = [v * 12 for v in values]

        # Filter implausible salaries
        values = [v for v in values if 10_000 <= v <= 1_000_000]
        if not values:
            return None, None

        return float(min(values)), float(max(values))

    @classmethod
    def parse_pair(cls, min_raw, max_raw, unit_hint: str = "") -> tuple:
        """
        Used by Adzuna/USAJobs that give numeric min/max separately,
        sometimes with a unit hint like 'Per Hour'.
        """
        unit = (unit_hint or "").lower()
        try:
            mn = float(min_raw) if min_raw is not None else None
            mx = float(max_raw) if max_raw is not None else None
        except (TypeError, ValueError):
            return None, None

        if mn is None and mx is None:
            return None, None
        if mn is None:
            mn = mx
        if mx is None:
            mx = mn

        # Apply hourly conversion if value looks hourly (low magnitude) or unit says so
        looks_hourly = (mx and mx < 500) or 'hour' in unit or 'hr' in unit
        if looks_hourly:
            mn *= cls.ANNUAL_MULT_HOURLY
            mx *= cls.ANNUAL_MULT_HOURLY
        elif 'month' in unit:
            mn *= 12
            mx *= 12

        if not (10_000 <= mx <= 1_000_000):
            return None, None
        return float(mn), float(mx)

# ============================================================================
# 5. DATA MODEL
# ============================================================================

@dataclass
class JobRecord:
    job_title: Optional[str] = None
    company: Optional[str] = None
    industry: Optional[str] = None
    location: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    description: Optional[str] = None
    salary_min: Optional[float] = None
    salary_max: Optional[float] = None
    job_url: Optional[str] = None
    skills: Optional[str] = None
    soft_skills: Optional[str] = None
    education: Optional[str] = None
    remote_status: Optional[str] = None
    benefits: Optional[str] = None
    date_retrieved: Optional[str] = None
    date_posted: Optional[str] = None

    _source: str = field(default="", repr=False)
    _raw: dict = field(default_factory=dict, repr=False)

    def fingerprint(self) -> str:
        title = (self.job_title or "").strip().lower()
        company = (self.company or "").strip().lower()
        city = (self.city or "").strip().lower()
        return hashlib.sha256(f"{title}|{company}|{city}".encode()).hexdigest()

    def to_bq_dict(self) -> dict:
        d = asdict(self)
        d.pop('_source', None)
        d.pop('_raw', None)
        return d

    def completeness_score(self) -> int:
        return sum(1 for f in [self.skills, self.benefits, self.industry,
                               self.remote_status, self.description]
                   if f and str(f).strip())

# ============================================================================
# 6. GEOGRAPHIC VALIDATOR (PATCHED v1.1)
# ============================================================================

class GeoValidator:
    @staticmethod
    def _normalize(text: str) -> str:
        return re.sub(r'\s+', ' ', (text or '').lower().strip())

    @classmethod
    def validate(cls, location_str: str, country_hint: str = "",
                 description: str = "") -> tuple:
        loc = cls._normalize(location_str)
        desc = cls._normalize(description)
        country = cls._normalize(country_hint)

        # ---- LAYER 1: Negative keyword veto ------------------------------
        combined_for_veto = f"{loc} {country}"
        for kw in NON_US_VETO_KEYWORDS:
            if len(kw) <= 4:
                if re.search(rf'\b{re.escape(kw)}\b', combined_for_veto):
                    return False, None, None, f"veto:{kw}"
            else:
                if kw in combined_for_veto:
                    return False, None, None, f"veto:{kw}"

        # ---- LAYER 2: Country whitelist (PATCHED v1.1) -------------------
        if country:
            if country in US_COUNTRY_ALIASES:
                pass  # US confirmed
            elif country in REMOTE_ELIGIBLE_ALIASES:
                pass  # remote-eligible — fall through to Layer 3
            elif any(alias in country for alias in US_COUNTRY_ALIASES):
                pass  # e.g. "United States of America" substring
            elif any(alias in country for alias in REMOTE_ELIGIBLE_ALIASES):
                pass  # e.g. "Anywhere in the world"
            else:
                return False, None, None, f"country_not_us:{country}"

        # ---- LAYER 3: Remote-only handling --------------------------------
        is_remote_only = bool(re.search(r'\bremote\b|\banywhere\b|\bworldwide\b|\bglobal\b|\bwork from home\b|\bwfh\b', loc))
        if is_remote_only:
            # Must have explicit US marker somewhere
            has_us_marker = bool(re.search(
                r'\b(us|usa|u\.s\.|united states|america|north america)\b',
                f"{loc} {country} {desc[:500]}"
            ))
            # Also try state extraction inside the remote string (e.g. "Remote, CA")
            state_in_loc = re.search(r',\s*([A-Z]{2})\b', location_str or '')
            if has_us_marker:
                return True, "Remote", "US", "remote_us_verified"
            elif state_in_loc and state_in_loc.group(1) in VALID_STATE_CODES:
                return True, "Remote", state_in_loc.group(1), "remote_us_state"
            else:
                # Country was US-aligned earlier? Trust it.
                if country in US_COUNTRY_ALIASES or any(a in country for a in US_COUNTRY_ALIASES):
                    return True, "Remote", "US", "remote_country_us"
                return False, None, None, "remote_no_us_marker"

        # ---- LAYER 4: State extraction ------------------------------------
        state_code = None
        city = None

        m = re.search(r',\s*([A-Z]{2})\b', location_str or '')
        if m and m.group(1) in VALID_STATE_CODES:
            state_code = m.group(1)
            city_part = location_str.split(',')[0].strip()
            city = city_part if city_part else None

        if not state_code:
            for state_name, code in STATE_NAME_TO_CODE.items():
                if re.search(rf'\b{re.escape(state_name)}\b', loc):
                    state_code = code
                    parts = re.split(rf',?\s*{re.escape(state_name)}', loc, maxsplit=1)
                    if parts and parts[0].strip():
                        city = parts[0].strip().title()
                    break

        if not state_code:
            for us_city, code in US_CITIES.items():
                if re.search(rf'\b{re.escape(us_city)}\b', loc):
                    state_code = code
                    city = us_city.title()
                    break

        if not state_code:
            return False, None, None, "no_us_state_identified"

        if state_code not in VALID_STATE_CODES:
            return False, None, None, f"invalid_state:{state_code}"

        if city:
            city = re.sub(r'\s+', ' ', city).strip().title()
            if re.search(r'\d', city) or not city.replace(' ', '').replace('-', '').replace('.', '').isascii():
                city = None

        return True, city, state_code, "ok"

# ============================================================================
# 7. ENRICHMENT ENGINE (PATCHED v1.1)
# ============================================================================

class Enrichment:
    # PATCHED v1.1: looser word boundaries with full IGNORECASE
    _skill_patterns = [
        (skill, re.compile(rf'(?<!\w){re.escape(skill)}(?!\w)', re.IGNORECASE))
        for skill in SKILLS_TAXONOMY
    ]
    _soft_skill_patterns = [
        (s, re.compile(rf'(?<!\w){re.escape(s)}(?!\w)', re.IGNORECASE))
        for s in SOFT_SKILLS_TAXONOMY
    ]
    _benefit_patterns = {
        name: re.compile(pat, re.IGNORECASE) for name, pat in BENEFITS_PATTERNS.items()
    }
    _education_patterns = [(re.compile(p, re.IGNORECASE), v) for p, v in EDUCATION_PATTERNS]

    # Canonical names for common variants
    _skill_canonical = {
        'powerbi': 'Power BI', 'power bi': 'Power BI',
        'sklearn': 'scikit-learn', 'scikit-learn': 'scikit-learn',
        'pyspark': 'PySpark', 'spark': 'Apache Spark',
        'gcp': 'Google Cloud', 'google cloud': 'Google Cloud',
        'aws': 'AWS', 'amazon web services': 'AWS',
        'sql server': 'SQL Server', 'mssql': 'SQL Server',
        'postgres': 'PostgreSQL', 'postgresql': 'PostgreSQL',
        'ga4': 'Google Analytics', 'google analytics': 'Google Analytics',
        'ab testing': 'A/B Testing', 'a/b testing': 'A/B Testing',
        'sql': 'SQL', 'r': 'R', 'python': 'Python', 'excel': 'Excel',
        'tableau': 'Tableau', 'looker': 'Looker', 'looker studio': 'Looker Studio',
    }

    @classmethod
    def _canonicalize_skill(cls, raw: str) -> str:
        key = raw.lower().strip()
        if key in cls._skill_canonical:
            return cls._skill_canonical[key]
        if len(raw) <= 4:
            return raw.upper()
        return raw.title()

    @classmethod
    def _normalize_tags(cls, tags) -> list:
        """Coerce API tags into a clean list of strings."""
        out = []
        for t in coerce_list(tags):
            t = re.sub(r'[\[\]{}"\']', '', str(t)).strip()
            if t and 1 < len(t) <= 50:
                out.append(t)
        return out

    @classmethod
    def extract_skills(cls, *texts, api_tags=None) -> str:
        """
        PATCHED v1.1: Dual-pronged extraction.
          Prong A: regex over text blob
          Prong B: clean & merge API-provided tags
        """
        found = set()

        # Prong A — regex
        blob = ' '.join(coerce_text(t) for t in texts if t)
        if blob:
            for skill, pattern in cls._skill_patterns:
                if pattern.search(blob):
                    found.add(cls._canonicalize_skill(skill))

        # Prong B — API tags
        if api_tags:
            tags = cls._normalize_tags(api_tags)
            for tag in tags:
                tag_lower = tag.lower()
                matched = False
                # Try to map tag to taxonomy entry
                for skill in SKILLS_TAXONOMY:
                    if skill == tag_lower or (len(skill) > 3 and skill in tag_lower):
                        found.add(cls._canonicalize_skill(skill))
                        matched = True
                        break
                # Otherwise keep tag verbatim if plausible
                if not matched and 2 <= len(tag) <= 30 and not tag_lower.startswith(('http', 'www')):
                    # Skip generic non-skill tags
                    if tag_lower not in {'full-time', 'part-time', 'contract', 'remote',
                                         'hybrid', 'on-site', 'entry-level', 'mid-level',
                                         'senior', 'junior', 'usa', 'us', 'united states'}:
                        found.add(tag.title() if tag.islower() else tag)

        return ", ".join(sorted(found))

    @classmethod
    def extract_soft_skills(cls, *texts, api_tags=None) -> str:
        found = set()
        blob = ' '.join(coerce_text(t) for t in texts if t)
        if blob:
            for skill, pattern in cls._soft_skill_patterns:
                if pattern.search(blob):
                    found.add(skill.title())

        if api_tags:
            for tag in cls._normalize_tags(api_tags):
                t_low = tag.lower()
                for soft in SOFT_SKILLS_TAXONOMY:
                    if soft in t_low:
                        found.add(soft.title())

        return ", ".join(sorted(found))

    @classmethod
    def extract_benefits(cls, *texts, api_tags=None, structured_benefits=None) -> str:
        """
        PATCHED v1.1: Dual-pronged benefits extraction.
        - texts: description, benefits_text from API
        - api_tags: tags[] array (e.g. arbeitnow, themuse)
        - structured_benefits: explicit benefits field if any
        """
        found = set()
        blob_parts = list(texts)
        if structured_benefits:
            blob_parts.append(coerce_text(structured_benefits))
        blob = ' '.join(coerce_text(t) for t in blob_parts if t)

        # Prong A — regex
        if blob:
            for name, pattern in cls._benefit_patterns.items():
                if pattern.search(blob):
                    found.add(name)

        # Prong B — API tags via alias mapping
        if api_tags:
            for tag in cls._normalize_tags(api_tags):
                t_low = tag.lower().strip()
                if t_low in BENEFIT_TAG_ALIASES:
                    found.add(BENEFIT_TAG_ALIASES[t_low])
                else:
                    # Substring match against alias keys
                    for alias_key, canonical in BENEFIT_TAG_ALIASES.items():
                        if alias_key in t_low:
                            found.add(canonical)
                            break

        return ", ".join(sorted(found))

    @classmethod
    def extract_education(cls, *texts) -> str:
        blob = ' '.join(coerce_text(t) for t in texts if t)
        if not blob:
            return ""
        priority = ['PhD', 'MBA', "Master's", "Bachelor's", "Associate's", 'High School']
        hits = set()
        for pattern, level in cls._education_patterns:
            if pattern.search(blob):
                hits.add(level)
        return ", ".join([lv for lv in priority if lv in hits])

    @classmethod
    def classify_industry(cls, company: str, description: str,
                          api_category: str = "") -> str:
        blob = f"{coerce_text(company)} {coerce_text(api_category)} {coerce_text(description)}".lower()
        if not blob.strip():
            return ""
        scores = defaultdict(int)
        for industry, keywords in INDUSTRY_KEYWORDS.items():
            for kw in keywords:
                if kw in blob:
                    weight = 3 if kw in coerce_text(company).lower() or kw in coerce_text(api_category).lower() else 1
                    scores[industry] += weight
        if not scores:
            return ""
        return max(scores, key=scores.get)

    @classmethod
    def classify_remote_status(cls, location: str, description: str,
                               api_flag=None) -> str:
        loc = coerce_text(location).lower()
        desc = coerce_text(description).lower()
        flag = coerce_text(api_flag).lower()

        if flag:
            if 'remote' in flag or 'anywhere' in flag or 'work from home' in flag:
                return 'Remote'
            if 'hybrid' in flag:
                return 'Hybrid'
            if 'on-site' in flag or 'onsite' in flag or 'in-office' in flag:
                return 'On-site'

        if 'remote' in loc or 'anywhere' in loc or 'worldwide' in loc or 'work from home' in loc:
            return 'Remote'
        if 'hybrid' in loc:
            return 'Hybrid'

        snippet = desc[:1500]
        if re.search(r'\b(fully remote|100%\s*remote|work from anywhere|remote-first)\b', snippet):
            return 'Remote'
        if re.search(r'\bhybrid\b|\b\d\s*days?\s*(in\s*(the\s*)?office|on[\-\s]?site)\b', snippet):
            return 'Hybrid'
        if re.search(r'\bon[\-\s]?site\b|\bin[\-\s]?office\b|\bin person\b', snippet):
            return 'On-site'

        if loc and 'remote' not in loc:
            return 'On-site'

        return ""

    @classmethod
    def clean_html(cls, html) -> str:
        text_in = coerce_text(html)
        if not text_in:
            return ""
        try:
            soup = BeautifulSoup(text_in, 'lxml')
            text = soup.get_text(separator=' ', strip=True)
            return re.sub(r'\s+', ' ', text)
        except Exception:
            return re.sub(r'<[^>]+>', ' ', text_in)

# ============================================================================
# 8. SCRAPER FALLBACK
# ============================================================================

class ScraperFallback:
    def __init__(self):
        self.session = cloudscraper.create_scraper(
            browser={'browser': 'chrome', 'platform': 'windows', 'mobile': False}
        )
        self.session.headers.update({'Accept-Language': 'en-US,en;q=0.9'})

    @sleep_and_retry
    @limits(calls=10, period=60)
    def fetch(self, url: str, timeout: int = 7) -> Optional[str]:
        if not url:
            return None
        try:
            r = self.session.get(url, timeout=timeout, allow_redirects=True)
            if r.status_code == 200 and len(r.text) > 500:
                return Enrichment.clean_html(r.text)
        except Exception as e:
            log.debug(f"Scraper failed for {url[:80]}: {e}")
        return None

# ============================================================================
# 9. EXTRACTORS
# ============================================================================

class BaseExtractor:
    NAME = "base"

    def __init__(self):
        self.stats = defaultdict(int)
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'Mozilla/5.0 (compatible; JobETL/1.1)'})

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        reraise=True,
    )
    def _get(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault('timeout', 20)
        r = self.session.get(url, **kwargs)
        r.raise_for_status()
        return r

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=30),
        retry=retry_if_exception_type((requests.RequestException, requests.Timeout)),
        reraise=True,
    )
    def _post(self, url: str, **kwargs) -> requests.Response:
        kwargs.setdefault('timeout', 20)
        r = self.session.post(url, **kwargs)
        r.raise_for_status()
        return r

    def fetch(self, role: str) -> list:
        raise NotImplementedError

    def fetch_all_roles(self, roles: list) -> list:
        all_jobs = []
        for role in roles:
            try:
                jobs = self.fetch(role)
                self.stats[f'fetched_{role}'] = len(jobs)
                all_jobs.extend(jobs)
                log.info(f"[{self.NAME}] '{role}': {len(jobs)} raw jobs")
            except Exception as e:
                log.error(f"[{self.NAME}] failed on '{role}': {e}")
                self.stats[f'error_{role}'] = str(e)
        return all_jobs


class AdzunaExtractor(BaseExtractor):
    NAME = "adzuna"
    BASE = "https://api.adzuna.com/v1/api/jobs/us/search"

    @sleep_and_retry
    @limits(calls=25, period=60)
    def fetch(self, role: str) -> list:
        if not ADZUNA_APP_ID or not ADZUNA_APP_KEY:
            log.warning("[adzuna] missing credentials, skipping")
            return []
        out = []
        pages = max(1, RESULTS_PER_ROLE_PER_API // 50)
        for page in range(1, pages + 1):
            url = f"{self.BASE}/{page}"
            params = {
                'app_id': ADZUNA_APP_ID, 'app_key': ADZUNA_APP_KEY,
                'results_per_page': 50, 'what_phrase': role,
                'content-type': 'application/json',
            }
            try:
                r = self._get(url, params=params)
                data = r.json()
                for j in data.get('results', []):
                    if not isinstance(j, dict):
                        continue
                    out.append(self._parse(j))
            except Exception as e:
                log.error(f"[adzuna] page {page} '{role}': {e}")
                break
        return out

    def _parse(self, j: dict) -> JobRecord:
        loc_obj = j.get('location') or {}
        loc_str = loc_obj.get('display_name', '')
        area = loc_obj.get('area') or []
        country_hint = area[0] if area else ''
        company = (j.get('company') or {}).get('display_name', '')
        category = (j.get('category') or {}).get('label', '')

        # Salary via universal parser
        smin, smax = SalaryParser.parse_pair(
            j.get('salary_min'), j.get('salary_max'),
            unit_hint=str(j.get('salary_is_predicted', ''))
        )

        return JobRecord(
            job_title=j.get('title'),
            company=company,
            location=loc_str,
            description=Enrichment.clean_html(j.get('description', '')),
            salary_min=smin,
            salary_max=smax,
            job_url=j.get('redirect_url'),
            date_posted=(j.get('created') or '')[:10] or None,
            _source=self.NAME,
            _raw={'country_hint': country_hint, 'category': category},
        )


class JoobleExtractor(BaseExtractor):
    NAME = "jooble"
    BASE = "https://jooble.org/api/"

    @sleep_and_retry
    @limits(calls=20, period=60)
    def fetch(self, role: str) -> list:
        if not JOOBLE_API_KEY:
            log.warning("[jooble] missing credentials, skipping")
            return []
        url = f"{self.BASE}{JOOBLE_API_KEY}"
        out = []
        pages = max(1, RESULTS_PER_ROLE_PER_API // 20)
        for page in range(1, pages + 1):
            payload = {"keywords": role, "location": "USA",
                       "page": str(page), "ResultOnPage": "20"}
            try:
                r = self._post(url, json=payload)
                data = r.json()
                for j in data.get('jobs', []):
                    if not isinstance(j, dict):
                        continue
                    out.append(self._parse(j))
            except Exception as e:
                log.error(f"[jooble] page {page} '{role}': {e}")
                break
        return out

    def _parse(self, j: dict) -> JobRecord:
        smin, smax = SalaryParser.parse(j.get('salary', ''))
        return JobRecord(
            job_title=j.get('title'),
            company=j.get('company'),
            location=j.get('location'),
            description=Enrichment.clean_html(j.get('snippet', '')),
            salary_min=smin, salary_max=smax,
            job_url=j.get('link'),
            date_posted=(j.get('updated') or '')[:10] or None,
            _source=self.NAME,
            _raw={'type': j.get('type', ''), 'salary_raw': j.get('salary', '')},
        )


class USAJobsExtractor(BaseExtractor):
    NAME = "usajobs"
    BASE = "https://data.usajobs.gov/api/search"

    def __init__(self):
        super().__init__()
        if USAJOBS_API_KEY and USAJOBS_EMAIL:
            self.session.headers.update({
                'Host': 'data.usajobs.gov',
                'User-Agent': USAJOBS_EMAIL,
                'Authorization-Key': USAJOBS_API_KEY,
            })

    @sleep_and_retry
    @limits(calls=50, period=60)
    def fetch(self, role: str) -> list:
        if not USAJOBS_API_KEY:
            log.warning("[usajobs] missing credentials, skipping")
            return []
        out = []
        params = {'Keyword': role, 'ResultsPerPage': min(RESULTS_PER_ROLE_PER_API, 100)}
        try:
            r = self._get(self.BASE, params=params)
            data = r.json()
            items = data.get('SearchResult', {}).get('SearchResultItems', [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                descriptor = item.get('MatchedObjectDescriptor', {})
                if not isinstance(descriptor, dict):
                    continue
                out.append(self._parse(descriptor))
        except Exception as e:
            log.error(f"[usajobs] '{role}': {e}")
        return out

    def _parse(self, d: dict) -> JobRecord:
        locs = d.get('PositionLocation') or []
        first_loc = locs[0] if locs and isinstance(locs[0], dict) else {}
        loc_str = first_loc.get('LocationName', '')
        country = first_loc.get('CountryCode', '')
        org = d.get('OrganizationName', '') or d.get('DepartmentName', '')

        # PATCHED v1.1: PositionRemuneration may be list or dict
        salary_obj = d.get('PositionRemuneration') or []
        if isinstance(salary_obj, list) and salary_obj and isinstance(salary_obj[0], dict):
            salary_obj = salary_obj[0]
        elif not isinstance(salary_obj, dict):
            salary_obj = {}

        smin, smax = SalaryParser.parse_pair(
            salary_obj.get('MinimumRange'),
            salary_obj.get('MaximumRange'),
            unit_hint=str(salary_obj.get('RateIntervalCode', '')),
        )

        user_area = d.get('UserArea', {})
        details = user_area.get('Details', {}) if isinstance(user_area, dict) else {}

        # PATCHED v1.1: MajorDuties may be list or string
        description_parts = [
            coerce_text(d.get('QualificationSummary', '')),
            coerce_text(details.get('JobSummary', '')),
            coerce_text(details.get('MajorDuties', '')),  # NOW SAFE — handles list
        ]
        description = ' '.join(p for p in description_parts if p)

        benefits_text = coerce_text(details.get('Benefits', ''))

        return JobRecord(
            job_title=d.get('PositionTitle'),
            company=org,
            location=loc_str,
            description=Enrichment.clean_html(description),
            salary_min=smin, salary_max=smax,
            job_url=d.get('PositionURI'),
            date_posted=(d.get('PublicationStartDate') or '')[:10] or None,
            _source=self.NAME,
            _raw={
                'country_hint': country,
                'benefits_text': benefits_text,
                'industry': 'Government / Public Sector',
            },
        )


class TheMuseExtractor(BaseExtractor):
    NAME = "themuse"
    BASE = "https://www.themuse.com/api/public/jobs"

    @sleep_and_retry
    @limits(calls=30, period=60)
    def fetch(self, role: str) -> list:
        out = []
        pages = max(1, RESULTS_PER_ROLE_PER_API // 20)
        for page in range(0, pages):
            params = {'page': page, 'descending': 'true', 'location': 'United States'}
            try:
                r = self._get(self.BASE, params=params)
                data = r.json()
                for j in data.get('results', []):
                    if not isinstance(j, dict):
                        continue
                    title = (j.get('name') or '').lower()
                    if role.lower() not in title:
                        continue
                    out.append(self._parse(j))
            except Exception as e:
                log.error(f"[themuse] page {page} '{role}': {e}")
                break
        return out

    def _parse(self, j: dict) -> JobRecord:
        locs = j.get('locations') or []
        loc_str = ", ".join(l.get('name', '') for l in locs if isinstance(l, dict))
        company = (j.get('company') or {}).get('name', '')
        levels = [l.get('name', '') for l in (j.get('levels') or []) if isinstance(l, dict)]
        categories = [c.get('name', '') for c in (j.get('categories') or []) if isinstance(c, dict)]
        tags = [t.get('short_name', '') for t in (j.get('tags') or []) if isinstance(t, dict)]

        return JobRecord(
            job_title=j.get('name'),
            company=company,
            location=loc_str,
            description=Enrichment.clean_html(j.get('contents', '')),
            job_url=(j.get('refs') or {}).get('landing_page'),
            date_posted=(j.get('publication_date') or '')[:10] or None,
            _source=self.NAME,
            _raw={
                'levels': levels,
                'categories': categories,
                'tags': tags,  # NOW PASSED AS LIST
            },
        )


class RemotiveExtractor(BaseExtractor):
    NAME = "remotive"
    BASE = "https://remotive.com/api/remote-jobs"

    @sleep_and_retry
    @limits(calls=20, period=60)
    def fetch(self, role: str) -> list:
        params = {'search': role, 'limit': RESULTS_PER_ROLE_PER_API}
        try:
            r = self._get(self.BASE, params=params)
            data = r.json()
            return [self._parse(j) for j in data.get('jobs', []) if isinstance(j, dict)]
        except Exception as e:
            log.error(f"[remotive] '{role}': {e}")
            return []

    def _parse(self, j: dict) -> JobRecord:
        loc_hint = j.get('candidate_required_location', '') or ''
        # Build a location string that lets GeoValidator do the right thing
        if 'usa' in loc_hint.lower() or 'united states' in loc_hint.lower():
            location = f"Remote, {loc_hint}"
        elif loc_hint:
            location = f"Remote, {loc_hint}"  # worldwide/global will now survive Layer 2
        else:
            location = 'Remote'

        smin, smax = SalaryParser.parse(j.get('salary', ''))

        return JobRecord(
            job_title=j.get('title'),
            company=j.get('company_name'),
            location=location,
            description=Enrichment.clean_html(j.get('description', '')),
            salary_min=smin, salary_max=smax,
            job_url=j.get('url'),
            date_posted=(j.get('publication_date') or '')[:10] or None,
            _source=self.NAME,
            _raw={
                'category': j.get('category', ''),
                'job_type': j.get('job_type', ''),
                'salary_raw': j.get('salary', ''),
                'tags': j.get('tags', []) or [],   # NOW LIST
                'candidate_required_location': loc_hint,
            },
        )


class ArbeitnowExtractor(BaseExtractor):
    NAME = "arbeitnow"
    BASE = "https://www.arbeitnow.com/api/job-board-api"

    @sleep_and_retry
    @limits(calls=20, period=60)
    def fetch(self, role: str) -> list:
        out = []
        for page in range(1, 4):
            try:
                r = self._get(self.BASE, params={'page': page})
                data = r.json()
                for j in data.get('data', []):
                    # PATCHED v1.1: GUARD against malformed non-dict items
                    if not isinstance(j, dict):
                        log.debug(f"[arbeitnow] skipping malformed item: {type(j).__name__}")
                        continue
                    title = (j.get('title') or '').lower()
                    if role.lower() not in title:
                        continue
                    out.append(self._parse(j))
            except Exception as e:
                log.error(f"[arbeitnow] page {page} '{role}': {e}")
                break
        return out

    def _parse(self, j: dict) -> JobRecord:
        location = j.get('location') or ''
        tags = j.get('tags') or []
        # Tags may not be a list — defensive coerce
        if not isinstance(tags, list):
            tags = coerce_list(tags)

        remote_flag = 'Remote' if j.get('remote') else None
        job_types = j.get('job_types') or []
        if not isinstance(job_types, list):
            job_types = coerce_list(job_types)

        # Arbeitnow rarely has salary, but if it does:
        smin, smax = SalaryParser.parse(j.get('salary', ''))

        return JobRecord(
            job_title=j.get('title'),
            company=j.get('company_name'),
            location=location,
            description=Enrichment.clean_html(j.get('description', '')),
            salary_min=smin, salary_max=smax,
            job_url=j.get('url'),
            date_posted=(j.get('created_at') or '')[:10] if j.get('created_at') else None,
            _source=self.NAME,
            _raw={
                'tags': tags,            # LIST
                'remote_flag': remote_flag,
                'job_types': job_types,  # LIST
            },
        )

# ============================================================================
# 10. PIPELINE ORCHESTRATOR
# ============================================================================

class JobETLPipeline:
    def __init__(self):
        self.extractors = [
            AdzunaExtractor(), JoobleExtractor(), USAJobsExtractor(),
            TheMuseExtractor(), RemotiveExtractor(), ArbeitnowExtractor(),
        ]
        self.scraper = ScraperFallback()
        self.seen_hashes: set = set()
        self.stats = defaultdict(int)
        self.rejection_reasons = defaultdict(int)

    def ingest(self) -> list:
        all_raw = []
        with ThreadPoolExecutor(max_workers=len(self.extractors)) as ex:
            futures = {ex.submit(e.fetch_all_roles, TARGET_ROLES): e for e in self.extractors}
            for fut in as_completed(futures):
                extractor = futures[fut]
                try:
                    jobs = fut.result()
                    all_raw.extend(jobs)
                    self.stats[f'raw_{extractor.NAME}'] = len(jobs)
                    log.info(f"[OK] [{extractor.NAME}] returned {len(jobs)} raw records")
                except Exception as e:
                    log.error(f"[ERROR] [{extractor.NAME}] crashed: {e}")
        log.info(f"TOTAL RAW: {len(all_raw)} jobs across all APIs")
        return all_raw

    def transform(self, raw_jobs: list) -> list:
        cleaned = []

        for job in tqdm(raw_jobs, desc="Transforming", ncols=80):
            # 1. Essential Check
            if not job.job_title or not job.company:
                self.stats['missing_essentials'] += 1
                continue

            # 2. Strict Title Filter (Must contain one of our target roles)
            title_lower = job.job_title.lower()
            if not any(role.lower() in title_lower for role in TARGET_ROLES):
                self.rejection_reasons['unrelated_job_title'] += 1
                self.stats[f'rejected_{job._source}'] += 1
                continue

            is_us, city, state, reason = GeoValidator.validate(
                location_str=job.location or "",
                country_hint=(job._raw.get('country_hint')
                              or job._raw.get('candidate_required_location') or ""),
                description=job.description or "",
            )
            if not is_us:
                self.rejection_reasons[reason] += 1
                self.stats[f'rejected_{job._source}'] += 1
                continue

            job.city = city
            job.state = state
            job.date_retrieved = datetime.now(timezone.utc).strftime('%Y-%m-%d')

            self._enrich(job)

            # Aggressive scraping fallback for thin records across ALL APIs
            if job.completeness_score() < 4 and job.job_url:
                scraped = self.scraper.fetch(job.job_url)
                if scraped:
                    if not job.description or len(job.description) < 300:
                        job.description = scraped[:8000]
                    self._enrich(job, deep=True)
                    self.stats['scraped'] += 1

            fp = job.fingerprint()
            if fp in self.seen_hashes:
                self.stats['duplicates'] += 1
                continue
            self.seen_hashes.add(fp)

            if job.description and len(job.description) > 20000:
                job.description = job.description[:20000]

            cleaned.append(job)
            self.stats[f'accepted_{job._source}'] += 1

        return cleaned

    def _enrich(self, job: JobRecord, deep: bool = False):
        desc = job.description or ""
        title = job.job_title or ""
        company = job.company or ""
        raw = job._raw or {}

        # Collect API tags from ALL possible sources
        api_tag_sources = []
        for key in ('tags', 'categories', 'levels', 'job_types'):
            v = raw.get(key)
            if v:
                api_tag_sources.extend(coerce_list(v))
        category = coerce_text(raw.get('category', '')) or coerce_text(raw.get('categories', ''))
        benefits_text = coerce_text(raw.get('benefits_text', ''))

        # PATCHED v1.1: pass api_tags to skills/soft_skills/benefits extractors
        if not job.skills:
            job.skills = Enrichment.extract_skills(
                title, desc, category, api_tags=api_tag_sources
            ) or None

        if not job.soft_skills:
            job.soft_skills = Enrichment.extract_soft_skills(
                desc, title, api_tags=api_tag_sources
            ) or None

        if not job.benefits:
            job.benefits = Enrichment.extract_benefits(
                desc, api_tags=api_tag_sources, structured_benefits=benefits_text
            ) or None

        if not job.education:
            job.education = Enrichment.extract_education(desc, title) or None

        if not job.industry:
            preset = raw.get('industry')
            job.industry = preset or Enrichment.classify_industry(
                company, desc, category
            ) or None

        if not job.remote_status:
            api_flag = (raw.get('remote_flag') or coerce_text(raw.get('job_type', ''))
                        or ' '.join(coerce_list(raw.get('job_types', []))))
            job.remote_status = Enrichment.classify_remote_status(
                job.location or "", desc, api_flag
            ) or None

    def load_to_bigquery(self, jobs: list):
        if not jobs:
            log.warning("Nothing to load.")
            return
        if not BQ_PROJECT_ID:
            log.error("BQ_PROJECT_ID env var not set.")
            return

        credentials = service_account.Credentials.from_service_account_file(
            CREDENTIALS_PATH,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        client = bigquery.Client(credentials=credentials, project=BQ_PROJECT_ID)
        table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"

        self._ensure_table(client, table_ref)
        existing_hashes = self._fetch_existing_hashes(client, table_ref)
        log.info(f"Loaded {len(existing_hashes)} existing fingerprints")

        to_insert = [j.to_bq_dict() for j in jobs if j.fingerprint() not in existing_hashes]
        log.info(f"After cross-BQ dedup: {len(to_insert)} new rows")

        if not to_insert:
            return

        # Emergency Local Backup
        try:
            with open("emergency_backup.json", "w", encoding="utf-8") as f:
                json.dump(to_insert, f, indent=4)
            log.info("Emergency backup saved locally to emergency_backup.json")
        except Exception as e:
            log.warning(f"Could not save local backup: {e}")

        # Free Tier Compatible BigQuery Load Job
        try:
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
                source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            )
            load_job = client.load_table_from_json(to_insert, table_ref, job_config=job_config)
            load_job.result()  # Wait for the job to complete
            log.info(f"BQ LOAD COMPLETE — {len(to_insert)} rows inserted via Load Job.")
        except Exception as e:
            log.error(f"BQ LOAD ERROR: {e}")

    def _ensure_table(self, client, table_ref):
        schema = [
            bigquery.SchemaField("job_title", "STRING"),
            bigquery.SchemaField("company", "STRING"),
            bigquery.SchemaField("industry", "STRING"),
            bigquery.SchemaField("location", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("state", "STRING"),
            bigquery.SchemaField("description", "STRING"),
            bigquery.SchemaField("salary_min", "FLOAT"),
            bigquery.SchemaField("salary_max", "FLOAT"),
            bigquery.SchemaField("job_url", "STRING"),
            bigquery.SchemaField("skills", "STRING"),
            bigquery.SchemaField("soft_skills", "STRING"),
            bigquery.SchemaField("education", "STRING"),
            bigquery.SchemaField("remote_status", "STRING"),
            bigquery.SchemaField("benefits", "STRING"),
            bigquery.SchemaField("date_retrieved", "DATE"),
            bigquery.SchemaField("date_posted", "DATE"),
        ]
        try:
            client.get_table(table_ref)
        except Exception:
            table = bigquery.Table(table_ref, schema=schema)
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY, field="date_retrieved",
            )
            client.create_table(table)
            log.info(f"[OK] Created table {table_ref}")

    def _fetch_existing_hashes(self, client, table_ref) -> set:
        query = f"""
        SELECT DISTINCT job_title, company, city
        FROM `{table_ref}`
        WHERE date_retrieved >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        """
        try:
            rows = client.query(query).result()
            hashes = set()
            for r in rows:
                title = (r.job_title or "").strip().lower()
                comp = (r.company or "").strip().lower()
                city = (r.city or "").strip().lower()
                hashes.add(hashlib.sha256(f"{title}|{comp}|{city}".encode()).hexdigest())
            return hashes
        except Exception as e:
            log.warning(f"Could not fetch existing hashes: {e}")
            return set()

    def run(self):
        log.info("=" * 70)
        log.info("  ULTIMATE US JOB ETL PIPELINE v1.1 — STARTING")
        log.info("=" * 70)
        t0 = time.time()

        raw = self.ingest()
        cleaned = self.transform(raw)
        self.load_to_bigquery(cleaned)

        elapsed = time.time() - t0
        log.info("=" * 70)
        log.info(f"  PIPELINE COMPLETE in {elapsed:.1f}s")
        log.info("=" * 70)
        log.info("RAW BY API:")
        for k, v in self.stats.items():
            if k.startswith('raw_'):
                log.info(f"  {k}: {v}")
        log.info("ACCEPTED BY API:")
        for k, v in self.stats.items():
            if k.startswith('accepted_'):
                log.info(f"  {k}: {v}")
        log.info("REJECTION REASONS:")
        for reason, count in sorted(self.rejection_reasons.items(), key=lambda x: -x[1]):
            log.info(f"  {reason}: {count}")
        log.info(f"DUPLICATES SKIPPED: {self.stats['duplicates']}")
        log.info(f"SCRAPED FOR ENRICHMENT: {self.stats['scraped']}")


if __name__ == "__main__":
    JobETLPipeline().run()

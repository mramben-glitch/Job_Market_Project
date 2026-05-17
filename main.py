import json
import os
import re
import time
import unicodedata
from datetime import datetime, timezone, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional, Set, Tuple

import geonamescache
import pandas as pd
import requests

from bs4 import BeautifulSoup
from dateutil import parser as date_parser
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

from extractors import extract_industry, extract_education, extract_benefits, extract_skills, extract_soft_skills

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



# ──────────────────────────────────────────────────────────────────────────────
# SHARED HELPERS FOR GEO & EXTRACTION
# ──────────────────────────────────────────────────────────────────────────────

def _normalize(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", text).strip().lower()


# ──────────────────────────────────────────────────────────────────────────────
# STATE EXTRACTION (powered by geonamescache — fully offline)
# ──────────────────────────────────────────────────────────────────────────────

@lru_cache(maxsize=1)
def _build_geo_lookups():
    """
    Build and cache all lookup tables from geonamescache.
    Called once per process; subsequent calls hit the LRU cache instantly.
    """
    gc = geonamescache.GeonamesCache(min_city_population=500)

    # --- US states ---
    us_states = gc.get_us_states()
    state_name_to_abbr: dict[str, str] = {v["name"].lower(): k for k, v in us_states.items()}
    abbr_set = frozenset(us_states.keys())

    # --- Cities (primary + alternate names, US only, pop >= 500) ---
    city_to_state_raw: dict[str, list[tuple[str, int]]] = {}
    for v in gc.get_cities().values():
        if v["countrycode"] != "US":
            continue
        state = v["admin1code"]
        pop   = v.get("population", 0)
        city_to_state_raw.setdefault(v["name"].lower(), []).append((state, pop))
        for alt in v.get("alternatenames", []):
            if alt and len(alt) > 2:
                city_to_state_raw.setdefault(alt.lower(), []).append((state, pop))

    # When a city name appears in multiple states, keep the most populous one.
    city_map: dict[str, str] = {
        name: max(entries, key=lambda x: x[1])[0]
        for name, entries in city_to_state_raw.items()
    }

    # --- Counties (full FIPS dataset, 3,235 entries) ---
    _COUNTY_SUFFIXES = (
        " county", " parish", " borough",
        " census area", " municipality", " city and borough",
    )
    county_multi: dict[str, list[str]] = {}
    for c in gc.get_us_counties():
        name = c["name"].lower()
        for suffix in _COUNTY_SUFFIXES:
            if name.endswith(suffix):
                name = name[: -len(suffix)]
                break
        name = name.strip()
        county_multi.setdefault(name, []).append(c["state"])

    # Unambiguous = county name maps to exactly one state across the whole US.
    county_unambiguous: dict[str, str] = {
        k: v[0] for k, v in county_multi.items() if len(v) == 1
    }

    return state_name_to_abbr, abbr_set, city_map, county_unambiguous, county_multi


_RE_COUNTY_SUFFIX = re.compile(
    r"([A-Za-z .'\-]+?)\s+(?:County|Parish|Borough|Municipality)\b",
    re.IGNORECASE,
)
_RE_STATE_US = re.compile(
    r"([A-Za-z ]+),\s*(?:US|USA|U\.S\.A?\.?)\b",
    re.IGNORECASE,
)


def extract_state(location: str, description: str = "") -> Optional[str]:
    """
    Return a 2-letter US state abbreviation or None.
    Resolution order (first match wins):
        1. 2-letter abbreviation token in location string
        2. Full state name in location string
        3. "State, US/USA" pattern in location string
        4. Unambiguous county name in location string
        5. City / alternate city name in location string
        6. Ambiguous county + city context for disambiguation
        7. Full state name in first 2,000 chars of description
        8. 2-letter abbreviation in first 500 chars of description (last resort)
    """
    state_name_to_abbr, abbr_set, city_map, county_unambiguous, county_multi = (
        _build_geo_lookups()
    )

    loc  = location or ""
    desc = description or ""

    # ── 1. 2-letter abbreviation token ─────────────────────────────────────
    for token in re.split(r"[,/|;\s]+", loc):
        t = token.strip().upper()
        if t in abbr_set:
            return t

    # ── 2. Full state name ─────────────────────────────────────────────────
    loc_norm = _normalize(loc)
    for name, abbr in state_name_to_abbr.items():
        if re.search(r"\b" + re.escape(name) + r"\b", loc_norm):
            return abbr

    # ── 3. "State, US/USA" pattern ────────────────────────────────────────
    m = _RE_STATE_US.search(loc)
    if m:
        candidate = m.group(1).strip().lower()
        if candidate in state_name_to_abbr:
            return state_name_to_abbr[candidate]

    # ── 4. Unambiguous county name ────────────────────────────────────────
    for m in _RE_COUNTY_SUFFIX.finditer(loc):
        county = _normalize(m.group(1))
        if county in county_unambiguous:
            return county_unambiguous[county]

    # ── 5. City name lookup (each comma-segment + full string) ────────────
    segments = [s.strip() for s in loc_norm.split(",")]
    for seg in [loc_norm] + segments:
        seg = seg.strip()
        if seg and seg in city_map:
            return city_map[seg]

    # ── 6. Ambiguous county + city disambiguation ──────────────────────────
    for m in _RE_COUNTY_SUFFIX.finditer(loc):
        county = _normalize(m.group(1))
        if county in county_multi:
            candidates = set(county_multi[county])
            for seg in segments:
                seg = seg.strip()
                if seg and seg in city_map and city_map[seg] in candidates:
                    return city_map[seg]
            if len(candidates) == 1:
                return next(iter(candidates))

    # ── 7. State name in description ───────────────────────────────────────
    if desc:
        desc_norm = _normalize(desc[:2000])
        for name, abbr in state_name_to_abbr.items():
            if re.search(r"\b" + re.escape(name) + r"\b", desc_norm):
                return abbr

    # ── 8. Abbreviation in description (last resort) ────────────────────────
    if desc:
        abbr_pattern = re.compile(
            r"(?<![A-Za-z])("
            + "|".join(sorted(abbr_set, key=len, reverse=True))
            + r")(?![A-Za-z])"
        )
        m = abbr_pattern.search(desc[:500])
        if m:
            return m.group(1).upper()

    return None


# ──────────────────────────────────────────────────────────────────────────────
# SKILLS EXTRACTION (role-specific: Analyst / Data Scientist)
# ──────────────────────────────────────────────────────────────────────────────

_SKILLS_RAW: list[tuple[str, list[str]]] = [
    # ── Core Languages ────────────────────────────────────────────────────
    ("Python",          [r"python"]),
    ("SQL",             [r"\bsql\b", r"structured\s+query\s+language"]),
    ("DAX",             [r"\bdax\b"]),
    ("M (Power Query)", [r"power\s*query\b", r"\bm\s+language\b"]),
    ("VBA",             [r"\bvba\b"]),
    ("MDX",             [r"\bmdx\b"]),
    ("Scala",           [r"\bscala\b"]),
    
    # ── Excel ─────────────────────────────────────────────────────────────
    ("Excel",           [r"\bexcel\b", r"microsoft\s+excel", r"ms\s+excel"]),
    ("VLOOKUP",         [r"\bv?hlookup\b", r"\bvlookup\b"]),
    ("INDEX MATCH",     [r"\bindex[\s/]+match\b"]),
    ("XLOOKUP",         [r"\bxlookup\b"]),
    ("Pivot Tables",    [r"pivot\s+table", r"pivottable"]),
    ("Advanced Excel",  [r"advanced\s+excel", r"excel\s+advanced"]),
    ("Conditional Formatting", [r"conditional\s+formatting"]),
    ("Macros",          [r"\bmacros?\b(?!\s*economics)"]),
    ("Power Pivot",     [r"power\s*pivot"]),
    ("Excel Dashboards",[r"excel\s+dashboard"]),
    
    # ── BI & Visualization ────────────────────────────────────────────────
    ("Tableau",         [r"tableau"]),
    ("Power BI",        [r"power[\s\-]?bi"]),
    ("Looker",          [r"\blooker\b(?!\s*studio)"]),
    ("Looker Studio",   [r"looker\s+studio", r"google\s+data\s+studio", r"data\s+studio"]),
    ("Qlik",            [r"qlik(?:view|sense|\.com)?"]),
    ("Metabase",        [r"metabase"]),
    ("Grafana",         [r"grafana"]),
    ("Superset",        [r"apache\s*superset", r"\bsuperset\b"]),
    ("Sisense",         [r"sisense"]),
    ("MicroStrategy",   [r"microstrategy"]),
    ("TIBCO Spotfire",  [r"spotfire"]),
    ("SAP BusinessObjects", [r"sap\s*bo\b", r"business\s*objects"]),
    ("Domo",            [r"\bdomo\b"]),
    ("ThoughtSpot",     [r"thoughtspot"]),
    
    # ── Databases & Querying ──────────────────────────────────────────────
    ("PostgreSQL",      [r"postgres(?:ql)?"]),
    ("MySQL",           [r"mysql"]),
    ("SQL Server",      [r"sql\s+server", r"\bmssql\b", r"\bt-sql\b", r"\btsql\b"]),
    ("Oracle DB",       [r"\boracle\b(?:\s*db|\s*database|\s*sql)?"]),
    ("SQLite",          [r"sqlite"]),
    ("BigQuery",        [r"big\s*query", r"\bgbq\b"]),
    ("Snowflake",       [r"snowflake"]),
    ("Redshift",        [r"redshift"]),
    ("Databricks",      [r"databricks"]),
    ("Teradata",        [r"teradata"]),
    ("Hive",            [r"\bhive\s*ql\b", r"\bhive\b"]),
    ("MongoDB",         [r"mongo(?:db)?"]),
    ("NoSQL",           [r"\bnosql\b"]),
    
    # ── Cloud Platforms ───────────────────────────────────────────────────
    ("AWS",             [r"\baws\b", r"amazon\s+web\s+services"]),
    ("Azure",           [r"microsoft\s+azure", r"\bazure\b"]),
    ("GCP",             [r"\bgcp\b", r"google\s+cloud"]),
    
    # ── ETL / Data Pipeline ───────────────────────────────────────────────
    ("dbt",             [r"\bdbt\b", r"data\s+build\s+tool"]),
    ("Airflow",         [r"\bairflow\b"]),
    ("Fivetran",        [r"fivetran"]),
    ("Stitch",          [r"\bstitch\b(?:\s*data)?"]),
    ("Informatica",     [r"informatica"]),
    ("Talend",          [r"\btalend\b"]),
    ("SSIS",            [r"\bssis\b"]),
    
    # ── Statistics & Analytics ────────────────────────────────────────────
    ("Statistics",      [r"\bstatistics\b", r"\bstatistical\s+analysis\b"]),
    ("Probability",     [r"\bprobability\b"]),
    ("A/B Testing",     [r"a\s*/\s*b\s+test", r"\bsplit\s+test", r"hypothesis\s+test"]),
    ("Regression Analysis", [r"(?:linear|logistic|multiple)\s+regression", r"regression\s+analysis"]),
    ("Time Series",     [r"time[\s\-]+series", r"\barima\b", r"\bsarima\b", r"\bprophet\b"]),
    ("Forecasting",     [r"\bforecasting\b", r"demand\s+forecast"]),
    ("Bayesian Analysis",[r"bayesian", r"\bbayes\b"]),
    ("Clustering",      [r"\bclustering\b", r"\bk[\s\-]?means\b"]),
    ("Cohort Analysis", [r"cohort\s+analysis"]),
    ("Funnel Analysis", [r"funnel\s+analysis"]),
    ("Causal Inference",[r"causal\s+inference", r"causal\s+analysis"]),
    ("Survival Analysis",[r"survival\s+analysis", r"churn\s+model"]),
    ("Multivariate Analysis",[r"multivariate\s+analysis", r"\bmanova\b", r"\banova\b"]),
    ("Monte Carlo",     [r"monte\s+carlo"]),
    ("Optimization",    [r"\boptimization\b", r"linear\s+programming"]),
    
    # ── Machine Learning & AI (Data Scientist) ────────────────────────────
    ("Machine Learning",[r"machine\s+learning", r"\bml\b(?=\s+model|\s+pipeline)"]),
    ("Deep Learning",   [r"deep\s+learning", r"neural\s+net(?:work)?"]),
    ("NLP",             [r"\bnlp\b", r"natural\s+language\s+processing"]),
    ("Computer Vision", [r"computer\s+vision", r"image\s+recognition"]),
    ("Generative AI",   [r"gen(?:erative)?\s*ai", r"\bllm\b", r"large\s+language\s+model", r"chatgpt"]),
    ("Feature Engineering",[r"feature\s+engineering", r"feature\s+selection"]),
    ("Model Validation",[r"model\s+validat", r"cross[\s\-]+validation"]),
    ("scikit-learn",    [r"scikit[\s\-]*learn", r"\bsklearn\b"]),
    ("TensorFlow",      [r"tensor\s*flow"]),
    ("PyTorch",         [r"py\s*torch"]),
    ("Keras",           [r"\bkeras\b"]),
    ("XGBoost",         [r"xgboost", r"xg\s*boost"]),
    ("LightGBM",        [r"light\s*gbm", r"lightgbm"]),
    ("MLflow",          [r"mlflow"]),
    
    # ── Python Libraries ──────────────────────────────────────────────────
    ("Pandas",          [r"\bpandas\b"]),
    ("NumPy",           [r"\bnumpy\b"]),
    ("Matplotlib",      [r"matplotlib"]),
    ("Seaborn",         [r"seaborn"]),
    ("Plotly",          [r"plotly"]),
    ("SciPy",           [r"scipy"]),
    ("Statsmodels",     [r"statsmodels"]),
    ("Jupyter",         [r"jupyter(?:\s+notebook|\s+lab)?"]),
    
    # ── R Ecosystem ───────────────────────────────────────────────────────
    ("RStudio",         [r"rstudio"]),
    ("ggplot2",         [r"ggplot2?"]),
    ("tidyverse",       [r"tidyverse", r"\bdplyr\b", r"\btidyr\b"]),
    ("Shiny",           [r"\bshiny\b(?:\s+app)?"]),
    
    # ── Marketing Analytics ───────────────────────────────────────────────
    ("Google Analytics",[r"google\s+analytics", r"\bga4\b"]),
    ("Adobe Analytics", [r"adobe\s+analytics", r"\bomniture\b"]),
    ("Google Ads",      [r"google\s+ads", r"google\s+adwords"]),
    ("Facebook Ads",    [r"facebook\s+ads", r"meta\s+ads"]),
    ("SEO",             [r"\bseo\b", r"search\s+engine\s+optimization"]),
    ("SEM",             [r"\bsem\b(?=\s+analyst|\s+specialist)", r"search\s+engine\s+marketing"]),
    ("Web Analytics",   [r"web\s+analytics", r"digital\s+analytics"]),
    ("Marketing Mix Modeling",[r"marketing\s+mix\s+model", r"\bmmm\b"]),
    ("Attribution Modeling",[r"attribution\s+model", r"multi[\s\-]?touch\s+attribution"]),
    ("Customer Segmentation",[r"customer\s+segmentation"]),
    ("Salesforce",      [r"salesforce", r"\bsfdc\b"]),
    ("HubSpot",         [r"hubspot"]),
    ("Marketo",         [r"marketo"]),
    ("Mailchimp",       [r"mailchimp"]),
    ("CRM",             [r"\bcrm\b", r"customer\s+relationship\s+management"]),
    
    # ── Product Analytics ──────────────────────────────────────────────────
    ("Mixpanel",        [r"mixpanel"]),
    ("Amplitude",       [r"amplitude"]),
    ("Pendo",           [r"\bpendo\b"]),
    ("Heap",            [r"\bheap\b(?:\s+analytics)?"]),
    ("FullStory",       [r"fullstory"]),
    ("Hotjar",          [r"hotjar"]),
    ("Segment",         [r"\bsegment\b(?:\s+cdp|\s+io)?"]),
    ("Customer Data Platform",[r"\bcdp\b", r"customer\s+data\s+platform"]),
    ("User Research",   [r"user\s+research", r"usability\s+test"]),
    ("Retention Analysis",[r"retention\s+analysis", r"retention\s+rate"]),
    ("DAU/MAU",         [r"\bdau\b", r"\bmau\b", r"\bwau\b"]),
    ("OKRs",            [r"\bokrs?\b", r"objectives\s+and\s+key\s+results"]),
    ("KPIs",            [r"\bkpis?\b", r"key\s+performance\s+indicator"]),
    
    # ── Financial Analytics ───────────────────────────────────────────────
    ("Financial Modeling",[r"financial\s+model", r"\bdcf\b"]),
    ("Financial Analysis",[r"financial\s+analysis"]),
    ("Valuation",       [r"\bvaluation\b", r"equity\s+valuation"]),
    ("FP&A",            [r"\bfp&a\b", r"financial\s+planning"]),
    ("Variance Analysis",[r"variance\s+analysis", r"budget\s+vs"]),
    ("P&L Management",  [r"p(?:rofit)?\s*(?:&|and)\s*l(?:oss)?", r"\bp&l\b"]),
    ("SAP",             [r"\bsap\b"]),
    ("Oracle Financials",[r"oracle\s+financials", r"oracle\s+erp"]),
    ("ERP",             [r"\berp\b", r"enterprise\s+resource\s+planning"]),
    ("QuickBooks",      [r"quickbooks"]),
    ("NetSuite",        [r"netsuite"]),
    ("Bloomberg",       [r"bloomberg"]),
    ("FactSet",         [r"factset"]),
    ("GAAP",            [r"\bgaap\b"]),
    ("IFRS",            [r"\bifrs\b"]),
    ("Risk Analysis",   [r"risk\s+analysis", r"risk\s+model"]),
    
    # ── Operations Analytics ───────────────────────────────────────────────
    ("Process Improvement",[r"process\s+improvement"]),
    ("Lean",            [r"\blean\b(?:\s+six\s+sigma)?"]),
    ("Six Sigma",       [r"six\s+sigma", r"\bdmaic\b"]),
    ("Supply Chain",    [r"supply\s+chain"]),
    ("Inventory Management",[r"inventory\s+management"]),
    ("Demand Planning", [r"demand\s+planning", r"demand\s+forecasting"]),
    ("Workforce Analytics",[r"workforce\s+analytics", r"hr\s+analytics"]),
    
    # ── Soft Skills & Methodologies ───────────────────────────────────────
    ("Agile",           [r"\bagile\b"]),
    ("Scrum",           [r"\bscrum\b"]),
    ("Data Storytelling",[r"data\s+storytelling", r"data\s+narrativ"]),
    ("Problem Solving", [r"problem[\s\-]+solving"]),
    ("Critical Thinking",[r"critical\s+thinking"]),
    ("Requirements Gathering",[r"requirements?\s+gathering"]),
    ("Stakeholder Management",[r"stakeholder\s+management"]),
    ("Project Management",[r"project\s+management", r"\bpmp\b"]),
    ("Jira",            [r"\bjira\b"]),
    ("Confluence",      [r"confluence"]),
    ("Data Governance", [r"data\s+governance", r"data\s+quality"]),
    ("Git",             [r"\bgit\b"]),
    ("GitHub",          [r"github"]),
]

# Compile all patterns (case-insensitive)
_R_PATTERN = re.compile(r"(?<!\w)R(?!\w)")   # strict case-sensitive

_SKILL_PATTERNS: list[tuple[str, re.Pattern]] = []
for _canonical, _patterns in _SKILLS_RAW:
    _combined = "|".join(f"(?:{p})" for p in _patterns)
    _SKILL_PATTERNS.append((_canonical, re.compile(_combined, re.IGNORECASE)))


# ──────────────────────────────────────────────────────────────────────────────
# REMOTE STATUS EXTRACTION (weighted scoring model)
# ──────────────────────────────────────────────────────────────────────────────

_REMOTE_SIGNALS: list[tuple[re.Pattern, int]] = [
    # Strong remote (+3)
    (re.compile(
        r"\b(?:fully?\s*remote|100\s*%\s*remote|remote[\s\-]*only|"
        r"entirely\s*remote|permanently\s*remote|remote[\s\-]*first)\b", re.I), +3),
    # Moderate remote (+2)
    (re.compile(
        r"\b(?:work(?:ing)?\s*(?:from|at)\s*home|wfh|telecommut(?:e|ing)|"
        r"distributed\s*team|location[\s\-]*independent|work\s*anywhere|"
        r"anywhere\s*in\s*(?:the\s*)?(?:us|usa|world)|no\s*office\s*required|"
        r"remote\s*work\s*(?:allowed|available|option|eligible))\b", re.I), +2),
    # General remote mention (+1)
    (re.compile(r"\bremote\b", re.I), +1),
    # Hybrid keyword (0, tracked separately)
    (re.compile(
        r"\b(?:hybrid|partial(?:ly)?\s*remote|remote[\s/\-]*hybrid|"
        r"flexible\s*work(?:ing)?|mix(?:ed)?\s*(?:of\s*)?(?:remote|office|on[\s\-]?site))\b",
        re.I), 0),
    # "X days in office per week" → explicit hybrid
    (re.compile(
        r"\b\d+\s*(?:days?\s*(?:per\s*week\s*)?(?:in[\s\-]?office|on[\s\-]?site|in\s*person)|"
        r"(?:in[\s\-]?office|on[\s\-]?site)\s*days?\s*per\s*week)\b", re.I), +2),
    # On-site (-2)
    (re.compile(
        r"\b(?:in[\s\-]person|on[\s\-]?site)\b|"
        r"\bin[\s\-]office\b(?!\s*day)(?!\s*\d)|"
        r"\b(?:must\s+(?:be\s+)?(?:located|based|reside|live)\s+(?:in|near|within)|"
        r"relocation\s+(?:required|assistance|package)|"
        r"authorized\s+to\s+work\s+in)\b|"
        r"(?:must\s+)?report\s+to\s+(?:(?:our|the)\s+)?\w+(?:[\s\-]\w+)?\s+office\b",
        re.I), -2),
    # Soft on-site (-1)
    (re.compile(
        r"\b(?:office[\s\-]based|our\s*(?:\w+\s+)*office|headquarters|hq|"
        r"commut(?:e|ing)|badge\s*access|parking\s*(?:provided|available)|"
        r"campus|building\s*access)\b", re.I), -1),
    # Job-type on-site (-2)
    (re.compile(
        r"\b(?:on[\s\-]?call|shift\s*work|night\s*shift|day\s*shift|"
        r"warehouse|field\s*(?:work|engineer|technician)|"
        r"travel\s*(?:required|up\s*to\s*\d))\b", re.I), -2),
]

_RE_HYBRID_EXPLICIT = re.compile(
    r"\b(?:hybrid|partial(?:ly)?\s*remote|flexible\s*work(?:ing)?)\b", re.I
)



# Global counter for debugging
JOBS_PROCESSED = 0


def load_environment() -> None:
    """Load environment variables from .env and set Google credentials."""
    dotenv_path = os.path.join(os.path.dirname(__file__), ".env")
    load_dotenv(dotenv_path=dotenv_path)

    credentials_path = os.path.join(os.path.dirname(__file__), "credentials.json")
    if os.path.isfile(credentials_path):
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path





def extract_remote_status(
    title: str,
    description: str,
    location: str = "",
) -> str:
    """
    Classify work arrangement using weighted scoring model.
    Returns: "Remote" | "Hybrid" | "On-site" | "Not Specified"
    """
    corpus = f"{title} {title} {location} {description}"  # title weighted 2×
    score        = 0
    hybrid_found = False

    for pattern, weight in _REMOTE_SIGNALS:
        if pattern.search(corpus):
            score += weight
            if weight == 0:
                hybrid_found = True

    if _RE_HYBRID_EXPLICIT.search(corpus):
        hybrid_found = True

    if score >= 2:
        return "Remote"
    elif score <= -2:
        return "On-site"
    elif hybrid_found:
        return "Hybrid"
    elif score == 1:
        return "Remote"
    else:
        return "Not Specified"





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
    """Build a standardized job row for BigQuery with all 17 columns. Return None if filtered out."""
    global JOBS_PROCESSED
    
    if not strict_inclusion_filter(job_title):
        return None

    JOBS_PROCESSED += 1

    location_str = location
    if isinstance(location, dict):
        location_str = location.get("display_name") or location.get("name")
    elif not isinstance(location, str):
        location_str = None

    # Clean description
    cleaned_description = strip_html(description)
    
    # Extract state from location (Claude's extract_state only returns state, not city)
    state = extract_state(location_str or "", cleaned_description or "")
    city = None  # BUG FIX #2: Claude's extract_state doesn't return city, so set to None
    
    # Extract skills, education, remote status, benefits (from extractors module)
    skills = extract_skills(cleaned_description)
    education = extract_education(cleaned_description)
    # BUG FIX: extract_remote_status now takes (title, description, location) as positional args
    remote_status = extract_remote_status(job_title or "", cleaned_description or "", location_str or "")
    benefits = extract_benefits(cleaned_description)
    
    # Extract industry and soft skills (NEW in Phase 3)
    company_name = safe_company_name(company)
    industry = extract_industry(company_name, cleaned_description or "")
    soft_skills = extract_soft_skills(cleaned_description)
    
    # Parse salary
    salary_min, salary_max = parse_salary_range(salary_raw)
    
    # Get today's date for fallback
    today_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    # Parse date_posted with ironclad fallback to today's date
    date_posted_str = parse_relative_date(date_posted, api_dict, today_date)

    # Debugging: print every 10th job
    if JOBS_PROCESSED % 10 == 0:
        print(f"[DEBUG] Job #{JOBS_PROCESSED}: ${salary_min or 'N/A'} - ${salary_max or 'N/A'} | State: {state or 'N/A'} | Remote: {remote_status} | Industry: {industry or 'N/A'} | Skills: {skills or 'N/A'}")

    return {
        "job_title": job_title,
        "company": company_name,
        "industry": industry,
        "location": location_str,
        "city": city,
        "state": state,
        "description": cleaned_description,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "job_url": job_url,
        "skills": skills,
        "soft_skills": soft_skills,
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
                    "full_description": 1,
                    "content-type": "full",
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
                f"https://jooble.org/api/{api_key}?full_description=true",
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
                    # Deep parse USAJobs UserArea.Details for comprehensive description
                    details = descriptor.get("UserArea", {}).get("Details", {})
                    description_parts = []
                    if details.get("JobSummary"):
                        description_parts.append(f"Job Summary:\n{details.get('JobSummary')}")
                    if details.get("MajorDuties"):
                        # MajorDuties may be a list
                        duties = details.get("MajorDuties")
                        if isinstance(duties, list):
                            description_parts.append(f"Major Duties:\n" + "\n".join(duties))
                        else:
                            description_parts.append(f"Major Duties:\n{duties}")
                    if details.get("Education"):
                        description_parts.append(f"Education:\n{details.get('Education')}")
                    if details.get("Requirements"):
                        description_parts.append(f"Requirements:\n{details.get('Requirements')}")
                    
                    # Concatenate all parts or fallback
                    full_description = "\n\n".join(description_parts) if description_parts else (
                        descriptor.get("QualificationSummary")
                        or descriptor.get("PositionSummary")
                    )
                    
                    row = build_row(
                        job_title=descriptor.get("PositionTitle"),
                        company=descriptor.get("OrganizationName"),
                        location=", ".join(location_values) if location_values else None,
                        description=full_description,
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
                    "full_description": "true",
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
    """Create BigQuery table with the 17-column Smart ETL schema if it does not exist."""
    schema = [
        bigquery.SchemaField("job_title", "STRING"),
        bigquery.SchemaField("company", "STRING"),
        bigquery.SchemaField("industry", "STRING"),
        bigquery.SchemaField("location", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("description", "STRING"),
        bigquery.SchemaField("salary_min", "FLOAT64"),
        bigquery.SchemaField("salary_max", "FLOAT64"),
        bigquery.SchemaField("job_url", "STRING"),
        bigquery.SchemaField("skills", "STRING"),
        bigquery.SchemaField("soft_skills", "STRING"),
        bigquery.SchemaField("education", "STRING"),
        bigquery.SchemaField("remote_status", "STRING"),
        bigquery.SchemaField("benefits", "STRING"),
        bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
        bigquery.SchemaField("date_posted", "DATE"),
    ]
    table = bigquery.Table(TABLE_ID, schema=schema)
    client.create_table(table, exists_ok=True)
    print(f"[BigQuery] Table {TABLE_ID} ensured with Smart ETL schema (17 columns).")


def load_rows_to_bigquery(rows: List[Dict[str, Any]], client: bigquery.Client) -> None:
    """Load job rows into BigQuery with the 17-column Smart ETL schema."""
    if not rows:
        print("[BigQuery] No new rows to load.")
        return

    try:
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("job_title", "STRING"),
                bigquery.SchemaField("company", "STRING"),
                bigquery.SchemaField("industry", "STRING"),
                bigquery.SchemaField("location", "STRING"),
                bigquery.SchemaField("city", "STRING"),
                bigquery.SchemaField("state", "STRING"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("salary_min", "FLOAT64"),
                bigquery.SchemaField("salary_max", "FLOAT64"),
                bigquery.SchemaField("job_url", "STRING"),
                bigquery.SchemaField("skills", "STRING"),
                bigquery.SchemaField("soft_skills", "STRING"),
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
    print("  • Skills detection: 140+ hard skills (R, Python, SQL, Excel, Power BI, Tableau, AWS, Azure, Spark, Snowflake, Looker, etc.)")
    print("  • Soft skills detection: 17 soft-skill categories (Communication, Leadership, Collaboration, Problem Solving, etc.)")
    print("  • Industry classification: 40+ industry mappings with keyword fallback")
    print("  • Education detection: Bachelor's, Master's, PhD")
    print("  • Remote status detection: Remote, Hybrid, On-site, Not Specified")
    print("  • Benefits detection: 401(k), Health Insurance, PTO, Bonus, Stock/Equity, Remote/Flexible, Learning & Dev, Commuter")
    print("  • Date parsing: 'today', 'yesterday', '3 days ago', '30+' → YYYY-MM-DD")
    print("  • Debugging: Sample debug output every 10 jobs")
    print("  • BigQuery schema: 17 columns (expanded from 15 with industry + soft_skills)")
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

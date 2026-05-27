"""
etl_engine.py
=============
Extraction + BigQuery loader for the Live Data Analyst Job Dashboard.
Includes strict US-only filtering, exact-title matching, and advanced regex salary extraction (Annual, Hourly, Monthly).
"""

from __future__ import annotations

import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

log = logging.getLogger("etl_engine")

try:
    import ahocorasick  # type: ignore
    _AC_AVAILABLE = True
except ImportError:
    _AC_AVAILABLE = False
    log.info("pyahocorasick not installed -- using regex fallback.")

# ===========================================================================
# 1.  CANONICAL DICTIONARIES
# ===========================================================================
HARD_SKILLS: dict[str, list[str]] = {
    "Python":          ["python", "python3", "py3", "phyton", "pyhton"],
    "R":               ["r programming", "r language", "r studio", "rstudio"],
    "SQL":             ["sql", "t-sql", "tsql", "pl/sql", "plsql", "ansi sql", "structured query language", "sequel"],
    "SAS":             ["sas programming", "sas analytics"],
    "SPSS":            ["spss", "ibm spss"],
    "Stata":           ["stata"],
    "MATLAB":          ["matlab", "mat lab"],
    "Scala":           ["scala"],
    "Java":            ["java programming", "java se", "java ee"],
    "JavaScript":      ["javascript", "java script", "node.js", "nodejs", "node js"],
    "VBA":             ["vba", "visual basic", "visual basic for applications"],
    "Bash":            ["bash scripting", "shell scripting", "shell script"],
    "Julia":           ["julia language", "julia programming"],
    "Tableau":         ["tableau", "tableu", "tablaeu", "tableau desktop", "tableau server"],
    "Power BI":        ["power bi", "powerbi", "power-bi", "pbi", "microsoft power bi"],
    "Looker":          ["looker", "looker studio", "google data studio", "data studio"],
    "Qlik":            ["qlik", "qlikview", "qlik view", "qlik sense", "qliksense"],
    "Excel":           ["excel", "ms excel", "microsoft excel", "advanced excel", "excell"],
    "Google Sheets":   ["google sheets", "gsheets", "g-sheets"],
    "Domo":            ["domo bi", "domo dashboards"],
    "Mode":            ["mode analytics", "mode bi"],
    "Sigma":           ["sigma computing"],
    "Metabase":        ["metabase"],
    "Superset":        ["apache superset", "superset"],
    "MicroStrategy":   ["microstrategy", "micro strategy"],
    "BigQuery":        ["bigquery", "big query", "google bigquery", "gbq"],
    "Snowflake":       ["snowflake"],
    "Redshift":        ["redshift", "amazon redshift", "aws redshift"],
    "Databricks":      ["databricks", "data bricks"],
    "PostgreSQL":      ["postgres", "postgresql", "postgre sql", "psql"],
    "MySQL":           ["mysql", "my sql"],
    "MongoDB":         ["mongodb", "mongo db", "mongo"],
    "Oracle DB":       ["oracle db", "oracle database", "oracle sql"],
    "SQL Server":      ["sql server", "ms sql", "mssql", "microsoft sql server"],
    "DynamoDB":        ["dynamodb", "dynamo db"],
    "Cassandra":       ["cassandra"],
    "Teradata":        ["teradata"],
    "AWS":             ["aws", "amazon web services"],
    "Azure":           ["azure", "microsoft azure", "ms azure"],
    "GCP":             ["gcp", "google cloud", "google cloud platform"],
    "Airflow":         ["airflow", "apache airflow"],
    "dbt":             ["dbt", "data build tool"],
    "Spark":           ["apache spark", "pyspark", "py-spark", "spark sql"],
    "Hadoop":          ["hadoop"],
    "Kafka":           ["kafka", "apache kafka"],
    "ETL":             ["etl", "elt", "extract transform load", "extract-transform-load"],
    "Fivetran":        ["fivetran", "five tran"],
    "Stitch":          ["stitch data", "stitch etl"],
    "Informatica":     ["informatica"],
    "Talend":          ["talend"],
    "SSIS":            ["ssis", "sql server integration services"],
    "SSRS":            ["ssrs", "sql server reporting services"],
    "Machine Learning":["machine learning", "machine-learning", "ml models", "mashine learning"],
    "Deep Learning":   ["deep learning", "deep-learning", "neural networks", "neural net"],
    "TensorFlow":      ["tensorflow", "tensor flow", "tf2"],
    "PyTorch":         ["pytorch", "py torch", "py-torch"],
    "scikit-learn":    ["scikit-learn", "scikit learn", "sklearn", "sci-kit learn"],
    "Pandas":          ["pandas library", "pandas dataframe"],
    "NumPy":           ["numpy", "num py", "num-py"],
    "NLP":             ["nlp", "natural language processing", "natural-language"],
    "Computer Vision": ["computer vision", "cv (computer vision)", "image recognition"],
    "A/B Testing":     ["a/b test", "a/b testing", "ab testing", "a-b testing", "split test", "split testing", "multivariate testing"],
    "Forecasting":     ["forecasting", "time series", "time-series", "timeseries", "arima", "prophet"],
    "Regression":      ["regression analysis", "linear regression", "logistic regression", "regression modeling"],
    "Clustering":      ["clustering", "k-means", "kmeans", "hierarchical clustering", "dbscan"],
    "AI":              ["artificial intelligence", "generative ai", "genai", "gen ai", "gen-ai", "llm", "llms", "large language model"],
    "MLOps":           ["mlops", "ml ops", "ml-ops"],
    "Google Analytics":["google analytics", "ga4", "ga 4", "universal analytics"],
    "Adobe Analytics": ["adobe analytics", "omniture", "site catalyst"],
    "Mixpanel":        ["mixpanel", "mix panel"],
    "Amplitude":       ["amplitude analytics"],
    "Segment":         ["segment.io", "segment analytics"],
    "Heap":            ["heap analytics", "heap io"],
    "Pendo":           ["pendo"],
    "Salesforce":      ["salesforce", "sfdc", "sales force"],
    "HubSpot":         ["hubspot", "hub spot", "hub-spot"],
    "Marketo":         ["marketo"],
    "Pardot":          ["pardot"],
    "Statistics":      ["statistics", "statistical analysis", "statistical modeling", "biostatistics", "stats"],
    "Hypothesis Testing": ["hypothesis testing", "statistical testing", "significance testing"],
    "Data Modeling":   ["data modeling", "data modelling", "dimensional modeling", "star schema", "snowflake schema"],
    "Data Mining":     ["data mining"],
    "Data Visualization": ["data visualization", "data visualisation", "data viz", "dataviz"],
    "Git":             ["git", "github", "git-hub", "gitlab", "git lab", "bitbucket", "bit bucket"],
    "JIRA":            ["jira", "atlassian jira"],
    "Confluence":      ["confluence"],
    "Agile":           ["agile", "agile methodology", "scrum", "kanban", "agile/scrum"],
    "PowerPoint":      ["powerpoint", "power point", "ms powerpoint", "ppt"],
    "Word":            ["ms word", "microsoft word"],
    "Outlook":         ["outlook", "ms outlook"],
    "Financial Modeling": ["financial modeling", "financial modelling", "financial models", "valuation modeling"],
    "FP&A":            ["fp&a", "fpa", "financial planning and analysis", "financial planning & analysis"],
    "ERP":             ["erp", "enterprise resource planning", "sap erp", "oracle erp", "netsuite"],
    "SAP":             ["sap", "sap s/4hana", "sap hana"],
    "Six Sigma":       ["six sigma", "lean six sigma", "6 sigma"],
    "Supply Chain":    ["supply chain", "supply-chain"],
    "Alteryx":         ["alteryx"],
    "Knime":           ["knime"],
    "RapidMiner":      ["rapidminer", "rapid miner"],
}

BARE_LETTER_RULES: dict[str, re.Pattern] = {
    "R":     re.compile(r"(?<![A-Za-z0-9_])R(?![A-Za-z0-9_&])"),
    "SAS":   re.compile(r"\bSAS\b(?![A-Za-z])"),
    "Domo":  re.compile(r"\bDomo\b"),
    "Spark": re.compile(r"\bSpark\b(?:\s+(?:SQL|Streaming|MLlib))?", re.IGNORECASE),
    "Pandas":re.compile(r"\bpandas\b", re.IGNORECASE),
    "AI":    re.compile(r"\bAI\b(?![A-Za-z])"),
    "Amplitude": re.compile(r"\bAmplitude\b(?!\s+(?:of|measurement))"),
}

BARE_CONTEXT_REQUIRED: dict[str, re.Pattern] = {
    "R":  re.compile(r"\b(python|sql|sas|spss|stata|stat|statisti|program|analyt|data|machine\s*learn|regression|ggplot|tidyverse|shiny|rstudio|model|forecast)\b", re.IGNORECASE),
    "AI": re.compile(r"\b(machine\s*learn|model|llm|gpt|nlp|deep\s*learn|tensorflow|pytorch|chatbot|ai\s+engineer|generative|prompt|agent)\b", re.IGNORECASE),
}

SOFT_SKILLS: dict[str, list[str]] = {
    "Communication":          ["communication", "communicate", "verbal communication", "written communication", "communications skills", "communicaton"],
    "Collaboration":          ["collaboration", "collaborative", "teamwork", "team player", "team-player", "cross-functional", "cross functional"],
    "Problem Solving":        ["problem solving", "problem-solving", "troubleshoot", "troubleshooting", "problemsolving"],
    "Analytical Thinking":    ["analytical thinking", "analytical skills", "critical thinking", "critical-thinking", "analytical mindset"],
    "Attention to Detail":    ["attention to detail", "detail-oriented", "detail oriented", "meticulous"],
    "Leadership":             ["leadership", "lead a team", "mentor", "mentoring", "team lead"],
    "Time Management":        ["time management", "prioritize", "prioritise", "prioritization", "prioritisation"],
    "Adaptability":           ["adaptable", "adaptability", "flexible", "flexibility", "adapt to change"],
    "Self-Starter":           ["self-starter", "self starter", "self-motivated", "self motivated", "autonomous", "self-directed", "self directed"],
    "Curiosity":              ["intellectual curiosity", "curious mindset", "curiosity-driven"],
    "Stakeholder Management": ["stakeholder management", "stakeholders", "manage stakeholders", "stakeholder engagement"],
    "Presentation":           ["presentation skills", "present findings", "storytelling", "data storytelling", "presenting to executives"],
    "Project Management":     ["project management", "project manager", "manage projects"],
    "Organization":           ["organizational skills", "organisational skills", "highly organized", "highly organised", "well organized", "well organised"],
    "Decision Making":        ["decision making", "decision-making", "data-driven decisions"],
    "Negotiation":            ["negotiation skills", "negotiate"],
    "Creativity":             ["creativity", "creative thinking"],
    "Customer Focus":         ["customer focused", "customer-focused", "client focused", "client-focused", "customer-centric", "customer centric"],
}

EDUCATION: dict[str, list[str]] = {
    "PhD":         ["ph.d", "phd", "ph d", "doctorate", "doctoral degree", "doctor of philosophy"],
    "MBA":         ["mba", "m.b.a", "master of business administration"],
    "Master's":    ["master's degree", "masters degree", "master degree", "master's", "masters", "graduate degree", "m.s degree", "m.s.", "m.a.", "m sc", "msc", "ms degree", "ma degree", "ms in ", "ma in ", "msc in ", "ms preferred", "ms required", "ms or phd", "ma preferred", "ma required"],
    "Bachelor's":  ["bachelor's degree", "bachelors degree", "bachelor degree", "bachelor's", "bachelors", "undergraduate degree", "4-year degree", "four-year degree", "four year degree", "b.s degree", "b.s.", "b.a.", "b.sc", "bsc", "bs degree", "ba degree", "b. eng", "beng", "bs in ", "ba in ", "bsc in ", "bs preferred", "bs required", "bs or ms", "ba preferred", "ba required"],
    "Associate's": ["associate's degree", "associates degree", "associate degree", "associate's", "associates", "a.a.", "a.s.", "aa degree", "as degree"],
    "High School": ["high school diploma", "high-school diploma", "ged", "highschool diploma"],
}

EDUCATION_PRIORITY = {"PhD": 5, "MBA": 4, "Master's": 4, "Bachelor's": 3, "Associate's": 2, "High School": 1}

BENEFITS: dict[str, list[str]] = {
    "401(k)":              ["401k", "401 k", "401(k)", "retirement plan", "retirement savings", "retirement benefits"],
    "Health Insurance":    ["health insurance", "medical insurance", "medical coverage", "health benefits", "medical benefits", "health coverage"],
    "Dental":              ["dental insurance", "dental coverage", "dental plan", "dental benefits", "dental"],
    "Vision":              ["vision insurance", "vision coverage", "vision plan", "vision benefits", "vision care", "vision"],
    "Life Insurance":      ["life insurance"],
    "Disability Insurance":["disability insurance", "short-term disability", "long-term disability", "std/ltd"],
    "PTO":                 ["pto", "paid time off", "paid vacation", "vacation days", "vacation time"],
    "Unlimited PTO":       ["unlimited pto", "unlimited vacation", "unlimited paid time off"],
    "Paid Holidays":       ["paid holidays"],
    "Parental Leave":      ["parental leave", "maternity leave", "paternity leave", "family leave"],
    "Remote Work":         ["remote work", "work from home", "wfh", "work-from-home", "fully remote", "100% remote"],
    "Hybrid Schedule":     ["hybrid schedule", "hybrid work", "hybrid model", "hybrid arrangement"],
    "Flexible Hours":      ["flexible hours", "flexible schedule", "flex time", "flex-time", "flexible working hours"],
    "Stock Options":       ["stock options", "equity grant", "equity package", "rsu", "rsus", "restricted stock units", "restricted stock"],
    "Bonus":               ["performance bonus", "annual bonus", "sign-on bonus", "signing bonus", "year-end bonus", "quarterly bonus", "discretionary bonus"],
    "Tuition Reimbursement":["tuition reimbursement", "tuition assistance", "education reimbursement", "education assistance"],
    "Professional Development":["professional development", "learning budget", "training budget", "learning stipend", "career development"],
    "Gym / Wellness":      ["gym membership", "wellness program", "wellness stipend", "fitness reimbursement", "fitness stipend", "wellness benefits"],
    "Free Meals":          ["free meals", "free lunch", "catered meals", "free food", "free breakfast"],
    "Commuter Benefits":   ["commuter benefits", "transit benefits", "transportation stipend", "commuter stipend"],
    "FSA":                 ["fsa", "flexible spending account"],
    "HSA":                 ["hsa", "health savings account"],
    "Employee Discount":   ["employee discount", "employee perks"],
    "Relocation":          ["relocation assistance", "relocation package", "relocation support"],
    "Pet Insurance":       ["pet insurance"],
    "ESPP":                ["espp", "employee stock purchase plan"],
}

INDUSTRY_KEYWORDS: dict[str, list[str]] = {
    "Technology":     ["saas", "tech company", "technology company", "software company", "software platform", "tech startup"],
    "Finance":        ["bank", "banking", "financial services", "investment bank", "hedge fund", "asset management", "fintech", "trading firm", "wealth management", "private equity", "venture capital"],
    "Healthcare":     ["healthcare", "health care", "hospital", "clinical", "patient care", "medical center", "health system"],
    "Pharma / Biotech":["pharmaceutical", "pharma", "biotech", "biotechnology", "drug discovery", "medtech"],
    "Insurance":      ["insurance company", "insurer", "under underwriting", "actuarial", "p&c insurance", "life insurance carrier"],
    "Retail / E-commerce":["retailer", "retail company", "e-commerce", "ecommerce", "consumer goods", "merchandising", "online retail"],
    "Consulting":     ["consulting firm", "management consulting", "advisory firm"],
    "Manufacturing":  ["manufacturing", "manufacturer", "factory", "production line", "industrial automation"],
    "Media / Advertising":["media company", "advertising agency", "publishing", "entertainment company", "ad tech", "adtech"],
    "Education":      ["edtech", "university", "school district", "higher education", "k-12"],
    "Government":     ["federal government", "state agency", "public sector", "department of", "municipal", "city of"],
    "Energy / Utilities":["oil and gas", "renewable energy", "utility company", "utilities", "power generation"],
    "Telecom":        ["telecom", "telecommunications", "wireless carrier"],
    "Logistics":      ["logistics company", "shipping", "freight", "warehousing", "3pl", "third-party logistics"],
    "Real Estate":    ["real estate", "property management", "reit", "proptech"],
    "Non-Profit":     ["non-profit", "nonprofit", "ngo", "501(c)"],
}

COMPANY_INDUSTRY_KEYWORDS: dict[str, list[str]] = {
    "Insurance":          ["insurance", "insurer", "assurance"],
    "Pharma / Biotech":   ["pharma", "biotech", "biosciences", "therapeutics", "pharmaceutical"],
    "Finance":            ["bank", "capital", "financial", "investments", "securities", "advisors"],
    "Retail / E-commerce":["retail", "retailer", "stores", "mart"],
    "Consulting":         ["consulting", "consultants", "advisory"],
    "Energy / Utilities": ["energy", "power", "utilities", "oil", "gas"],
    "Government":         ["department of", "agency", "city of", "state of", "federal", "bureau of"],
    "Education":          ["university", "college", "school", "academy"],
    "Telecom":            ["telecom", "wireless", "communications"],
    "Logistics":          ["logistics", "shipping", "freight"],
    "Real Estate":        ["real estate", "properties", "realty"],
    "Manufacturing":      ["manufacturing", "industries"],
    "Non-Profit":         ["foundation", "association", "society"],
    "Media / Advertising":["media", "advertising", "publishing", "studios"],
    "Healthcare":         ["health", "hospital", "clinic", "medical", "healthcare"],
    "Technology":         ["technologies", "software", "labs", "systems", "io", "data"],
}

# ===========================================================================
# 2.  MATCHER (Aho-Corasick with regex fallback)
# ===========================================================================
class _Matcher:
    _ALNUM = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")

    def __init__(self, mapping: dict[str, list[str]]) -> None:
        self.mapping = mapping
        self._ac = None
        self._regex: dict[str, re.Pattern] = {}
        self._alias_edges: dict[str, tuple[bool, bool]] = {}

        if _AC_AVAILABLE:
            A = ahocorasick.Automaton()
            for canonical, aliases in mapping.items():
                for alias in aliases:
                    alias_lc = alias.lower()
                    A.add_word(alias_lc, (canonical, alias_lc))
                    self._alias_edges[alias_lc] = (alias_lc[0] in self._ALNUM, alias_lc[-1] in self._ALNUM)
            A.make_automaton()
            self._ac = A
        else:
            for canonical, aliases in mapping.items():
                parts = []
                for a in aliases:
                    left  = r"(?<![A-Za-z0-9_])" if a[0]  in self._ALNUM else ""
                    right = r"(?![A-Za-z0-9_])"  if a[-1] in self._ALNUM else ""
                    parts.append(f"{left}{re.escape(a)}{right}")
                pat = "(?:" + "|".join(parts) + ")"
                self._regex[canonical] = re.compile(pat, re.IGNORECASE)

    def find(self, text: str) -> list[str]:
        if not text:
            return []
        text_lc = text.lower()
        hits: set[str] = set()

        if self._ac is not None:
            for end_idx, (canonical, alias) in self._ac.iter(text_lc):
                start_idx = end_idx - len(alias) + 1
                left_alnum, right_alnum = self._alias_edges[alias]
                left_ok = (not left_alnum) or (start_idx == 0) or (text_lc[start_idx - 1] not in self._ALNUM)
                right_ok = (not right_alnum) or (end_idx + 1 == len(text_lc)) or (text_lc[end_idx + 1] not in self._ALNUM)
                if left_ok and right_ok:
                    hits.add(canonical)
        else:
            for canonical, pat in self._regex.items():
                if pat.search(text):
                    hits.add(canonical)
        return [c for c in self.mapping if c in hits]

# ===========================================================================
# 3.  ENRICHMENT STATISTICS
# ===========================================================================
@dataclass
class EnrichmentStats:
    rows: int = 0
    filled: dict[str, int] = field(default_factory=lambda: {
        "industry": 0, "city": 0, "state": 0,
        "salary_min": 0, "salary_max": 0,
        "hard_skills": 0, "soft_skills": 0,
        "education": 0, "benefits": 0,
        "remote_status": 0,
    })

    def log_report(self) -> None:
        log.info("=" * 64)
        log.info("ENRICHMENT REPORT  rows=%d", self.rows)
        log.info("-" * 64)
        for col, n in self.filled.items():
            pct = (n / self.rows * 100) if self.rows else 0
            log.info("  %-15s %5d / %5d  (%.1f%%)", col, n, self.rows, pct)
        log.info("=" * 64)

# ===========================================================================
# 4.  THE ENRICHER
# ===========================================================================
_HTML_TAG = re.compile(r"<[^>]+>")
_MULTI_WS = re.compile(r"\s+")

class JobEnricher:
    _US_STATES = {
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
        "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
        "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
        "TX","UT","VT","VA","WA","WV","WI","WY","DC",
    }
    _US_STATE_NAMES = {
        "alabama":"AL","alaska":"AK","arizona":"AZ","arkansas":"AR",
        "california":"CA","colorado":"CO","connecticut":"CT","delaware":"DE",
        "florida":"FL","georgia":"GA","hawaii":"HI","idaho":"ID","illinois":"IL",
        "indiana":"IN","iowa":"IA","kansas":"KS","kentucky":"KY","louisiana":"LA",
        "maine":"ME","maryland":"MD","massachusetts":"MA","michigan":"MI",
        "minnesota":"MN","mississippi":"MS","missouri":"MO","montana":"MT",
        "nebraska":"NE","nevada":"NV","new hampshire":"NH","new jersey":"NJ",
        "new mexico":"NM","new york":"NY","north carolina":"NC","north dakota":"ND",
        "ohio":"OH","oklahoma":"OK","oregon":"OR","pennsylvania":"PA",
        "rhode island":"RI","south carolina":"SC","south dakota":"SD",
        "tennessee":"TN","texas":"TX","utah":"UT","vermont":"VT","virginia":"VA",
        "washington":"WA","west virginia":"WV","wisconsin":"WI","wyoming":"WY",
        "district of columbia":"DC",
    }
    _SAL_PERIOD_HINTS = {
        "hour": 2080, "hr": 2080, "/hr": 2080, "per hour": 2080, "hourly": 2080,
        "week": 52, "wk": 52, "/wk": 52, "per week": 52, "weekly": 52,
        "month": 12, "mo": 12, "/mo": 12, "per month": 12, "monthly": 12,
    }
    _SAL_MIN_ANNUAL = 15_000
    _SAL_MAX_ANNUAL = 1_000_000

    def __init__(self) -> None:
        self.stats = EnrichmentStats()
        self.m_hard      = _Matcher(HARD_SKILLS)
        self.m_soft      = _Matcher(SOFT_SKILLS)
        self.m_edu       = _Matcher(EDUCATION)
        self.m_benefits  = _Matcher(BENEFITS)
        self.m_industry  = _Matcher(INDUSTRY_KEYWORDS)
        self.m_industry_company = _Matcher(COMPANY_INDUSTRY_KEYWORDS)

        # STRICT ROLE FILTER
        self._allowed_roles = re.compile(
            r"\b(data|analyst|analytics|scientist|intelligence|product|marketing|business|operations|financial|finance)\b", 
            re.IGNORECASE
        )
        
        # STRICT LOCATION FILTER (Non-US Banned List)
        self._banned_locations = re.compile(
            r"\b(india|china|uk|united kingdom|ireland|london|bangalore|shanghai|hyderabad|dublin|europe|asia|emea|latam|mexico|canada|toronto|vancouver|sydney|australia)\b", 
            re.IGNORECASE
        )

    _PUNCT_NORMALIZE = str.maketrans({
        "\u2018": "'", "\u2019": "'", "\u201c": '"', "\u201d": '"',
        "\u2013": "-", "\u2014": "-", "\u2026": "...", "\u00a0": " ",
        "\u200b": "",  "\u200c": "", "\u200d": "", "\u2011": "-",
    })

    @classmethod
    def _clean(cls, text: str | None) -> str:
        if not text: return ""
        text = text.translate(cls._PUNCT_NORMALIZE)
        text = unicodedata.normalize("NFKD", text)
        text = _HTML_TAG.sub(" ", text)
        return _MULTI_WS.sub(" ", text).strip()

    @staticmethod
    def _bare_letter_hits(text: str) -> list[str]:
        out = []
        for canonical, pat in BARE_LETTER_RULES.items():
            if not pat.search(text): continue
            ctx_pat = BARE_CONTEXT_REQUIRED.get(canonical)
            if ctx_pat and not ctx_pat.search(text): continue
            out.append(canonical)
        return out

    @staticmethod
    def _resolve_education(hits: list[str]) -> str | None:
        if not hits: return None
        return max(hits, key=lambda h: EDUCATION_PRIORITY.get(h, 0))

    def _parse_location(self, raw_loc: str | None) -> tuple[str | None, str | None, str | None]:
        loc = (raw_loc or "").strip()
        if not loc: return (None, None, None)
        low = loc.lower()
        if "remote" in low or "anywhere" in low or "work from home" in low:
            return (loc, "Remote", None)
        parts = [p.strip() for p in loc.split(",") if p.strip()]
        if len(parts) >= 2:
            city_part, state_part = parts[0], parts[1]
            if len(state_part) == 2 and state_part.upper() in self._US_STATES:
                return (loc, city_part, state_part.upper())
            if state_part.lower() in self._US_STATE_NAMES:
                return (loc, city_part, self._US_STATE_NAMES[state_part.lower()])
        for code in re.findall(r"\b[A-Z]{2}\b", loc):
            if code in self._US_STATES:
                city = loc.split(code)[0].strip(" ,")
                return (loc, city or None, code)
        for name, code in self._US_STATE_NAMES.items():
            idx = low.find(name)
            if idx > 0:
                city = loc[:idx].strip(" ,").title() or None
                return (loc, city, code)
        return (loc, None, None)

    @staticmethod
    def _parse_remote_status(location: str, description: str) -> str | None:
        blob = f"{location or ''} {description or ''}".lower()
        if not blob.strip(): return None
        if re.search(r"\bhybrid\b", blob): return "Hybrid"
        if re.search(r"\b(remote|work[- ]from[- ]home|wfh|telework|telecommute|fully\s+remote)\b", blob): return "Remote"
        if location and "remote" not in (location or "").lower(): return "On-site"
        return None

    @classmethod
    def _normalize_salary(cls, val: Any, hint_text: str) -> float | None:
        if val is None or val == "": return None
        try: f = float(val)
        except (TypeError, ValueError): return None
        if f <= 0: return None
        hint = (hint_text or "").lower()
        multiplier = 1
        for kw, m in cls._SAL_PERIOD_HINTS.items():
            if kw in hint:
                multiplier = m
                break
        annual = f * multiplier
        if annual < cls._SAL_MIN_ANNUAL or annual > cls._SAL_MAX_ANNUAL: return None
        return round(annual, 2)

    # REGEX SALARY EXTRACTOR (Annual, Hourly, Monthly)
    def _extract_salary_from_text(self, text: str) -> tuple[float | None, float | None]:
        if not text: return None, None
        
        # 1. Annual Standard: $80,000 - $120,000
        p1 = re.search(r"\$\s*(\d{2,3}),(\d{3})\s*(?:-|to|and)\s*\$\s*(\d{2,3}),(\d{3})", text)
        if p1:
            return float(p1.group(1) + p1.group(2)), float(p1.group(3) + p1.group(4))
            
        # 2. Annual Shorthand: $80k - $120k
        p2 = re.search(r"\$\s*(\d{2,3})k\s*(?:-|to|and)\s*\$\s*(\d{2,3})k", text, re.IGNORECASE)
        if p2:
            return float(p2.group(1)) * 1000, float(p2.group(2)) * 1000

        # 3. Hourly: $25 - $45 /hr, per hour, hourly (Converts to Annual: * 2080)
        p3 = re.search(r"\$\s*(\d{1,3}(?:\.\d{2})?)\s*(?:-|to|and)\s*\$\s*(\d{1,3}(?:\.\d{2})?)\s*(?:/hr|per hour|hourly|an hour)", text, re.IGNORECASE)
        if p3:
            return float(p3.group(1)) * 2080, float(p3.group(2)) * 2080

        # 4. Monthly: $5,000 - $8,000 /mo, per month (Converts to Annual: * 12)
        p4 = re.search(r"\$\s*(\d{1,2}),?(\d{3})\s*(?:-|to|and)\s*\$\s*(\d{1,2}),?(\d{3})\s*(?:/mo|per month|monthly|a month)", text, re.IGNORECASE)
        if p4:
            return float(p4.group(1) + p4.group(2)) * 12, float(p4.group(3) + p4.group(4)) * 12
            
        return None, None

    def enrich(self, raw: dict[str, Any]) -> dict[str, Any]:
        title = self._clean(raw.get("job_title") or raw.get("title"))
        
        # FILTER 1: Drop irrelevant roles
        if not title or not self._allowed_roles.search(title):
            return {} 

        location_raw = raw.get("location") or ""
        
        # FILTER 2: Drop foreign locations
        if self._banned_locations.search(location_raw):
            return {}

        self.stats.rows += 1
        company = self._clean(raw.get("company")) or None
        description = self._clean(raw.get("description") or raw.get("snippet") or "")
        search_text = f"{title} {company or ''} {description}".strip()

        hard = self.m_hard.find(search_text)
        hard += [c for c in self._bare_letter_hits(search_text) if c not in hard]
        soft = self.m_soft.find(search_text)
        edu  = self._resolve_education(self.m_edu.find(search_text))
        bens = self.m_benefits.find(search_text)

        ind_company = self.m_industry_company.find(company or "")
        ind_desc    = self.m_industry.find(description)
        if ind_company: industry = ind_company[0]
        elif ind_desc: industry = ind_desc[0]
        else: industry = None

        location, city, state = self._parse_location(location_raw)
        remote_status = self._parse_remote_status(location or "", description)

        sal_hint = " ".join(str(raw.get(k, "")) for k in ("salary_period", "salary_unit", "salary_type", "salary_text", "period"))
        salary_min = self._normalize_salary(raw.get("salary_min"), sal_hint)
        salary_max = self._normalize_salary(raw.get("salary_max"), sal_hint)
        
        # Apply Regex Extractor if API provided nulls
        if not salary_min and not salary_max:
            s_min, s_max = self._extract_salary_from_text(description)
            if s_min and s_max and s_min >= self._SAL_MIN_ANNUAL:
                salary_min, salary_max = s_min, s_max

        if salary_min and salary_max and salary_min > salary_max:
            salary_min, salary_max = salary_max, salary_min

        dp = raw.get("date_posted")
        if isinstance(dp, datetime): date_posted = dp.date().isoformat()
        elif isinstance(dp, str) and dp: date_posted = dp[:10]
        else: date_posted = None

        row = {
            "job_title":      title or None,
            "company":        company,
            "industry":       industry,
            "location":       location,
            "city":           city,
            "state":          state,
            "description":    description[:5000] if description else None,
            "salary_min":     salary_min,
            "salary_max":     salary_max,
            "job_url":        raw.get("job_url") or raw.get("url"),
            "hard_skills":    ", ".join(hard) if hard else None,
            "soft_skills":    ", ".join(soft) if soft else None,
            "education":      edu,
            "remote_status":  remote_status,
            "benefits":       ", ".join(bens) if bens else None,
            "date_posted":    date_posted,
            "date_retrieved": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "source_api":     raw.get("source_api") or "unknown",
        }

        for col in self.stats.filled:
            if row.get(col) not in (None, "", []):
                self.stats.filled[col] += 1
        return row

    def enrich_many(self, raws: Iterable[dict[str, Any]]) -> pd.DataFrame:
        rows = [self.enrich(r) for r in raws if (r.get("job_url") or r.get("url"))]
        rows = [r for r in rows if r]  # Filter out dropped jobs
        df = pd.DataFrame(rows)
        if not df.empty:
            df = df.drop_duplicates(subset=["job_url"], keep="first").reset_index(drop=True)
        return df

# ===========================================================================
# 5.  BIGQUERY LOADER
# ===========================================================================
SCHEMA: list[bigquery.SchemaField] = [
    bigquery.SchemaField("job_title",      "STRING"),
    bigquery.SchemaField("company",        "STRING"),
    bigquery.SchemaField("industry",       "STRING"),
    bigquery.SchemaField("location",       "STRING"),
    bigquery.SchemaField("city",           "STRING"),
    bigquery.SchemaField("state",          "STRING"),
    bigquery.SchemaField("description",    "STRING"),
    bigquery.SchemaField("salary_min",     "FLOAT"),
    bigquery.SchemaField("salary_max",     "FLOAT"),
    bigquery.SchemaField("job_url",        "STRING", mode="REQUIRED"),
    bigquery.SchemaField("hard_skills",    "STRING"),
    bigquery.SchemaField("soft_skills",    "STRING"),
    bigquery.SchemaField("education",      "STRING"),
    bigquery.SchemaField("remote_status",  "STRING"),
    bigquery.SchemaField("benefits",       "STRING"),
    bigquery.SchemaField("date_posted",    "DATE"),
    bigquery.SchemaField("date_retrieved", "TIMESTAMP"),
    bigquery.SchemaField("source_api",     "STRING"),
]

class BigQueryLoader:
    def __init__(self, project_id: str, dataset: str, table: str, credentials_path: str | None = None) -> None:
        if credentials_path: os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", credentials_path)
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.client = bigquery.Client(project=project_id)
        self.full_target = f"{project_id}.{dataset}.{table}"

    def ensure_table(self) -> None:
        try:
            self.client.get_table(self.full_target)
        except NotFound:
            table_ref = bigquery.Table(self.full_target, schema=SCHEMA)
            self.client.create_table(table_ref)

    def upsert(self, df: pd.DataFrame) -> int:
        if df is None or df.empty: return 0
        self.ensure_table()
        df = df.copy()
        if "date_posted" in df.columns: df["date_posted"] = pd.to_datetime(df["date_posted"], errors="coerce").dt.date
        if "date_retrieved" in df.columns: df["date_retrieved"] = pd.to_datetime(df["date_retrieved"], errors="coerce", utc=True)

        try:
            query = f"SELECT DISTINCT job_url FROM `{self.full_target}`"
            existing = set(self.client.query(query).to_dataframe()["job_url"])
            df = df[~df["job_url"].isin(existing)]
        except Exception:
            pass

        if df.empty: return 0
        job_config = bigquery.LoadJobConfig(schema=SCHEMA, write_disposition="WRITE_APPEND")
        self.client.load_table_from_dataframe(df, self.full_target, job_config=job_config).result()
        return len(df)
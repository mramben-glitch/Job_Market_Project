"""
extractors.py
=============
Job-listing fetchers for six free APIs:

    Adzuna, USAJobs, Jooble, The Muse, Remotive, ArbeitNow

Design goals
------------
* Pull thousands of rows per run by:
    - Iterating over multiple target role queries
    - Paginating each query to its safe limit
    - Running queries in parallel via a ThreadPoolExecutor
* Respect free-tier rate limits via:
    - A simple per-host token bucket (max in-flight per domain)
    - Exponential backoff on 429 / 5xx
    - Short jitter between requests
* Output ONE consistent raw shape downstream consumers can rely on:

    {
        "job_title", "company", "description", "location",
        "salary_min", "salary_max", "salary_period",
        "job_url", "date_posted", "source_api"
    }

Authentication
--------------
Reads credentials from environment variables (no hardcoded keys):

    ADZUNA_APP_ID, ADZUNA_APP_KEY
    USAJOBS_USER_AGENT, USAJOBS_AUTH_KEY
    JOOBLE_API_KEY
    THE_MUSE_API_KEY        (optional, public endpoint works without)
    # Remotive and ArbeitNow are fully public
"""

from __future__ import annotations

import logging
import os
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Iterable

import requests

log = logging.getLogger("extractors")

# ---------------------------------------------------------------------------
# Target roles -- expanded list with synonyms gets more API breadth
# ---------------------------------------------------------------------------
TARGET_ROLES: list[str] = [
    "Data Analyst",
    "Product Analyst",
    "Marketing Analyst",
    "Business Analyst",
    "Business Intelligence Analyst",
    "Financial Analyst",
    "Operations Analyst",
    "Data Scientist",
]

# ---------------------------------------------------------------------------
# HTTP session with retry/backoff helpers
# ---------------------------------------------------------------------------
DEFAULT_TIMEOUT = 20
USER_AGENTS = [
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# Per-host concurrency caps to keep us under free-tier rate limits.
_HOST_SEMAPHORES: dict[str, threading.Semaphore] = {
    "api.adzuna.com":         threading.Semaphore(3),
    "data.usajobs.gov":       threading.Semaphore(3),
    "jooble.org":             threading.Semaphore(2),
    "www.themuse.com":        threading.Semaphore(3),
    "remotive.com":           threading.Semaphore(4),
    "www.arbeitnow.com":      threading.Semaphore(4),
}


def _host_of(url: str) -> str:
    try:
        return url.split("/", 3)[2].lower()
    except IndexError:
        return ""


def _http(method: str, url: str, *, retries: int = 4, **kwargs) -> requests.Response | None:
    """
    HTTP wrapper with:
      * per-host semaphore
      * UA rotation
      * exponential backoff on 429 / 5xx
      * silent return of None on terminal failure (caller decides)
    """
    host = _host_of(url)
    sem = _HOST_SEMAPHORES.get(host)
    headers = kwargs.pop("headers", {}) or {}
    headers.setdefault("User-Agent", random.choice(USER_AGENTS))
    headers.setdefault("Accept", "application/json")
    kwargs.setdefault("timeout", DEFAULT_TIMEOUT)

    def _do() -> requests.Response | None:
        backoff = 1.0
        for attempt in range(1, retries + 1):
            try:
                resp = requests.request(method, url, headers=headers, **kwargs)
            except requests.RequestException as e:
                log.warning("[%s] network error attempt %d: %s", host, attempt, e)
                time.sleep(backoff + random.random())
                backoff *= 2
                continue
            if resp.status_code == 200:
                return resp
            if resp.status_code in (429, 500, 502, 503, 504):
                wait = backoff + random.random()
                log.warning("[%s] %s -- backing off %.1fs (attempt %d/%d)",
                            host, resp.status_code, wait, attempt, retries)
                time.sleep(wait)
                backoff *= 2
                continue
            # 4xx other than 429 = permanent
            log.warning("[%s] %s for %s -- giving up", host, resp.status_code, url)
            return None
        log.warning("[%s] exhausted retries for %s", host, url)
        return None

    if sem is None:
        return _do()
    with sem:
        return _do()


# ===========================================================================
# 1.  ADZUNA   --   US listings, salary often present
# ===========================================================================
def fetch_adzuna(query: str, *, country: str = "us", pages: int = 10,
                 results_per_page: int = 50) -> list[dict[str, Any]]:
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_APP_KEY")
    if not (app_id and app_key):
        log.info("Adzuna credentials missing -- skipping.")
        return []

    out: list[dict[str, Any]] = []
    for page in range(1, pages + 1):
        url = (f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
               f"?app_id={app_id}&app_key={app_key}"
               f"&results_per_page={results_per_page}"
               f"&what={requests.utils.quote(query)}"
               f"&content-type=application/json")
        resp = _http("GET", url)
        if resp is None:
            break
        data = resp.json() or {}
        results = data.get("results", []) or []
        if not results:
            break
        for r in results:
            out.append({
                "job_title":     r.get("title"),
                "company":       (r.get("company") or {}).get("display_name"),
                "description":   r.get("description"),
                "location":      (r.get("location") or {}).get("display_name"),
                "salary_min":    r.get("salary_min"),
                "salary_max":    r.get("salary_max"),
                "salary_period": "year",   # Adzuna normalises to annual
                "job_url":       r.get("redirect_url"),
                "date_posted":   r.get("created"),
                "source_api":    "adzuna",
            })
        time.sleep(0.3 + random.random() * 0.4)
    log.info("Adzuna  '%-30s'  -> %d rows", query, len(out))
    return out


# ===========================================================================
# 2.  USAJOBS   --   federal-government postings
# ===========================================================================
def fetch_usajobs(query: str, *, pages: int = 10,
                  results_per_page: int = 250) -> list[dict[str, Any]]:
    ua  = os.getenv("USAJOBS_USER_AGENT")
    key = os.getenv("USAJOBS_AUTH_KEY")
    if not (ua and key):
        log.info("USAJobs credentials missing -- skipping.")
        return []

    headers = {
        "Host":              "data.usajobs.gov",
        "User-Agent":        ua,
        "Authorization-Key": key,
    }
    out: list[dict[str, Any]] = []
    for page in range(1, pages + 1):
        url = ("https://data.usajobs.gov/api/search?"
               f"Keyword={requests.utils.quote(query)}"
               f"&Page={page}&ResultsPerPage={results_per_page}")
        resp = _http("GET", url, headers=headers)
        if resp is None:
            break
        data = resp.json() or {}
        items = (data.get("SearchResult") or {}).get("SearchResultItems") or []
        if not items:
            break
        for it in items:
            md = (it.get("MatchedObjectDescriptor") or {})
            pos_loc = (md.get("PositionLocationDisplay") or "")
            ujd = md.get("UserArea", {}).get("Details", {}) if md else {}
            description = ujd.get("JobSummary") or md.get("QualificationSummary") or ""
            remuneration = md.get("PositionRemuneration") or []
            salary_min = salary_max = None
            salary_period = None
            if remuneration:
                rm = remuneration[0]
                try:
                    salary_min = float(rm.get("MinimumRange") or 0) or None
                    salary_max = float(rm.get("MaximumRange") or 0) or None
                    salary_period = (rm.get("RateIntervalCode") or "").lower()
                except (TypeError, ValueError):
                    pass
            out.append({
                "job_title":     md.get("PositionTitle"),
                "company":       (md.get("OrganizationName")
                                  or md.get("DepartmentName")),
                "description":   description,
                "location":      pos_loc,
                "salary_min":    salary_min,
                "salary_max":    salary_max,
                "salary_period": salary_period or "year",
                "job_url":       md.get("PositionURI"),
                "date_posted":   md.get("PublicationStartDate"),
                "source_api":    "usajobs",
            })
        time.sleep(0.5 + random.random() * 0.5)
    log.info("USAJobs '%-30s'  -> %d rows", query, len(out))
    return out


# ===========================================================================
# 3.  JOOBLE   --   POST API, 500/day soft limit on free
# ===========================================================================
def fetch_jooble(query: str, *, pages: int = 10) -> list[dict[str, Any]]:
    key = os.getenv("JOOBLE_API_KEY")
    if not key:
        log.info("Jooble key missing -- skipping.")
        return []

    out: list[dict[str, Any]] = []
    url = f"https://jooble.org/api/{key}"
    for page in range(1, pages + 1):
        payload = {"keywords": query, "location": "USA", "page": page}
        resp = _http("POST", url, json=payload,
                     headers={"Content-Type": "application/json"})
        if resp is None:
            break
        data = resp.json() or {}
        jobs = data.get("jobs") or []
        if not jobs:
            break
        for j in jobs:
            sal_text = (j.get("salary") or "")
            out.append({
                "job_title":     j.get("title"),
                "company":       j.get("company"),
                "description":   j.get("snippet") or "",
                "location":      j.get("location"),
                "salary_min":    None,    # Jooble returns text, parsed downstream
                "salary_max":    None,
                "salary_text":   sal_text,
                "salary_period": None,
                "job_url":       j.get("link"),
                "date_posted":   j.get("updated"),
                "source_api":    "jooble",
            })
        time.sleep(0.5 + random.random() * 0.5)
    log.info("Jooble  '%-30s'  -> %d rows", query, len(out))
    return out


# ===========================================================================
# 4.  THE MUSE   --   no key required; category-driven
# ===========================================================================
_MUSE_CATEGORY_MAP = {
    "Data Analyst": "Data Science",
    "Product Analyst": "Data Science",
    "Marketing Analyst": "Marketing",
    "Business Analyst": "Business & Strategy",
    "Business Intelligence Analyst": "Data Science",
    "Financial Analyst": "Accounting and Finance",
    "Operations Analyst": "Operations",
    "Data Scientist": "Data Science",
}

def fetch_themuse(query: str, *, pages: int = 20) -> list[dict[str, Any]]:
    category = _MUSE_CATEGORY_MAP.get(query)
    if not category:
        return []
    key = os.getenv("THE_MUSE_API_KEY")
    out: list[dict[str, Any]] = []
    for page in range(0, pages):
        url = (f"https://www.themuse.com/api/public/jobs?"
               f"category={requests.utils.quote(category)}&page={page}")
        if key:
            url += f"&api_key={key}"
        resp = _http("GET", url)
        if resp is None:
            break
        data = resp.json() or {}
        results = data.get("results") or []
        if not results:
            break
        for r in results:
            locs = r.get("locations") or [{}]
            location = (locs[0] or {}).get("name") if locs else None
            out.append({
                "job_title":     r.get("name"),
                "company":       (r.get("company") or {}).get("name"),
                "description":   r.get("contents") or "",
                "location":      location,
                "salary_min":    None,
                "salary_max":    None,
                "salary_period": None,
                "job_url":       (r.get("refs") or {}).get("landing_page"),
                "date_posted":   r.get("publication_date"),
                "source_api":    "themuse",
            })
        time.sleep(0.3 + random.random() * 0.4)
    log.info("TheMuse '%-30s'  -> %d rows", query, len(out))
    return out


# ===========================================================================
# 5.  REMOTIVE   --   public; remote-only postings
# ===========================================================================
def fetch_remotive(query: str) -> list[dict[str, Any]]:
    url = (f"https://remotive.com/api/remote-jobs"
           f"?search={requests.utils.quote(query)}")
    resp = _http("GET", url)
    if resp is None:
        return []
    data = resp.json() or {}
    jobs = data.get("jobs") or []
    out: list[dict[str, Any]] = []
    for j in jobs:
        salary_text = j.get("salary") or ""
        out.append({
            "job_title":     j.get("title"),
            "company":       j.get("company_name"),
            "description":   j.get("description") or "",
            "location":      j.get("candidate_required_location") or "Remote",
            "salary_min":    None,
            "salary_max":    None,
            "salary_text":   salary_text,
            "salary_period": None,
            "job_url":       j.get("url"),
            "date_posted":   j.get("publication_date"),
            "source_api":    "remotive",
        })
    log.info("Remotive '%-29s' -> %d rows", query, len(out))
    return out


# ===========================================================================
# 6.  ARBEITNOW   --   public; mostly EU but lots of remote
# ===========================================================================
def fetch_arbeitnow(query: str, *, pages: int = 10) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(1, pages + 1):
        url = f"https://www.arbeitnow.com/api/job-board-api?page={page}"
        resp = _http("GET", url)
        if resp is None:
            break
        data = resp.json() or {}
        jobs = data.get("data") or []
        if not jobs:
            break
        q_low = query.lower()
        for j in jobs:
            title = (j.get("title") or "").lower()
            description = (j.get("description") or "").lower()
            # ArbeitNow doesn't take search params -- filter client-side.
            if q_low not in title and q_low not in description:
                continue
            out.append({
                "job_title":     j.get("title"),
                "company":       j.get("company_name"),
                "description":   j.get("description") or "",
                "location":      j.get("location") or "Remote",
                "salary_min":    None,
                "salary_max":    None,
                "salary_period": None,
                "job_url":       j.get("url"),
                "date_posted":   j.get("created_at"),
                "source_api":    "arbeitnow",
            })
        time.sleep(0.3 + random.random() * 0.4)
    log.info("ArbeitNow '%-28s' -> %d rows", query, len(out))
    return out


# ===========================================================================
# ORCHESTRATION  --  fan out every (api, query) into a ThreadPoolExecutor
# ===========================================================================
ApiFunc = Callable[..., list[dict[str, Any]]]

API_REGISTRY: dict[str, ApiFunc] = {
    "adzuna":    fetch_adzuna,
    "usajobs":   fetch_usajobs,
    "jooble":    fetch_jooble,
    "themuse":   fetch_themuse,
    "remotive":  fetch_remotive,
    "arbeitnow": fetch_arbeitnow,
}


def fetch_all(roles: Iterable[str] = TARGET_ROLES,
              apis: Iterable[str] | None = None,
              max_workers: int = 12) -> list[dict[str, Any]]:
    """
    Fan-out every (api, role) tuple in parallel and return one flat list of
    raw job dicts.
    """
    apis = list(apis) if apis else list(API_REGISTRY.keys())
    tasks: list[tuple[str, str]] = [(a, r) for a in apis for r in roles]
    log.info("Dispatching %d (api, role) tasks across %d workers...",
             len(tasks), max_workers)
    out: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_task = {
            ex.submit(API_REGISTRY[api], role): (api, role)
            for api, role in tasks
        }
        for fut in as_completed(future_to_task):
            api, role = future_to_task[fut]
            try:
                rows = fut.result()
                out.extend(rows)
            except Exception as e:                  # pragma: no cover
                log.exception("Task %s/%s failed: %s", api, role, e)

    log.info("Total raw rows collected: %d", len(out))
    return out
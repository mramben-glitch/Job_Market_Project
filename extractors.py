"""
extractors.py
=============
Offline, pre-compiled regex extraction engine for job market data.
Contains 5 pure functions:
  1. extract_industry
  2. extract_education
  3. extract_benefits
  4. extract_skills (Hard Skills)
  5. extract_soft_skills
"""

from __future__ import annotations

import re
import unicodedata
from typing import Optional


# ═══════════════════════════════════════════════════════════════════════════════
# SHARED UTILITY
# ═══════════════════════════════════════════════════════════════════════════════

def _norm(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace."""
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", text).strip().lower()


# ═══════════════════════════════════════════════════════════════════════════════
# 1.  INDUSTRY EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

_COMPANY_TO_INDUSTRY: dict[str, str] = {
    # ── Technology ──────────────────────────────────────────────────────────
    "google": "Technology", "alphabet": "Technology",
    "meta": "Technology", "facebook": "Technology", "instagram": "Technology",
    "apple": "Technology", "microsoft": "Technology", "amazon": "Technology",
    "netflix": "Technology", "twitter": "Technology", "x corp": "Technology",
    "linkedin": "Technology", "uber": "Technology", "lyft": "Technology",
    "airbnb": "Technology", "snap": "Technology", "pinterest": "Technology",
    "dropbox": "Technology", "box": "Technology", "slack": "Technology",
    "zoom": "Technology", "salesforce": "Technology", "servicenow": "Technology",
    "workday": "Technology", "zendesk": "Technology", "hubspot": "Technology",
    "atlassian": "Technology", "datadog": "Technology", "splunk": "Technology",
    "pagerduty": "Technology", "twilio": "Technology", "okta": "Technology",
    "cloudflare": "Technology", "fastly": "Technology", "mongodb": "Technology",
    "elastic": "Technology", "confluent": "Technology", "snowflake": "Technology",
    "databricks": "Technology", "palantir": "Technology", "c3 ai": "Technology",
    "openai": "Technology", "anthropic": "Technology", "cohere": "Technology",
    "stripe": "Technology", "square": "Technology", "block": "Technology",
    "robinhood": "Technology", "coinbase": "Technology", "plaid": "Technology",
    "chime": "Technology", "brex": "Technology", "rippling": "Technology",
    "gusto": "Technology", "toast": "Technology", "doordash": "Technology",
    "instacart": "Technology", "grubhub": "Technology", "postmates": "Technology",
    "wix": "Technology", "squarespace": "Technology", "shopify": "Technology",
    "bigcommerce": "Technology", "magento": "Technology",
    "ibm": "Technology", "oracle": "Technology", "sap": "Technology",
    "cisco": "Technology", "intel": "Technology", "nvidia": "Technology",
    "amd": "Technology", "qualcomm": "Technology", "broadcom": "Technology",
    "vmware": "Technology", "dell technologies": "Technology",
    "hp inc": "Technology", "hewlett packard": "Technology",
    "lenovo": "Technology", "samsung electronics": "Technology",
    "adobe": "Technology", "autodesk": "Technology", "intuit": "Technology",
    "veeva systems": "Technology", "ansys": "Technology",
    "cadence design": "Technology", "synopsys": "Technology",
    "fortinet": "Technology", "palo alto networks": "Technology",
    "crowdstrike": "Technology", "sentinelone": "Technology",
    "zscaler": "Technology", "cyberark": "Technology",

    # ── Finance / Banking ───────────────────────────────────────────────────
    "goldman sachs": "Finance", "jp morgan": "Finance", "jpmorgan": "Finance",
    "morgan stanley": "Finance", "bank of america": "Finance",
    "wells fargo": "Finance", "citibank": "Finance", "citigroup": "Finance",
    "us bank": "Finance", "pnc": "Finance", "truist": "Finance",
    "capital one": "Finance", "american express": "Finance", "amex": "Finance",
    "discover": "Finance", "synchrony": "Finance", "ally financial": "Finance",
    "charles schwab": "Finance", "fidelity": "Finance",
    "vanguard": "Finance", "blackrock": "Finance", "state street": "Finance",
    "t rowe price": "Finance", "invesco": "Finance", "pimco": "Finance",
    "kkr": "Finance", "carlyle": "Finance", "apollo": "Finance",
    "blackstone": "Finance", "ares management": "Finance",
    "visa": "Finance", "mastercard": "Finance", "paypal": "Finance",
    "western union": "Finance", "moneygram": "Finance",
    "nasdaq": "Finance", "nyse": "Finance", "intercontinental exchange": "Finance",
    "moodys": "Finance", "s&p global": "Finance", "msci": "Finance",
    "factset": "Finance", "bloomberg": "Finance",
    "raymond james": "Finance", "edward jones": "Finance",
    "ameriprise": "Finance", "lpl financial": "Finance",
    "nerdwallet": "Finance", "sofi": "Finance", "lending club": "Finance",
    "affirm": "Finance", "klarna": "Finance", "upstart": "Finance",
    "fannie mae": "Finance", "freddie mac": "Finance", "sallie mae": "Finance",
    "federal reserve": "Finance",

    # ── Healthcare / Pharma ─────────────────────────────────────────────────
    "unitedhealth": "Healthcare", "united health": "Healthcare",
    "anthem": "Healthcare", "aetna": "Healthcare", "cigna": "Healthcare",
    "humana": "Healthcare", "cvs health": "Healthcare", "cvs": "Healthcare",
    "kaiser permanente": "Healthcare", "hca healthcare": "Healthcare",
    "centene": "Healthcare", "molina healthcare": "Healthcare",
    "elevance health": "Healthcare",
    "pfizer": "Healthcare", "johnson & johnson": "Healthcare",
    "merck": "Healthcare", "abbvie": "Healthcare", "bristol myers squibb": "Healthcare",
    "eli lilly": "Healthcare", "amgen": "Healthcare", "biogen": "Healthcare",
    "gilead": "Healthcare", "regeneron": "Healthcare", "moderna": "Healthcare",
    "biontech": "Healthcare", "astrazeneca": "Healthcare",
    "novartis": "Healthcare", "roche": "Healthcare", "genentech": "Healthcare",
    "baxter": "Healthcare", "becton dickinson": "Healthcare",
    "medtronic": "Healthcare", "stryker": "Healthcare", "zimmer biomet": "Healthcare",
    "boston scientific": "Healthcare", "abbott": "Healthcare",
    "quest diagnostics": "Healthcare", "labcorp": "Healthcare",
    "mckesson": "Healthcare", "amerisourcebergen": "Healthcare",
    "cardinal health": "Healthcare",

    # ── Retail / Consumer Goods ──────────────────────────────────────────────
    "walmart": "Retail", "target": "Retail", "costco": "Retail",
    "kroger": "Retail", "albertsons": "Retail", "whole foods": "Retail",
    "trader joes": "Retail", "dollar general": "Retail", "dollar tree": "Retail",
    "best buy": "Retail", "home depot": "Retail", "lowes": "Retail",
    "ikea": "Retail", "wayfair": "Retail", "chewy": "Retail",
    "overstock": "Retail", "etsy": "Retail", "ebay": "Retail",
    "gap": "Retail", "h&m": "Retail", "zara": "Retail",
    "nike": "Retail", "adidas": "Retail", "under armour": "Retail",
    "lululemon": "Retail", "nordstrom": "Retail", "macys": "Retail",
    "kohls": "Retail", "tj maxx": "Retail", "marshalls": "Retail",
    "procter & gamble": "Consumer Goods", "p&g": "Consumer Goods",
    "unilever": "Consumer Goods", "colgate": "Consumer Goods",
    "henkel": "Consumer Goods", "kimberly clark": "Consumer Goods",
    "church & dwight": "Consumer Goods",
    "coca cola": "Consumer Goods", "pepsico": "Consumer Goods",
    "nestle": "Consumer Goods", "kraft heinz": "Consumer Goods",
    "general mills": "Consumer Goods", "kelloggs": "Consumer Goods",
    "conagra": "Consumer Goods", "mondelez": "Consumer Goods",
    "hershey": "Consumer Goods", "campbell soup": "Consumer Goods",

    # ── Media / Entertainment ────────────────────────────────────────────────
    "disney": "Media & Entertainment", "warner bros": "Media & Entertainment",
    "warnermedia": "Media & Entertainment", "comcast": "Media & Entertainment",
    "nbc universal": "Media & Entertainment", "paramount": "Media & Entertainment",
    "cbs": "Media & Entertainment", "fox": "Media & Entertainment",
    "hbo": "Media & Entertainment", "hulu": "Media & Entertainment",
    "spotify": "Media & Entertainment", "pandora": "Media & Entertainment",
    "iheartmedia": "Media & Entertainment", "sirius xm": "Media & Entertainment",
    "new york times": "Media & Entertainment", "washington post": "Media & Entertainment",
    "buzzfeed": "Media & Entertainment", "vox media": "Media & Entertainment",
    "conde nast": "Media & Entertainment", "hearst": "Media & Entertainment",
    "ea": "Media & Entertainment", "electronic arts": "Media & Entertainment",
    "activision": "Media & Entertainment", "blizzard": "Media & Entertainment",
    "take-two": "Media & Entertainment", "roblox": "Media & Entertainment",
    "unity": "Media & Entertainment", "epic games": "Media & Entertainment",

    # ── Consulting / Professional Services ──────────────────────────────────
    "mckinsey": "Consulting", "bcg": "Consulting",
    "boston consulting group": "Consulting", "bain": "Consulting",
    "deloitte": "Consulting", "pwc": "Consulting",
    "pricewaterhousecoopers": "Consulting",
    "ernst & young": "Consulting", "ey": "Consulting",
    "kpmg": "Consulting", "accenture": "Consulting",
    "capgemini": "Consulting", "infosys": "Consulting",
    "tcs": "Consulting", "tata consultancy": "Consulting",
    "wipro": "Consulting", "cognizant": "Consulting",
    "booz allen": "Consulting", "leidos": "Consulting",
    "saic": "Consulting", "mitre": "Consulting",
    "oliver wyman": "Consulting", "roland berger": "Consulting",

    # ── Telecommunications ───────────────────────────────────────────────────
    "at&t": "Telecommunications", "verizon": "Telecommunications",
    "t-mobile": "Telecommunications", "sprint": "Telecommunications",
    "dish": "Telecommunications", "charter": "Telecommunications",
    "lumen": "Telecommunications", "centurylink": "Telecommunications",
    "cox communications": "Telecommunications", "frontier": "Telecommunications",

    # ── Transportation / Logistics ───────────────────────────────────────────
    "fedex": "Transportation & Logistics", "ups": "Transportation & Logistics",
    "dhl": "Transportation & Logistics", "usps": "Transportation & Logistics",
    "xpo logistics": "Transportation & Logistics",
    "jb hunt": "Transportation & Logistics",
    "ch robinson": "Transportation & Logistics",
    "werner enterprises": "Transportation & Logistics",
    "ryder": "Transportation & Logistics", "penske": "Transportation & Logistics",
    "delta": "Transportation & Logistics", "united airlines": "Transportation & Logistics",
    "american airlines": "Transportation & Logistics",
    "southwest airlines": "Transportation & Logistics",
    "jetblue": "Transportation & Logistics",

    # ── Energy ──────────────────────────────────────────────────────────────
    "exxonmobil": "Energy", "chevron": "Energy", "conocophillips": "Energy",
    "pioneer natural": "Energy", "halliburton": "Energy",
    "schlumberger": "Energy", "baker hughes": "Energy",
    "nextera energy": "Energy", "duke energy": "Energy",
    "southern company": "Energy", "dominion energy": "Energy",
    "sempra": "Energy", "pge": "Energy",
    "firstenergy": "Energy", "constellation energy": "Energy",
    "bp": "Energy", "shell": "Energy",

    # ── Real Estate ──────────────────────────────────────────────────────────
    "cbre": "Real Estate", "jll": "Real Estate",
    "jones lang lasalle": "Real Estate",
    "cushman & wakefield": "Real Estate",
    "colliers": "Real Estate", "redfin": "Real Estate",
    "zillow": "Real Estate", "realtor.com": "Real Estate",
    "compass": "Real Estate", "keller williams": "Real Estate",
    "prologis": "Real Estate", "simon property group": "Real Estate",

    # ── Government / Education / Non-profit ─────────────────────────────────
    "red cross": "Non-profit", "ymca": "Non-profit",
    "united way": "Non-profit", "goodwill": "Non-profit",
    "mit": "Education", "stanford university": "Education",
    "harvard university": "Education", "coursera": "Education",
    "udemy": "Education", "chegg": "Education",
    "pearson": "Education", "mcgraw hill": "Education",

    # ── Automotive ──────────────────────────────────────────────────────────
    "ford": "Automotive", "general motors": "Automotive", "gm": "Automotive",
    "stellantis": "Automotive", "chrysler": "Automotive",
    "tesla": "Automotive", "rivian": "Automotive", "lucid motors": "Automotive",
    "carvana": "Automotive", "autotrader": "Automotive",
    "carmax": "Automotive",

    # ── Staffing ────────────────────────────────────────────────────────────
    "manpower": "Staffing", "adecco": "Staffing",
    "randstad": "Staffing", "robert half": "Staffing",
    "kelly services": "Staffing", "insight global": "Staffing",
    "aerotek": "Staffing", "kforce": "Staffing",
    "indeed": "Staffing", "glassdoor": "Staffing",
    "ziprecruiter": "Staffing",
}

_INDUSTRY_KEYWORDS_RAW: list[tuple[str, list[str]]] = [
    ("Technology", [
        r"software\s+(?:engineer|develop|product|platform|infrastructure)",
        r"cloud\s+(?:platform|services|infrastructure|native)",
        r"machine\s+learning\s+platform", r"data\s+(?:platform|infrastructure)",
        r"api\s+integrat", r"microservices", r"devops", r"ci/cd",
        r"\bsaas\b", r"\bpaas\b", r"\biaas\b",
        r"\bkubernetes\b", r"\bdocker\b", r"\bterraform\b",
        r"full[\s\-]?stack", r"mobile\s+app(?:lication)?",
        r"cybersecurity\s+(?:platform|firm|company)",
        r"digital\s+transformation\s+(?:company|firm|partner)",
    ]),
    ("Finance", [
        r"investment\s+(?:bank|management|portfolio|strategy|grade)",
        r"asset\s+management", r"wealth\s+management", r"hedge\s+fund",
        r"private\s+equity", r"venture\s+capital",
        r"financial\s+services\s+(?:firm|company|industry)",
        r"trading\s+(?:desk|floor|platform|strategy)",
        r"fixed\s+income", r"equity\s+(?:research|trading|market)",
        r"\bderivatives\b", r"options\s+pricing",
        r"quantitative\s+(?:finance|trading|research)",
        r"\baml\b", r"anti[\s\-]money\s+laundering",
        r"credit\s+(?:risk|scoring|underwriting|card)",
        r"insurance\s+(?:underwriting|claims|policy|actuarial)",
        r"actuarial\s+(?:science|model)", r"\btreasury\b",
        r"\bfintech\b", r"payment\s+processing", r"card\s+network",
        r"\bmortgage\b", r"lending\s+platform", r"loan\s+origination",
    ]),
    ("Healthcare", [
        r"clinical\s+(?:trial|data|research|operations|outcome)",
        r"patient\s+(?:data|outcome|care|record|journey|engagement)",
        r"electronic\s+health\s+record", r"\behr\b", r"\bemr\b",
        r"\bhipaa\b", r"\bhl7\b", r"\bfhir\b",
        r"\bpharmaceutical\b", r"drug\s+(?:discovery|development|approval)",
        r"medical\s+(?:device|imaging|record|billing)",
        r"health\s+(?:plan|system|network|informatics)",
        r"biotech(?:nology)?", r"\bgenomics\b", r"\bproteomics\b",
        r"hospital\s+(?:system|network|operations)",
        r"revenue\s+cycle\s+management",
        r"population\s+health", r"\btelehealth\b", r"digital\s+health",
    ]),
    ("Retail", [
        r"e[\s\-]?commerce\s+(?:platform|company|business)",
        r"retail\s+(?:company|chain|industry|operations|analytics)",
        r"merchandise\s+(?:planning|assortment|analytics)",
        r"store\s+(?:operations|performance|analytics)",
        r"category\s+management", r"\bplanogram\b",
        r"loyalty\s+(?:program|analytics)", r"basket\s+analysis",
        r"price\s+elasticity", r"markdown\s+optimization",
    ]),
    ("Consumer Goods", [
        r"\bcpg\b", r"\bfmcg\b",
        r"brand\s+(?:analytics|management|performance|marketing)",
        r"trade\s+(?:promotion|spend|analytics)",
        r"shopper\s+(?:insights|analytics|marketing)",
        r"market\s+share\s+analysis",
        r"new\s+product\s+(?:launch|development|innovation)",
        r"consumer\s+(?:insights|panel|research|behavior)",
    ]),
    ("Media & Entertainment", [
        r"streaming\s+(?:platform|service|analytics|content)",
        r"content\s+(?:strategy|analytics|monetization|platform)",
        r"audience\s+(?:analytics|measurement|engagement|segmentation)",
        r"advertising\s+technology", r"\badtech\b", r"\bprogrammatic\b",
        r"media\s+(?:buying|planning|analytics|company)",
        r"video\s+(?:game|gaming|streaming|platform)",
        r"publishing\s+(?:platform|industry|analytics)",
        r"music\s+(?:streaming|analytics)",
        r"social\s+media\s+analytics", r"influencer\s+analytics",
    ]),
    ("Consulting", [
        r"management\s+consulting",
        r"strategy\s+consulting",
        r"advisory\s+(?:firm|services|practice)",
        r"professional\s+services\s+firm",
        r"consulting\s+(?:firm|practice|engagement|client)",
        r"client[\s\-]facing\s+(?:analytics|deliverable|presentation)",
        r"\bengagement\s+manager\b",
    ]),
    ("Telecommunications", [
        r"wireless\s+(?:network|carrier|subscriber)",
        r"broadband\s+(?:network|provider)",
        r"5g\s+(?:network|rollout|technology)",
        r"network\s+(?:operations|analytics|infrastructure)",
        r"\barpu\b", r"average\s+revenue\s+per\s+user",
        r"subscriber\s+(?:analytics|growth|retention)",
    ]),
    ("Transportation & Logistics", [
        r"supply\s+chain\s+(?:logistics|optimization|management)",
        r"last[\s\-]mile\s+(?:delivery|logistics)",
        r"fleet\s+(?:management|analytics|operations)",
        r"route\s+optimization",
        r"warehouse\s+(?:management|operations|analytics)",
        r"freight\s+(?:analytics|brokerage|forwarding)",
        r"logistics\s+(?:platform|network|operations|analytics)",
        r"transportation\s+(?:management|analytics)",
    ]),
    ("Energy", [
        r"oil\s+(?:and|&)\s+gas",
        r"renewable\s+energy", r"solar\s+(?:energy|power|analytics)",
        r"wind\s+(?:energy|power|farm)",
        r"energy\s+(?:analytics|trading|grid|storage|market)",
        r"utility\s+(?:company|analytics|operations)",
        r"\bsmart\s+grid\b", r"\bpower\s+plant\b",
    ]),
    ("Real Estate", [
        r"real\s+estate\s+(?:analytics|investment|platform|data)",
        r"property\s+(?:management|analytics|valuation)",
        r"commercial\s+real\s+estate",
        r"residential\s+(?:real\s+estate|lending)",
        r"\breit\b", r"\bcap\s+rate\b",
        r"occupancy\s+(?:analytics|rate|forecast)",
        r"lease\s+(?:analytics|management|abstraction)",
        r"\bproptech\b",
    ]),
    ("Government", [
        r"federal\s+(?:agency|government|contract)",
        r"government\s+(?:agency|contract|sector)",
        r"public\s+sector\s+(?:analytics|data)",
        r"department\s+of\s+(?:defense|health|commerce|transportation)",
        r"security\s+clearance\s+(?:required|preferred)",
        r"top\s+secret\s+(?:clearance|sci)",
    ]),
    ("Education", [
        r"higher\s+education\s+(?:institution|analytics|data)",
        r"student\s+(?:outcomes|analytics|success|data)",
        r"\bedtech\b", r"learning\s+(?:management|analytics|platform)",
        r"academic\s+(?:analytics|research|institution)",
        r"k[\s\-]?12\s+(?:education|analytics)",
    ]),
]

_INDUSTRY_KEYWORD_CLUSTERS: list[tuple[str, re.Pattern]] = []
for _ind, _pats in _INDUSTRY_KEYWORDS_RAW:
    _combined = "|".join(f"(?:{p})" for p in _pats)
    _INDUSTRY_KEYWORD_CLUSTERS.append((_ind, re.compile(_combined, re.IGNORECASE)))

def extract_industry(company_name: Optional[str], description: str = "") -> Optional[str]:
    """Classify a job into an industry string."""
    if company_name:
        cn = _norm(company_name)
        if cn in _COMPANY_TO_INDUSTRY:
            return _COMPANY_TO_INDUSTRY[cn]
        for key, industry in _COMPANY_TO_INDUSTRY.items():
            if key in cn:
                return industry

    if description:
        for industry, pattern in _INDUSTRY_KEYWORD_CLUSTERS:
            if pattern.search(description):
                return industry

    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 2.  EDUCATION EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

_EDUCATION_PATTERNS: list[tuple[str, re.Pattern]] = [
    ("PhD", re.compile(
        r"\b(?:ph\.?d\.?(?:\s+(?:in|or|degree))?|d\.?sc\.?|doctor(?:al|ate)(?:\s+(?:degree|student|candidate))?|doctoral\s+degree)\b",
        re.IGNORECASE
    )),
    ("Master's", re.compile(
        r"\b(?:m\.?s\.?(?:\s+(?:in|or|degree))?|m\.?a\.?(?:\s+(?:in|or|degree))?|m\.?eng\.?|m\.?b\.?a\.?|mba\b|master(?:s|'s)?(?:\s+(?:degree|of|in))?|post[\s\-]?graduate\s+degree|graduate\s+degree)\b",
        re.IGNORECASE
    )),
    ("Bachelor's", re.compile(
        r"\b(?:b\.?a\.?(?:\s+(?:in|or|degree))?|b\.?s\.?(?:\s+(?:in|or|degree))?|b\.?eng\.?|b\.?sc\.?|bachelor(?:s|'s)?(?:\s+(?:degree|of|in))?|undergraduate\s+degree|4[\s\-]year\s+degree|four[\s\-]year\s+degree|degree\s+in\s+(?:a\s+)?(?:quantitative|technical|stem|related|relevant|analytical|business|computer|science|engineering|statistics|math|finance|economics|accounting|information\s+systems)|(?:stem|quantitative|technical)\s+(?:background|degree|field|discipline)|related\s+bachelor(?:'?s)?|college\s+degree|university\s+degree|\bbachelor\b)\b",
        re.IGNORECASE
    )),
]

def extract_education(description: Optional[str]) -> Optional[str]:
    """Return the HIGHEST required/preferred degree found in the text."""
    if not description:
        return None
    for degree, pattern in _EDUCATION_PATTERNS:
        if pattern.search(description):
            return degree
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# 3.  BENEFITS EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

_BENEFITS_RAW: list[tuple[str, list[str]]] = [
    ("401(k)", [
        r"401\s*\(?k\)?", r"403\s*\(?b\)?", r"retirement\s+(?:plan|savings|benefit|contribution|account|fund)",
        r"employer\s+(?:match|matching|contribution)\b", r"pension\s+(?:plan|benefit|fund)",
        r"defined\s+(?:contribution|benefit)\s+plan", r"roth\s+(?:ira|401)", r"profit[\s\-]?sharing\s+plan",
        r"deferred\s+compensation", r"save\s+for\s+retirement",
    ]),
    ("Health Insurance", [
        r"health\s+(?:insurance|coverage|benefits?|plan|care|savings\s+account)", r"medical\s+(?:insurance|coverage|benefits?|plan)",
        r"dental\s+(?:insurance|coverage|benefits?|plan)", r"vision\s+(?:insurance|coverage|benefits?|plan|care)",
        r"\bhsa\b", r"\bfsa\b", r"health\s+savings\s+account", r"flexible\s+spending\s+account",
        r"comprehensive\s+(?:health|medical|benefits?)", r"employee\s+(?:health|medical|wellness)\s+(?:plan|program|benefits?)",
        r"prescription\s+(?:drug|benefit|coverage)", r"telehealth\s+benefit", r"mental\s+health\s+(?:benefit|coverage|support|program)",
        r"behavioral\s+health\s+(?:benefit|coverage)", r"wellness\s+(?:program|benefit|reimbursement|stipend)",
        r"employee\s+assistance\s+program", r"\beap\b(?=[\s,.]|$)",
    ]),
    ("PTO", [
        r"paid\s+(?:time\s+off|vacation|leave|holidays?|sick\s+(?:days?|leave)|personal\s+days?)",
        r"unlimited\s+(?:pto|vacation|time\s+off|paid\s+time)", r"flexible\s+(?:pto|time\s+off|vacation|leave)",
        r"generous\s+(?:pto|vacation|time\s+off|leave|paid\s+time\s+off)", r"\bpto\b",
        r"vacation\s+(?:days?|weeks?|time|accrual|policy)", r"annual\s+leave", r"sick\s+(?:days?|leave|time)",
        r"personal\s+days?", r"holidays?\s+(?:off|pay|observed|schedule)", r"time\s+away\s+from\s+work",
        r"time\s+off\s+(?:policy|program|benefits?)", r"parental\s+leave", r"maternity\s+leave", r"paternity\s+leave",
        r"family\s+leave", r"bereavement\s+leave", r"\bsabbatical\b",
    ]),
    ("Bonus", [
        r"(?:performance|annual|quarterly|monthly|year[\s\-]?end|signing|sign[\s\-]?on|retention|spot|referral)\s+bonus(?:es)?",
        r"bonus\s+(?:program|plan|eligible|eligibility|potential|opportunity|structure)", r"incentive\s+(?:pay|compensation|plan|program|bonus)",
        r"commission\s+(?:plan|structure|based)", r"target\s+bonus", r"bonus\s+up\s+to", r"cash\s+incentive",
        r"variable\s+(?:pay|compensation)\b", r"merit\s+(?:increase|raise|bonus)",
    ]),
    ("Stock / Equity", [
        r"(?:stock|equity|share)\s+(?:options?|grants?|awards?|plan|program|compensation|incentive|vesting|purchase)",
        r"restricted\s+stock\s+units?", r"\brsus?\b", r"employee\s+stock\s+(?:purchase\s+plan|options?)",
        r"\bespp\b", r"equity\s+(?:compensation|package|stake|vesting)", r"long[\s\-]?term\s+incentive",
        r"\bltip?\b", r"profit\s+(?:sharing|participation)", r"vesting\s+schedule",
    ]),
    ("Remote / Flexible Work", [
        r"remote\s+work\s+(?:option|flexibility|stipend|allowance|reimbursement)",
        r"work[\s\-]from[\s\-]home\s+(?:flexibility|option|allowance|stipend)",
        r"flexible\s+work(?:ing)?\s+(?:hours?|schedule|arrangements?|options?)",
        r"hybrid\s+(?:work|schedule|model)\s+(?:option|flexibility|arrangement)?",
        r"home\s+office\s+(?:stipend|allowance|reimbursement|setup)", r"internet\s+(?:stipend|reimbursement|allowance)",
        r"equipment\s+(?:provided|stipend|allowance|reimbursement)",
    ]),
    ("Learning & Development", [
        r"tuition\s+(?:reimbursement|assistance|benefit|program|repayment|waiver)",
        r"education(?:al)?\s+(?:reimbursement|assistance|benefit|program|allowance)",
        r"professional\s+development\s+(?:budget|stipend|program|allowance|opportunities?)",
        r"learning\s+(?:and|&)\s+development\s+(?:program|budget|opportunities?)",
        r"certif(?:ication|icate)\s+(?:reimbursement|support|program|assistance)",
        r"conference\s+(?:attendance|budget|reimbursement)", r"training\s+(?:budget|programs?|opportunities?|resources?)",
        r"mentorship\s+program", r"continuing\s+education", r"student\s+loan\s+(?:repayment|assistance|benefit)",
    ]),
    ("Commuter Benefits", [
        r"commuter\s+(?:benefits?|allowance|reimbursement|subsidy|stipend)", r"transit\s+(?:benefits?|pass|allowance|reimbursement|stipend)",
        r"parking\s+(?:paid|free|provided|covered|reimbursed|subsidized|benefits?|stipend|allowance)",
        r"transportation\s+(?:benefits?|allowance|reimbursement|subsidy)", r"pre[\s\-]?tax\s+transit",
    ]),
]

_BENEFITS_BUCKETS: list[tuple[str, re.Pattern]] = []
for _ben_name, _ben_pats in _BENEFITS_RAW:
    _combined = "|".join(f"(?:{p})" for p in _ben_pats)
    _BENEFITS_BUCKETS.append((_ben_name, re.compile(_combined, re.IGNORECASE)))

def extract_benefits(description: Optional[str]) -> Optional[str]:
    """Return a comma-separated string of benefits found in the description."""
    if not description:
        return None
    found: list[str] = []
    for canonical, pattern in _BENEFITS_BUCKETS:
        if pattern.search(description):
            found.append(canonical)
    return ", ".join(found) if found else None


# ═══════════════════════════════════════════════════════════════════════════════
# 4.  SKILLS EXTRACTION  (Hard Skills)
# ═══════════════════════════════════════════════════════════════════════════════

_R_SKILL_PATTERN = re.compile(r"(?<![A-Za-z])R(?![A-Za-z])")

_SKILLS_RAW: list[tuple[str, list[str]]] = [
    # ── Core Languages ────────────────────────────────────────────────────────
    ("Python",          [r"\bpython\b"]),
    ("SQL",             [r"\bsql\b", r"structured\s+query\s+language"]),
    ("Scala",           [r"\bscala\b"]),
    ("Java",            [r"\bjava\b(?!\s*script)"]),
    ("JavaScript",      [r"\bjavascript\b"]),
    ("TypeScript",      [r"\btypescript\b"]),
    ("Go",              [r"\bgolang\b", r"\bgo\s+(?:language|developer|engineer)\b"]),
    ("Rust",            [r"\brust\b(?:\s+programming|\s+language)?"]),
    ("C++",             [r"\bc\+\+\b", r"\bcpp\b"]),
    ("MATLAB",          [r"\bmatlab\b"]),
    ("SAS",             [r"\bsas\b(?:\s+programming|\s+analytics|\s+studio)?"]),
    ("SPSS",            [r"\bspss\b"]),
    ("Stata",           [r"\bstata\b"]),
    ("Julia",           [r"\bjulia\b(?:\s+programming|\s+language)?"]),
    ("Shell / Bash",    [r"\bbash\b", r"\bshell\s+script", r"\bpowershell\b"]),

    # ── Excel & Spreadsheet ───────────────────────────────────────────────────
    ("Excel",               [r"\bexcel\b", r"microsoft\s+excel", r"ms\s+excel"]),
    ("Advanced Excel",      [r"advanced\s+excel", r"excel\s+advanced"]),
    ("VBA",                 [r"\bvba\b", r"visual\s+basic\s+for\s+applications"]),
    ("Power Query",         [r"power\s*query", r"\bm\s+language\b"]),
    ("DAX",                 [r"\bdax\b"]),
    ("MDX",                 [r"\bmdx\b"]),
    ("Power Pivot",         [r"power\s*pivot"]),
    ("VLOOKUP",             [r"\bv?hlookup\b", r"\bvlookup\b"]),
    ("XLOOKUP",             [r"\bxlookup\b"]),
    ("INDEX MATCH",         [r"\bindex[\s/]+match\b"]),
    ("Pivot Tables",        [r"pivot\s+table", r"pivottable"]),
    ("Conditional Formatting", [r"conditional\s+formatting"]),
    ("Macros",              [r"\bmacros?\b(?!\s*economics)"]),
    ("Excel Dashboards",    [r"excel\s+dashboard"]),
    ("Google Sheets",       [r"google\s+sheets?"]),

    # ── BI & Visualization ────────────────────────────────────────────────────
    ("Tableau",         [r"\btableau\b"]),
    ("Power BI",        [r"power[\s\-]?bi\b"]),
    ("Looker",          [r"\blooker\b(?!\s+studio)"]),
    ("Looker Studio",   [r"looker\s+studio", r"google\s+data\s+studio", r"data\s+studio\b"]),
    ("LookML",          [r"\blookml\b"]),
    ("Qlik",            [r"qlik(?:view|sense|\.com)?"]),
    ("Metabase",        [r"\bmetabase\b"]),
    ("Grafana",         [r"\bgrafana\b"]),
    ("Superset",        [r"apache\s*superset", r"\bsuperset\b"]),
    ("Sisense",         [r"\bsisense\b"]),
    ("MicroStrategy",   [r"\bmicrostrategy\b"]),
    ("TIBCO Spotfire",  [r"\bspotfire\b"]),
    ("SAP BusinessObjects", [r"sap\s*bo\b", r"business\s*objects"]),
    ("Domo",            [r"\bdomo\b"]),
    ("ThoughtSpot",     [r"\bthoughtspot\b"]),
    ("Mode Analytics",  [r"\bmode\s+analytics\b"]),
    ("D3.js",           [r"\bd3\.js\b"]),
    ("Plotly Dash",     [r"\bplotly\s+dash\b"]),
    ("Streamlit",       [r"\bstreamlit\b"]),

    # ── Databases & Querying ──────────────────────────────────────────────────
    ("PostgreSQL",      [r"\bpostgres(?:ql)?\b"]),
    ("MySQL",           [r"\bmysql\b"]),
    ("SQL Server",      [r"\bsql\s+server\b", r"\bmssql\b", r"\bt[\s\-]sql\b", r"\btsql\b"]),
    ("Oracle DB",       [r"\boracle\b(?:\s*db|\s*database|\s*sql)?"]),
    ("SQLite",          [r"\bsqlite\b"]),
    ("BigQuery",        [r"\bbig\s*query\b", r"\bgbq\b"]),
    ("Snowflake",       [r"\bsnowflake\b"]),
    ("Redshift",        [r"\bredshift\b"]),
    ("Databricks",      [r"\bdatabricks\b"]),
    ("Teradata",        [r"\bteradata\b"]),
    ("Hive",            [r"\bhive(?:ql)?\b"]),
    ("Presto / Trino",  [r"\bpresto\b", r"\btrino\b"]),
    ("Athena",          [r"\bamazon\s+athena\b"]),
    ("MongoDB",         [r"\bmongo(?:db)?\b"]),
    ("Redis",           [r"\bredis\b"]),
    ("Cassandra",       [r"\bcassandra\b"]),
    ("DynamoDB",        [r"\bdynamo\s*db\b"]),
    ("Elasticsearch",   [r"\belasticsearch\b", r"\belastic\s+search\b"]),
    ("Neo4j",           [r"\bneo4j\b"]),
    ("NoSQL",           [r"\bnosql\b"]),
    ("dbt",             [r"\bdbt\b", r"data\s+build\s+tool"]),

    # ── Cloud Platforms ───────────────────────────────────────────────────────
    ("AWS",             [r"\baws\b", r"amazon\s+web\s+services"]),
    ("Azure",           [r"microsoft\s+azure\b", r"\bazure\b"]),
    ("GCP",             [r"\bgcp\b", r"google\s+cloud(?:\s+platform)?"]),
    ("AWS S3",          [r"\bamazon\s+s3\b", r"\baws\s+s3\b", r"\bs3\s+bucket"]),
    ("AWS Glue",        [r"\baws\s+glue\b", r"\bamazon\s+glue\b"]),
    ("Azure Data Factory", [r"\bazure\s+data\s+factory\b", r"\badf\b(?=\s+pipeline)"]),
    ("Azure Synapse",   [r"\bazure\s+synapse\b"]),
    ("Spark",           [r"\bapache\s+spark\b", r"\bpyspark\b", r"\bspark\s+(?:sql|streaming|ml)\b"]),
    ("Kafka",           [r"\bapache\s+kafka\b", r"\bkafka\s+(?:stream|topic|consumer|producer)\b"]),
    ("Hadoop",          [r"\bhadoop\b"]),
    ("Flink",           [r"\bapache\s+flink\b"]),
    ("Delta Lake",      [r"\bdelta\s+lake\b"]),

    # ── ETL / Data Pipeline / Orchestration ──────────────────────────────────
    ("Airflow",         [r"\bairflow\b", r"\bapache\s+airflow\b"]),
    ("Fivetran",        [r"\bfivetran\b"]),
    ("Stitch",          [r"\bstitch\s+(?:data|etl)?\b"]),
    ("Informatica",     [r"\binformatica\b"]),
    ("Talend",          [r"\btalend\b"]),
    ("SSIS",            [r"\bssis\b"]),
    ("Prefect",         [r"\bprefect\b"]),
    ("Matillion",       [r"\bmatillion\b"]),
    ("DataStage",       [r"\bdatastage\b"]),

    # ── Statistics & Analytics ────────────────────────────────────────────────
    ("Statistics",          [r"\bstatistics\b", r"\bstatistical\s+(?:analysis|modeling|methods?)\b"]),
    ("Probability",         [r"\bprobability\b", r"\bprobabilistic\s+model"]),
    ("A/B Testing",         [r"a\s*/\s*b\s+test(?:ing)?", r"\bsplit\s+test(?:ing)?", r"hypothesis\s+test(?:ing)?", r"experiment(?:ation)?\s+(?:platform|framework|design)", r"randomized\s+controlled\s+trial"]),
    ("Regression Analysis", [r"(?:linear|logistic|multiple|polynomial|stepwise)\s+regression", r"regression\s+(?:analysis|model(?:ing)?)"]),
    ("Time Series",         [r"time[\s\-]+series\b", r"\barima\b", r"\bsarima\b", r"\bprophet\b", r"\bsarimax\b"]),
    ("Forecasting",         [r"\bforecasting\b", r"demand\s+forecast(?:ing)?"]),
    ("Bayesian Analysis",   [r"\bbayesian\b", r"\bbayes(?:ian)?\s+(?:model|inference|network|statistic)", r"\bmarkov\s+chain\b", r"\bmcmc\b"]),
    ("Clustering",          [r"\bclustering\b", r"\bk[\s\-]?means\b", r"\bdbscan\b", r"\bhierarchical\s+cluster"]),
    ("Cohort Analysis",     [r"\bcohort\s+analysis\b"]),
    ("Funnel Analysis",     [r"\bfunnel\s+analysis\b", r"\bconversion\s+funnel\b"]),
    ("Causal Inference",    [r"\bcausal\s+(?:inference|analysis|model)\b", r"\bdifference[\s\-]in[\s\-]differences\b", r"\bregression\s+discontinuity\b", r"\binstrumental\s+variable\b"]),
    ("Survival Analysis",   [r"\bsurvival\s+analysis\b", r"\bchurn\s+model(?:ing)?", r"\bhazard\s+(?:model|ratio)\b", r"\bkaplan[\s\-]meier\b"]),
    ("Multivariate Analysis",[r"\bmultivariate\s+(?:analysis|regression|statistics)\b", r"\bmanova\b", r"\banova\b", r"\bfactor\s+analysis\b"]),
    ("Monte Carlo",         [r"\bmonte\s+carlo\b"]),
    ("Optimization",        [r"\b(?:linear|integer|mathematical|convex)\s+(?:programming|optimization)\b", r"\boperations\s+research\b"]),
    ("PCA",                 [r"\bpca\b", r"\bprincipal\s+component\b", r"\bdimensionality\s+reduction\b"]),
    ("Statistical Modeling",[r"\bstatistical\s+model(?:ing)?\b", r"\bpredictive\s+model(?:ing)?\b"]),

    # ── Machine Learning & AI ─────────────────────────────────────────────────
    ("Machine Learning",    [r"\bmachine\s+learning\b", r"\bml\s+(?:model|pipeline|platform|engineer|ops)\b", r"\bpredictive\s+analytics\b"]),
    ("Deep Learning",       [r"\bdeep\s+learning\b", r"\bneural\s+net(?:work)?\b"]),
    ("NLP",                 [r"\bnlp\b", r"\bnatural\s+language\s+processing\b", r"\btext\s+(?:analytics|mining|classification|extraction)\b", r"\bsentiment\s+analysis\b"]),
    ("Computer Vision",     [r"\bcomputer\s+vision\b", r"\bimage\s+(?:recognition|classification|segmentation|detection)\b", r"\bobject\s+detection\b"]),
    ("Generative AI",       [r"\bgenerative\s+ai\b", r"\bgen(?:erative)?\s*ai\b", r"\bllm\b", r"\blarge\s+language\s+model\b", r"\bchatgpt\b", r"\bgpt[\s\-]?4?\b", r"\bprompt\s+engineering\b", r"\brag\b(?=\s+(?:pipeline|architecture|system))"]),
    ("Recommendation Systems",[r"\brecommendation\s+(?:system|engine|model|algorithm)\b", r"\bcollaborative\s+filtering\b"]),
    ("Feature Engineering", [r"\bfeature\s+(?:engineering|selection|extraction|importance|store)\b"]),
    ("Model Validation",    [r"\bmodel\s+validat(?:ion|ing)\b", r"\bcross[\s\-]+validation\b", r"\bmodel\s+evaluation\b", r"\bbacktest(?:ing)?\b"]),
    ("MLOps",               [r"\bmlops\b", r"\bml\s+ops\b", r"\bmodel\s+(?:deployment|serving|monitoring|registry)\b"]),
    ("scikit-learn",        [r"\bscikit[\s\-]*learn\b", r"\bsklearn\b"]),
    ("TensorFlow",          [r"\btensor\s*flow\b"]),
    ("PyTorch",             [r"\bpy\s*torch\b"]),
    ("Keras",               [r"\bkeras\b"]),
    ("XGBoost",             [r"\bxgboost\b", r"\bxg\s*boost\b", r"\bgradient\s+boost(?:ing)?\b"]),
    ("LightGBM",            [r"\blightgbm\b", r"\blight\s*gbm\b"]),
    ("CatBoost",            [r"\bcatboost\b"]),
    ("MLflow",              [r"\bmlflow\b"]),
    ("Hugging Face",        [r"\bhugging\s*face\b", r"\btransformers\s+library\b"]),
    ("LangChain",           [r"\blangchain\b"]),

    # ── Python Libraries ──────────────────────────────────────────────────────
    ("Pandas",              [r"\bpandas\b"]),
    ("NumPy",               [r"\bnumpy\b"]),
    ("Matplotlib",          [r"\bmatplotlib\b"]),
    ("Seaborn",             [r"\bseaborn\b"]),
    ("Plotly",              [r"\bplotly\b"]),
    ("SciPy",               [r"\bscipy\b"]),
    ("Statsmodels",         [r"\bstatsmodels\b"]),
    ("Jupyter",             [r"\bjupyter\b(?:\s+notebook|\s+lab)?"]),
    ("PySpark",             [r"\bpyspark\b"]),
    ("FastAPI",             [r"\bfastapi\b"]),

    # ── R Ecosystem ───────────────────────────────────────────────────────────
    ("RStudio",             [r"\brstudio\b"]),
    ("ggplot2",             [r"\bggplot2?\b"]),
    ("tidyverse",           [r"\btidyverse\b", r"\bdplyr\b", r"\btidyr\b", r"\bpurrr\b"]),
    ("Shiny",               [r"\bshiny\b(?:\s+app)?"]),
    ("R Markdown",          [r"\br\s+markdown\b", r"\brmarkdown\b"]),

    # ── Marketing Analytics ───────────────────────────────────────────────────
    ("Google Analytics",    [r"\bgoogle\s+analytics\b", r"\bga4\b", r"\buniversal\s+analytics\b"]),
    ("Adobe Analytics",     [r"\badobe\s+analytics\b", r"\bomniture\b"]),
    ("Google Ads",          [r"\bgoogle\s+ads\b", r"\bgoogle\s+adwords\b"]),
    ("Facebook Ads",        [r"\bfacebook\s+ads\b", r"\bmeta\s+ads\b", r"\bmeta\s+business\s+suite\b"]),
    ("Google Tag Manager",  [r"\bgoogle\s+tag\s+manager\b"]),
    ("SEO",                 [r"\bseo\b", r"\bsearch\s+engine\s+optimization\b"]),
    ("SEM",                 [r"\bsem\b(?=\s+(?:analyst|specialist|campaign|strategy|tool))", r"\bsearch\s+engine\s+marketing\b", r"\bpaid\s+search\b"]),
    ("Web Analytics",       [r"\bweb\s+analytics\b", r"\bdigital\s+analytics\b"]),
    ("Marketing Mix Modeling", [r"\bmarketing\s+mix\s+model(?:ing|ling)?\b", r"\bmmm\b"]),
    ("Attribution Modeling",[r"\battribution\s+model(?:ing)?\b", r"\bmulti[\s\-]?touch\s+attribution\b"]),
    ("Customer Segmentation",[r"\bcustomer\s+segmentation\b", r"\baudience\s+segmentation\b"]),
    ("Salesforce",          [r"\bsalesforce\b", r"\bsfdc\b", r"\bsales\s+cloud\b"]),
    ("HubSpot",             [r"\bhubspot\b"]),
    ("Marketo",             [r"\bmarketo\b"]),
    ("Mailchimp",           [r"\bmailchimp\b"]),
    ("CRM",                 [r"\bcrm\b", r"\bcustomer\s+relationship\s+management\b"]),
    ("Klaviyo",             [r"\bklaviyo\b"]),
    ("Braze",               [r"\bbraze\b"]),
    ("Optimizely",          [r"\boptimizely\b"]),
    ("Sprout Social",       [r"\bsprout\s+social\b"]),

    # ── Product Analytics ──────────────────────────────────────────────────────
    ("Mixpanel",            [r"\bmixpanel\b"]),
    ("Amplitude",           [r"\bamplitude\b(?!\s+(?:modulation|variation))"]),
    ("Pendo",               [r"\bpendo\b"]),
    ("Heap",                [r"\bheap\b(?:\s+analytics)?"]),
    ("FullStory",           [r"\bfullstory\b"]),
    ("Hotjar",              [r"\bhotjar\b"]),
    ("Segment",             [r"\bsegment\s*(?:cdp|\.io|platform)?\b"]),
    ("Customer Data Platform", [r"\bcdp\b", r"\bcustomer\s+data\s+platform\b"]),
    ("User Research",       [r"\buser\s+research\b", r"\busability\s+test(?:ing)?\b", r"\bux\s+research\b"]),
    ("Retention Analysis",  [r"\bretention\s+(?:analysis|rate|model|metric)\b"]),
    ("DAU/MAU",             [r"\bdau\b", r"\bmau\b", r"\bwau\b", r"\bdaily\s+active\s+users?\b", r"\bmonthly\s+active\s+users?\b"]),
    ("OKRs",                [r"\bokrs?\b", r"\bobjectives?\s+and\s+key\s+results?\b"]),
    ("KPIs",                [r"\bkpis?\b", r"\bkey\s+performance\s+indicator\b"]),

    # ── Financial Analytics ───────────────────────────────────────────────────
    ("Financial Modeling",  [r"\bfinancial\s+model(?:ing)?\b", r"\bdcf\b", r"\bdiscounted\s+cash\s+flow\b", r"\blbo\b"]),
    ("Financial Analysis",  [r"\bfinancial\s+analysis\b", r"\bfinancial\s+reporting\b"]),
    ("Valuation",           [r"\bvaluation\b", r"\bequity\s+valuation\b"]),
    ("FP&A",                [r"\bfp&a\b", r"\bfinancial\s+planning\s+(?:and|&)\s+analysis\b"]),
    ("Variance Analysis",   [r"\bvariance\s+analysis\b", r"\bbudget\s+vs\b"]),
    ("P&L Management",      [r"\bp(?:rofit)?\s*(?:&|and)\s*l(?:oss)?\b", r"\bp&l\b"]),
    ("SAP",                 [r"\bsap\b(?!\s+bo\b)(?!\s+business\s*objects)"]),
    ("ERP",                 [r"\berp\b", r"\benterprise\s+resource\s+planning\b"]),
    ("QuickBooks",          [r"\bquickbooks\b"]),
    ("NetSuite",            [r"\bnetsuite\b"]),
    ("Bloomberg",           [r"\bbloomberg\b(?=\s+(?:terminal|data|platform|api|analytics))"]),
    ("FactSet",             [r"\bfactset\b"]),
    ("Anaplan",             [r"\banaplan\b"]),
    ("Hyperion",            [r"\bhyperion\b"]),
    ("GAAP",                [r"\bgaap\b"]),
    ("IFRS",                [r"\bifrs\b"]),
    ("Risk Analysis",       [r"\brisk\s+(?:analysis|model(?:ing)?|management|assessment)\b", r"\bvalue\s+at\s+risk\b"]),

    # ── Operations Analytics ───────────────────────────────────────────────────
    ("Process Improvement", [r"\bprocess\s+improvement\b", r"\bprocess\s+optimization\b"]),
    ("Lean",                [r"\blean\b(?:\s+six\s+sigma|\s+manufacturing|\s+process|\s+methodology)?"]),
    ("Six Sigma",           [r"\bsix\s+sigma\b", r"\bdmaic\b", r"\bgreen\s+belt\b", r"\bblack\s+belt\b"]),
    ("Supply Chain",        [r"\bsupply\s+chain\b"]),
    ("Inventory Management",[r"\binventory\s+(?:management|optimization|analytics|control)\b"]),
    ("Demand Planning",     [r"\bdemand\s+(?:planning|forecasting|sensing)\b"]),
    ("Workforce Analytics", [r"\bworkforce\s+analytics\b", r"\bhr\s+analytics\b", r"\bpeople\s+analytics\b"]),
    ("Pricing Analytics",   [r"\bpricing\s+(?:analytics|model(?:ing)?|strategy|optimization)\b", r"\bdynamic\s+pricing\b"]),

    # ── Collaboration & Governance (Hard Tools) ────────────────────────────────
    ("Agile",               [r"\bagile\b(?:\s+methodology|\s+framework)?"]),
    ("Scrum",               [r"\bscrum\b"]),
    ("Kanban",              [r"\bkanban\b"]),
    ("Jira",                [r"\bjira\b"]),
    ("Confluence",          [r"\bconfluence\b"]),
    ("Data Governance",     [r"\bdata\s+governance\b", r"\bdata\s+quality\b", r"\bdata\s+stewardship\b", r"\bmaster\s+data\s+management\b"]),
    ("Git",                 [r"\bgit\b(?!\s*hub)(?!\s*lab)"]),
    ("GitHub",              [r"\bgithub\b"]),
    ("GitLab",              [r"\bgitlab\b"]),
    ("Data Catalog",        [r"\balation\b", r"\bcollibra\b"]),
    ("Presentation Tools",  [r"\bpowerpoint\b", r"\bms\s+office\b", r"\boffice\s+365\b", r"\bmicrosoft\s+365\b"]),
]

_SKILL_PATTERNS: list[tuple[str, re.Pattern]] = []
for _canonical, _patterns in _SKILLS_RAW:
    _combined = "|".join(f"(?:{p})" for p in _patterns)
    _SKILL_PATTERNS.append((_canonical, re.compile(_combined, re.IGNORECASE)))

def extract_skills(description: Optional[str]) -> Optional[str]:
    """Return a sorted, comma-separated string of canonical hard skills."""
    if not description:
        return None

    found: set[str] = set()
    for canonical, pattern in _SKILL_PATTERNS:
        if pattern.search(description):
            found.add(canonical)

    if _R_SKILL_PATTERN.search(description):
        found.add("R")

    return ", ".join(sorted(found)) if found else None


# ═══════════════════════════════════════════════════════════════════════════════
# 5.  SOFT SKILLS EXTRACTION
# ═══════════════════════════════════════════════════════════════════════════════

_SOFT_SKILLS_RAW: list[tuple[str, list[str]]] = [
    ("Communication", [
        r"\bcommunication\s+skills?\b", r"\bwritten\s+and\s+verbal\b", r"\bverbal\s+and\s+written\b",
        r"\bwritten\s+communication\b", r"\bverbal\s+communication\b", r"\boral\s+communication\b",
        r"\bexcellent\s+communicat(?:or|ion)\b", r"\bstrong\s+communicat(?:or|ion)\b",
        r"\beffective\s+communicat(?:or|ion)\b", r"\bclear\s+communicat(?:or|ion)\b",
        r"\bcommunicat(?:e|ing)\s+(?:complex|technical|findings|insights|results|data)\b",
        r"\bcommunicat(?:e|ing)\s+(?:clearly|effectively|concisely|persuasively)\b",
        r"\btranslat(?:e|ing)\s+(?:complex|technical|data|insights)\s+(?:to|for|into)\b",
        r"\bexplain(?:ing)?\s+(?:complex|technical|data|insights)\s+(?:to|for)\b",
        r"\barticulate\b", r"\bpresent(?:ing)?\s+(?:findings|results|insights|analysis|data|reports?)\b",
        r"\bconvey(?:ing)?\s+(?:insights|findings|complex|technical)\b",
        r"\bdocumentation\s+skills?\b", r"\bwrite\s+(?:clear|concise|effective)\s+reports?\b",
        r"\breport\s+writing\b", r"\btech(?:nical)?\s+writing\b", r"\bdata\s+storytelling\b",
        r"\bstorytelling\s+(?:ability|skills?|with\s+data)?\b", r"\bnarrative(?:s)?\s+(?:from|with|around)\s+data\b",
        r"\bnon[\s\-]technical\s+audience\b", r"\bbusiness\s+audience\b", r"\bdiverse\s+audience\b",
        r"\btailor(?:ing)?\s+(?:message|communication|content)\b",
    ]),
    ("Leadership", [
        r"\bleadership\s+skills?\b", r"\bleadership\s+(?:ability|experience|qualities|capabilities)\b",
        r"\bdemonstrated\s+leadership\b", r"\blead(?:ing)?\s+(?:a\s+team|teams?|cross[\s\-]functional|projects?|initiatives?|efforts?)\b",
        r"\blead(?:ing)?\s+(?:analyst|data|business|product)\b", r"\bdrive(?:s|n|ing)?\s+(?:decisions?|results?|initiatives?|change|impact|strategy|alignment)\b",
        r"\bdriving\s+(?:business|organizational|team|cross[\s\-]functional)\b", r"\bmentor(?:ing|ship)?\b",
        r"\bcoach(?:ing)?\s+(?:team\s+members?|junior|analysts?|others?)\b", r"\bdevelop(?:ing)?\s+(?:team\s+members?|junior\s+analysts?|others?)\b",
        r"\bmanag(?:e|ing|ed)\s+(?:a\s+team|teams?|analysts?|staff|direct\s+reports?)\b", r"\bpeople\s+management\b",
        r"\binfluence(?:ing)?\s+(?:without\s+authority|stakeholders?|decisions?|leaders?|senior)\b", r"\binfluencing\s+skills?\b",
        r"\bownership\s+(?:of|over)\b", r"\btake\s+ownership\b", r"\bsense\s+of\s+ownership\b",
        r"\baccountability\b", r"\bchampion(?:ing)?\s+(?:data|analytics|best\s+practices|initiatives?)\b",
        r"\badvocate\s+(?:for|of)\s+(?:data|analytics|best\s+practices)\b", r"\bset(?:ting)?\s+(direction|vision|priorities|goals)\b",
        r"\bthought\s+leader(?:ship)?\b", r"\bexecutive\s+(?:presence|communication|stakeholders?)\b",
        r"\bdecision[\s\-]making\s+(?:skills?|ability|authority)\b", r"\bown\s+the\s+(?:roadmap|strategy|analysis|pipeline|process)\b",
    ]),
    ("Collaboration", [
        r"\bcollabor(?:at(?:e|ion|ive|ing)|ator)\b", r"\bteam\s+player\b", r"\bteam[\s\-]oriented\b",
        r"\bworks?\s+(?:well\s+)?(?:with|in|across)\s+(?:teams?|others?|cross[\s\-]functional|diverse)\b",
        r"\bcross[\s\-]functional\b", r"\bcross[\s\-]team\b", r"\bcross[\s\-]departmental\b",
        r"\bcross[\s\-]organizational\b", r"\bpartner(?:ing|ship)?\s+(?:with|across|between)\b",
        r"\bpartner(?:ing)?\s+closely\b", r"\bbuild(?:ing)?\s+(?:strong\s+)?relationships?\b",
        r"\brelationship[\s\-]building\b", r"\binterpersonal\s+skills?\b", r"\binterpersonal\s+(?:ability|effectiveness)\b",
        r"\bwork(?:ing)?\s+collaboratively\b", r"\bcollaborative\s+(?:environment|culture|mindset|approach|team)\b",
        r"\bliaise\b", r"\bliaison\b", r"\bcoordinate\s+(?:with|across|between)\b",
        r"\bjointly\s+(?:develop|work|own|deliver)\b", r"\bshared\s+(?:goals?|objectives?|ownership)\b",
        r"\bembedded\s+(?:within|in)\s+(?:a\s+)?(?:team|business|org)\b", r"\bintegrate\s+(?:with|across)\s+(?:teams?|departments?|business)\b",
        r"\bcontribute\s+to\s+(?:a\s+)?(?:team|culture|org)\b", r"\bteamwork\b",
    ]),
    ("Problem Solving", [
        r"\bproblem[\s\-]solv(?:e|er|ing)\b", r"\bproblem[\s\-]solving\s+(?:skills?|ability|mindset)\b",
        r"\bsolv(?:e|ing)\s+(?:complex|ambiguous|difficult|challenging|business|open[\s\-]ended)\s+problems?\b",
        r"\bsolv(?:e|ing)\s+problems?\s+(?:creatively|analytically|independently)\b", r"\bsolution[\s\-]orient(?:ed|ation)\b",
        r"\bsolutions[\s\-]focused\b", r"\bresource(?:ful|fulness)\b", r"\bidentif(?:y|ying|ied)\s+(?:root\s+cause|issues?|gaps?|opportunities?|inefficiencies)\b",
        r"\broot\s+cause\s+(?:analysis|investigation|identification)\b", r"\btroubleshoot(?:ing)?\b",
        r"\bdiagnos(?:e|ing|tic)\s+(?:issues?|problems?|root\s+cause)\b", r"\bdevelop(?:ing)?\s+(?:creative|innovative|practical|scalable)\s+solutions?\b",
        r"\bfind(?:ing)?\s+solutions?\b", r"\bpropose\s+(?:solutions?|improvements?|recommendations?)\b",
        r"\bimprove(?:ment)?\s+(?:opportunities?|processes?)\b", r"\boptimiz(?:e|ing|ation)\s+(?:processes?|workflows?|systems?)\b",
        r"\bcreative\s+thinking\b", r"\binnovative\s+thinking\b", r"\binnovation\s+mindset\b",
        r"\bout[\s\-]of[\s\-]the[\s\-]box\s+thinking\b", r"\bthink(?:ing)?\s+outside\s+the\s+box\b",
        r"\bstructured\s+(?:thinking|problem[\s\-]solving|approach)\b", r"\bfirst[\s\-]principles\s+thinking\b",
        r"\bframework[\s\-]based\s+thinking\b",
    ]),
    ("Critical Thinking", [
        r"\bcritical\s+thinking\b", r"\bcritical[\s\-]thinker\b", r"\banalytical\s+thinking\b",
        r"\banalytical\s+(?:mindset|approach|rigor|skills?|ability)\b", r"\bstrong\s+analytical\b",
        r"\blogical\s+(?:thinking|reasoning|approach)\b", r"\breasoning\s+(?:skills?|ability)\b",
        r"\bsound\s+(?:judgment|reasoning|logic)\b", r"\bgood\s+judgment\b", r"\bexercise\s+(?:sound|good|strong)\s+judgment\b",
        r"\bchallenge\s+(?:assumptions?|status\s+quo|existing\s+processes?)\b", r"\bquestion\s+(?:assumptions?|data|findings|results|the\s+status\s+quo)\b",
        r"\bskeptical\b", r"\bhealthy\s+skepticism\b", r"\bdata[\s\-]driven\s+(?:decision[\s\-]making|approach|mindset|thinking)\b",
        r"\bevidence[\s\-]based\s+(?:decision[\s\-]making|approach|recommendations?)\b", r"\bsynthesiz(?:e|ing)\s+(?:information|data|insights|findings|complex)\b",
        r"\bsynthesis\s+of\s+(?:information|data|insights)\b", r"\bdraw(?:ing)?\s+(?:conclusions?|insights?)\s+from\b",
        r"\binterpret(?:ing|ation)?\s+(?:data|results?|findings|trends?|patterns?)\b", r"\bevaluat(?:e|ing|ion)\s+(?:data|evidence|options?|trade[\s\-]offs?)\b",
        r"\btrade[\s\-]off(?:s)?\s+(?:analysis|evaluation|thinking)\b", r"\bconnect\s+the\s+dots\b",
        r"\bsee\s+the\s+(?:big\s+picture|whole\s+picture)\b",
    ]),
    ("Analytical Thinking", [
        r"\bquantitative\s+(?:mindset|thinking|skills?|background|approach|reasoning)\b", r"\bnumerical\s+(?:reasoning|skills?|aptitude|proficiency)\b",
        r"\bnumbers[\s\-]driven\b", r"\bmetrics[\s\-]driven\b", r"\bmetrics[\s\-]oriented\b",
        r"\bdata[\s\-]driven\b", r"\bdata[\s\-]oriented\b", r"\bcomfort(?:able)?\s+with\s+(?:numbers?|data|ambiguity|uncertainty|complexity)\b",
        r"\bat\s+ease\s+with\s+(?:numbers?|data|ambiguity)\b", r"\bpassion(?:ate)?\s+(?:about|for)\s+(?:data|analytics|numbers?|insights?)\b",
        r"\blove\s+(?:data|numbers?|analytics)\b", r"\binsight[\s\-]driven\b", r"\binsight(?:ful|s)\s+(?:thinking|approach|analysis)\b",
        r"\banalyz(?:e|ing)\s+(?:large|complex|messy|structured|unstructured)\s+(?:data|datasets?|information)\b",
        r"\bbreak(?:ing)?\s+down\s+(?:complex|ambiguous|large)\s+(?:problems?|data|questions?)\b", r"\bdecompos(?:e|ing)\s+(?:problems?|complex|ambiguous)\b",
        r"\bstructure(?:d)?\s+(?:analysis|approach|thinking)\b", r"\bsystematic\s+(?:approach|thinking|analysis|problem[\s\-]solving)\b",
        r"\brigor(?:ous|ously)?\b", r"\brigorous\s+(?:analysis|methodology|approach|thinking)\b",
    ]),
    ("Attention to Detail", [
        r"\battention\s+to\s+detail\b", r"\battention[\s\-]to[\s\-]detail\b", r"\bdetail[\s\-]orient(?:ed|ation)\b",
        r"\bdetail[\s\-]focused\b", r"\bdetail[\s\-]minded\b", r"\bmeticulous\b",
        r"\bthorough(?:ness)?\b", r"\bthorough\s+(?:analysis|documentation|understanding|review)\b",
        r"\baccurat(?:e|cy)\b(?!\s+(?:model|prediction|forecast))", r"\bdata\s+accuracy\b", r"\bdata\s+integrity\b",
        r"\bquality[\s\-]focused\b", r"\bhigh[\s\-]quality\s+(?:work|output|deliverables?|analysis|reporting)\b",
        r"\bprecis(?:e|ion)\b", r"\bcareful\s+(?:attention|review|analysis|consideration)\b",
        r"\bensure\s+(?:accuracy|quality|correctness|data\s+integrity)\b", r"\bvalidat(?:e|ing|ion)\s+(?:data|results?|outputs?|findings)\b",
        r"\bsanity[\s\-]check(?:ing|s)?\b", r"\bquality\s+check(?:ing|s)?\b", r"\berror[\s\-]free\b",
        r"\bzero[\s\-]defect\b", r"\bpunctual(?:ity)?\b", r"\borganized\s+and\s+(?:detail|thorough|meticulous)\b",
    ]),
    ("Time Management", [
        r"\btime\s+management\b", r"\bmanag(?:e|ing)\s+(?:multiple\s+)?(?:priorities|projects?|deadlines?|tasks?|workload)\b",
        r"\bjuggl(?:e|ing)\s+(?:multiple|competing|several)\s+(?:priorities|projects?|deadlines?|tasks?)\b", r"\bmultitask(?:ing)?\b",
        r"\bmeet(?:ing)?\s+deadlines?\b", r"\bdeadline[\s\-]driven\b", r"\bwork(?:ing)?\s+(?:under|to)\s+(?:tight\s+)?deadlines?\b",
        r"\bdeliver(?:ing|ed)?\s+(?:on\s+time|within\s+deadline|results\s+on\s+time)\b", r"\bprioritiz(?:e|ing|ation)\b",
        r"\bcompeting\s+(?:priorities|demands?|deadlines?)\b", r"\befficient(?:ly)?\b", r"\befficiency[\s\-](focused|driven|oriented)\b",
        r"\bself[\s\-]manag(?:e|ing|ed|ement)\b", r"\bwork(?:ing)?\s+independently\b", r"\bwork(?:ing)?\s+with\s+minimal\s+(?:supervision|oversight|direction|guidance)\b",
        r"\bminimal\s+supervision\b", r"\bautonomous(?:ly)?\b", r"\bhigh\s+degree\s+of\s+(?:initiative|autonomy|independence)\b",
        r"\bself[\s\-]directed\b", r"\bproactive\s+(?:in\s+managing|approach\s+to|time)\b", r"\bplan(?:ning)?\s+and\s+(?:organiz|prioritiz|schedul)\b",
    ]),
    ("Adaptability", [
        r"\badapt(?:able|ability|ive|ing)\b", r"\bflexib(?:le|ility)\b", r"\bfast[\s\-]paced\s+(?:environment|setting|team|startup|company)\b",
        r"\bhigh[\s\-]growth\s+(?:environment|company|startup|setting)\b", r"\bdynamic\s+(?:environment|team|setting|company|startup)\b",
        r"\beverchanging\b", r"\bever[\s\-]changing\s+(?:environment|landscape|requirements?|priorities)\b",
        r"\bambiguity\b", r"\bcomfort(?:able)?\s+with\s+ambiguity\b", r"\bthrive(?:s)?\s+(?:in|under)\s+(?:ambiguity|uncertainty|pressure|fast[\s\-]paced)\b",
        r"\bembrace\s+(?:change|ambiguity|uncertainty)\b", r"\bchange\s+management\b", r"\bnavigate\s+(?:ambiguity|uncertainty|change|complex|competing)\b",
        r"\bquick(?:ly)?\s+(?:learn|adapt|pivot|adjust)\b", r"\brapid(?:ly)?\s+(?:learn|adapt|change|iterate|scale)\b",
        r"\blearn(?:ing)\s+quickly\b", r"\bquick\s+learner\b", r"\bfast\s+learner\b",
        r"\bpivot(?:ing)?\s+(?:quickly|rapidly|when\s+needed)\b", r"\badjust(?:ing)?\s+to\s+(?:changing|new|evolving)\b",
        r"\bevolving\s+(?:requirements?|priorities|landscape|environment)\b", r"\bwear(?:ing)?\s+multiple\s+hats\b",
        r"\bshift(?:ing)?\s+(?:priorities|directions?|focus)\b", r"\bopen\s+to\s+(?:change|feedback|new\s+ideas?|learning)\b",
        r"\bopen[\s\-]minded\b", r"\bcomfort(?:able)?\s+with\s+(?:change|uncertainty|new)\b", r"\bstartup\s+(?:environment|mindset|culture|pace)\b",
    ]),
    ("Self-Starter", [
        r"\bself[\s\-]starter\b", r"\bself[\s\-]motivat(?:ed|ion)\b", r"\bself[\s\-]driven\b",
        r"\bproactive(?:ly)?\b", r"\btake(?:s)?\s+initiative\b", r"\bshows?\s+initiative\b",
        r"\bdemonstrates?\s+initiative\b", r"\bhigh\s+(?:level\s+of\s+)?initiative\b", r"\binitiative[\s\-]taker\b",
        r"\bentrepreneurial\s+(?:mindset|spirit|attitude|approach)\b", r"\bowner(?:ship)?\s+mindset\b",
        r"\bdriven\s+(?:individual|professional|analyst)\b", r"\bhighly\s+motivated\b", r"\bgo[\s\-]getter\b",
        r"\bpush(?:ing)?\s+(?:boundaries|the\s+envelope|for\s+more)\b", r"\bdon['\']?t\s+wait\s+to\s+be\s+told\b",
        r"\bseek(?:ing|s)?\s+(?:out|opportunities?)\b", r"\bvolunteer(?:ing|s)?\s+(?:for|to)\b",
        r"\bidentif(?:y|ies|ying)\s+(?:and\s+)?(?:pursue|drive|lead)\s+(?:opportunities?|improvements?)\b",
        r"\bcome\s+up\s+with\s+(?:new|creative|innovative)\b", r"\bask\s+(?:the\s+right\s+questions?|hard\s+questions?|great\s+questions?)\b",
        r"\bgo\s+beyond\s+(?:the\s+ask|requirements?|what\s+is\s+asked)\b", r"\bexceed(?:ing|s)?\s+expectations?\b",
        r"\bsurpass(?:ing|es)?\s+expectations?\b", r"\bhigh\s+achiever\b",
    ]),
    ("Presentation", [
        r"\bpresentation\s+skills?\b", r"\bpresent(?:ing|ations?)?\s+to\s+(?:senior|executive|leadership|c[\s\-]suite|management|non[\s\-]technical)\b",
        r"\bpresent(?:ing|ation)?\s+(?:complex|technical|findings|results|data|insights)\b", r"\bpublic\s+speaking\b",
        r"\bspeak(?:ing)?\s+(?:at|in\s+front\s+of)\s+(?:large|senior|executive|diverse)\b", r"\bconferences?\s+(?:speaking|presentation|talk)\b",
        r"\bdeliver(?:ing|ed)?\s+(?:presentations?|talks?|briefings?|reports?)\s+to\b", r"\bexecutive\s+(?:presentations?|briefings?|summaries|reports?|communication)\b",
        r"\bc[\s\-]suite\s+(?:communication|presentations?|stakeholders?)\b", r"\bboard[\s\-]level\s+(?:communication|presentations?|reporting)\b",
        r"\bpowerpoint\s+(?:skills?|expertise|presentations?)\b", r"\bslide(?:s|deck|s\s+deck)?\s+(?:creation|design|development|building)\b",
        r"\bbuild(?:ing)?\s+(?:decks?|slides?|presentations?)\b", r"\bstoryboard(?:ing)?\b",
        r"\bvisualization\s+(?:skills?|of\s+(?:data|results?|findings))\b", r"\bvisualiz(?:e|ing)\s+(?:data|results?|findings|insights)\s+(?:for|to)\b",
        r"\bcommunicat(?:e|ing)\s+(?:visually|through\s+visualizations?)\b", r"\bcharts?\s+and\s+graphs?\b",
        r"\binfographic\b", r"\bwhiteboards?\s+(?:session|presentation|thinking)\b",
    ]),
    ("Stakeholder Management", [
        r"\bstakeholder\s+(?:management|engagement|communication|relationships?|alignment)\b", r"\bmanag(?:e|ing)\s+stakeholders?\b",
        r"\bmanag(?:e|ing)\s+(?:up|expectations?)\b", r"\bbusiness\s+partner(?:ing|ship)?\b", r"\bbusiness[\s\-]facing\b",
        r"\bpartner\s+with\s+(?:business|executive|senior|leadership|finance|marketing|product|engineering|operations)\b",
        r"\bwork(?:ing)?\s+with\s+(?:business|senior|executive|leadership|c[\s\-]suite|non[\s\-]technical)\s+(?:leaders?|stakeholders?|teams?|partners?)\b",
        r"\bsenior\s+(?:leadership|executives?|management|stakeholders?)\b(?!\s+position)", r"\bc[\s\-]suite\s+(?:exposure|stakeholders?|partners?)\b",
        r"\brequirements?\s+gathering\b", r"\bgather(?:ing)?\s+(?:business\s+)?requirements?\b", r"\belicit(?:ing)?\s+requirements?\b",
        r"\bunderstand(?:ing)?\s+(?:business\s+)?(?:needs?|requirements?|objectives?)\b", r"\btranslat(?:e|ing)\s+business\s+(?:needs?|requirements?|objectives?)\b",
        r"\balign(?:ing|ment)?\s+(?:with|across)\s+(?:stakeholders?|teams?|business|leadership)\b", r"\bbuild(?:ing)?\s+(?:buy[\s\-]in|consensus|trust)\b",
        r"\bgain(?:ing)?\s+(?:buy[\s\-]in|alignment|consensus|trust)\b", r"\binfluence\s+without\s+authority\b",
        r"\bnavigat(?:e|ing)\s+(?:organizational|corporate|political)\s+(?:dynamics?|landscape|complexity)\b", r"\binternal\s+(?:clients?|customers?)\b",
    ]),
    ("Strategic Thinking", [
        r"\bstrategic\s+(?:thinking|mindset|vision|direction|planning|approach)\b", r"\bstrateg(?:ist|ic\s+thinker)\b",
        r"\bbig[\s\-]picture\s+(?:thinking|mindset|view|thinker)\b", r"\bbusiness\s+acumen\b", r"\bbusiness\s+(?:sense|savvy|judgment|instinct)\b",
        r"\bcommercial\s+(?:acumen|awareness|mindset)\b", r"\bbusiness[\s\-]minded\b", r"\blong[\s\-]term\s+(?:thinking|vision|strategy|planning)\b",
        r"\bforward[\s\-]thinking\b", r"\bforward[\s\-]looking\b", r"\bholistic\s+(?:thinking|view|approach|perspective)\b",
        r"\bconnect(?:ing)?\s+(?:analysis|data|insights)\s+to\s+(?:business|strategy|outcomes?|goals?)\b", r"\bimpact[\s\-](?:focused|oriented|driven)\b",
        r"\blink(?:ing)?\s+(?:data|analytics|insights)\s+to\s+(?:business|strategy|value|impact)\b", r"\btranslat(?:e|ing)\s+(?:insights|analysis|data)\s+(?:into|to)\s+(?:strategy|action|business|value)\b",
        r"\bdriving\s+(?:business\s+)?(?:impact|value|outcomes?|strategy)\b", r"\bstrategic\s+(?:priorities|goals?|objectives?|roadmap|initiatives?)\b",
        r"\broadmap\s+(?:development|planning|strategy)\b", r"\balign(?:ing)?\s+(?:analytics|data|insights)\s+(?:to|with)\s+(?:business|strategy|goals?)\b",
        r"\bgoal[\s\-]setting\b", r"\bobjective[\s\-]setting\b",
    ]),
    ("Curiosity", [
        r"\bcuriosity\b", r"\bcurious\s+(?:mindset|individual|professional|about\s+data|about\s+business)\b",
        r"\bintellectually\s+curious\b", r"\bintellectual\s+curiosity\b", r"\bpassion(?:ate)?\s+(?:about|for)\s+(?:learning|data|analytics|solving|exploring|insights?)\b",
        r"\blove\s+(?:to\s+)?(?:learn|explore|discover|dig\s+into|ask\s+questions?)\b", r"\bdesire\s+to\s+(?:learn|grow|explore|understand|improve)\b",
        r"\beager(?:ness)?\s+to\s+(?:learn|grow|improve|develop)\b", r"\bexplor(?:e|atory|ing)\s+(?:data|questions?|insights?|new)\b",
        r"\bdig(?:ging)?\s+(?:deep|into\s+(?:data|problems?|questions?))\b", r"\bgrowth\s+mindset\b",
        r"\bcontinuous\s+(?:learning|improvement|growth)\b", r"\blifelong\s+learner\b", r"\bstay(?:ing)?\s+(?:current|up[\s\-]to[\s\-]date|abreast)\s+(?:with|on)\b",
        r"\bkeep(?:ing)?\s+up\s+with\s+(?:trends?|developments?|latest)\b", r"\bup[\s\-]to[\s\-]date\s+(?:on|with)\s+(?:industry|trends?|developments?)\b",
        r"\blearn(?:ing)?\s+(?:new\s+tools?|new\s+skills?|continuously|constantly)\b", r"\bwilling(?:ness)?\s+to\s+(?:learn|experiment|explore|try)\b",
        r"\bask(?:ing)?\s+the\s+right\s+questions?\b", r"\bwhat\s+if\b(?=\s+(?:questions?|thinking|mindset))",
        r"\bhypothesis[\s\-]driven\b", r"\bexperim(?:ent(?:al|ing)|entation)\s+(?:mindset|culture|approach)\b",
    ]),
    ("Work Ethic", [
        r"\bwork\s+ethic\b", r"\bstrong\s+work\s+ethic\b", r"\bhard[\s\-]working\b",
        r"\bdedicated\b(?!\s+(?:server|team|resource))", r"\bdedication\s+to\b", r"\bdiligent(?:ly|ce)?\b",
        r"\bcommitted\s+to\s+(?:excellence|quality|delivering|results?|success)\b", r"\bcommitment\s+to\s+(?:excellence|quality|results?|success|delivering)\b",
        r"\bdriven\s+to\s+(?:succeed|deliver|achieve|excel)\b", r"\bgo(?:ing)?\s+the\s+extra\s+mile\b",
        r"\babove\s+and\s+beyond\b", r"\bdeliver(?:ing)?\s+(?:high[\s\-]quality|excellent|exceptional|outstanding)\s+(?:work|results?|output|analysis)\b",
        r"\bexceed(?:ing)?\s+expectations?\b", r"\bresult(?:s)?[\s\-]orient(?:ed|ation)\b", r"\bresult(?:s)?[\s\-]driven\b",
        r"\boutput[\s\-]focused\b", r"\bhigh[\s\-]perform(?:er|ing|ance)\b", r"\bhigh\s+performer\b",
        r"\breliable\b", r"\bresponsible\b(?=\s+for)", r"\bdependable\b",
        r"\bconsistent(?:ly)?\b(?=\s+deliver)", r"\bexcellence\s+(?:in|of)\s+(?:work|execution|delivery)\b", r"\bcommitment\s+to\s+excellence\b",
        r"\bwilling(?:ness)?\s+to\s+(?:roll\s+up\s+sleeves|put\s+in\s+the\s+work|go\s+above)\b", r"\broll\s+up\s+(?:your|their|our)?\s*sleeves\b",
    ]),
    ("Emotional Intelligence", [
        r"\bemotional\s+intelligence\b", r"\bemotional\s+(?:intelligence|quotient|maturity|awareness|regulation)\b", r"\b(?:eq|e\.q\.)\b(?=\s+(?:skills?|matters?|is))",
        r"\bempathy\b", r"\bempathetic\b", r"\bself[\s\-]aware(?:ness)?\b",
        r"\bself[\s\-]reflect(?:ive|ion)?\b", r"\bactively\s+listen(?:ing|s)?\b", r"\bactive\s+listen(?:ing|er)\b",
        r"\blisten(?:ing)?\s+(?:skills?|actively|carefully)\b", r"\bconflict\s+(?:resolution|management|avoidance|handling)\b", r"\bnavigate\s+(?:conflict|difficult\s+conversations?|disagreements?)\b",
        r"\bdifficult\s+conversations?\b", r"\btact(?:ful)?(?:ness|ly)?\b", r"\bdiplomacy\b",
        r"\bdiplomatic\b", r"\bbuild(?:ing)?\s+(?:trust|rapport)\b", r"\btrust[\s\-]building\b",
        r"\bpsychological\s+safety\b", r"\binclusiv(?:e|ity|eness)\b", r"\bdiversit(?:y|y\s+and\s+inclusion)\b(?!\s+(?:hiring|data|metrics?|report))",
        r"\bequit(?:y|able)\b(?!\s+(?:compensation|analyst|research|portfolio))", r"\bbelonging\b", r"\brespect(?:ful)?\s+(communication|environment|workplace)\b",
        r"\bkind(?:ness)?\b(?=\s+(?:in\s+the\s+workplace|and\s+empathy))", r"\bcompassion(?:ate)?\b",
    ]),
    ("Organizational Skills", [
        r"\borganizational\s+skills?\b", r"\bhighly\s+organized\b", r"\borganized\s+(?:individual|professional|analyst|thinker|approach)\b",
        r"\borganize\s+(?:work|data|information|tasks?|projects?)\b", r"\bstructured\s+(?:approach|working\s+style|thinking|methodology)\b", r"\bprocess[\s\-]orient(?:ed|ation)\b",
        r"\bprocess[\s\-]driven\b", r"\bdocument(?:ing|ation)\s+(?:processes?|workflows?|findings|results?|work)\b", r"\bmaintain(?:ing)?\s+(?:organized|clear|accurate|up[\s\-]to[\s\-]date)\s+(?:records?|documentation|files?|notes?)\b",
        r"\bkeep(?:ing)?\s+(?:organized|structured|clear)\s+(?:records?|documentation|notes?)\b", r"\bplan(?:ning)?\s+and\s+(?:track|monitor|organiz|schedul|manag)\b", r"\bproject\s+(?:planning|tracking|organization|coordination)\b",
        r"\bschedule\s+management\b", r"\bworkflow\s+(?:management|organization|coordination)\b", r"\bsystematic(?:ally)?\b",
        r"\bmethodical(?:ly)?\b", r"\bwell[\s\-]organized\b", r"\bkeep(?:ing)?\s+things?\s+(?:organized|on\s+track|running\s+smoothly)\b",
        r"\btracking\s+(?:progress|milestones?|deliverables?|action\s+items?)\b", r"\baction\s+item\s+(?:tracking|management|follow[\s\-]up)\b", r"\bfollow[\s\-]through\b",
        r"\bfollow\s+up\b(?=\s+(?:on|with|to\s+ensure))", r"\bclosing\s+the\s+loop\b",
    ]),
]

_SOFT_SKILL_BUCKETS: list[tuple[str, re.Pattern]] = []
for _bucket_name, _bucket_pats in _SOFT_SKILLS_RAW:
    _combined = "|".join(f"(?:{p})" for p in _bucket_pats)
    _SOFT_SKILL_BUCKETS.append((_bucket_name, re.compile(_combined, re.IGNORECASE)))

def extract_soft_skills(description: Optional[str]) -> Optional[str]:
    """Return a comma-separated string of soft-skill bucket names."""
    if not description:
        return None
    found: list[str] = []
    for canonical, pattern in _SOFT_SKILL_BUCKETS:
        if pattern.search(description):
            found.append(canonical)
    return ", ".join(found) if found else None

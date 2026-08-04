"""
Continuous Job/Internship Search Agent - v3
--------------------------------------------
Runs 24/7 as a local daemon. No API keys required. Ranks jobs against your
resume using local TF-IDF + cosine similarity (scikit-learn) — the "AI"
layer — fully offline. Sends new matches via Zapier webhook, direct Gmail
SMTP, Telegram bot, or Discord webhook.

SOURCES (all free, no auth, no login):
  - RemoteOK                     https://remoteok.com/api
  - Arbeitnow Job Board          https://www.arbeitnow.com/api/job-board-api
  - Jobicy                       https://jobicy.com/api/v2/remote-jobs
  - Himalayas                    https://himalayas.app/jobs/api
  - Remotive                     https://remotive.com/api/remote-jobs
  - Hacker News "Who's Hiring"   via Algolia HN Search API
  - We Work Remotely RSS feeds
  - Greenhouse / Lever           public per-company job boards (opt-in list)

SETUP
  1. pip install -r requirements.txt
  2. Edit config.yaml (or set env vars - env vars always win)
  3. Drop resume.pdf or resume.docx in this folder
  4. Test:      python job_search_agent.py --once --dry-run
  5. Run once:  python job_search_agent.py --once
  6. Run 24/7:  python job_search_agent.py
"""

import os
import re
import sys
import html
import json
import time
import signal
import logging
import smtplib
import requests
import feedparser
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import db

# -----------------------------------------------------------------------
# PATHS
# -----------------------------------------------------------------------
BASE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CONFIG_FILE = BASE_DIR / "config.yaml"
SEEN_JOBS_FILE = BASE_DIR / "seen_jobs.json"
LOG_DIR = BASE_DIR / "log"
LOG_FILE = LOG_DIR / "agent.log"
CYCLE_STATS_FILE = LOG_DIR / "cycle_stats.json"

log = logging.getLogger(__name__)

# -----------------------------------------------------------------------
# CONFIG — load config.yaml first, then env vars override
# -----------------------------------------------------------------------
def _load_yaml_config() -> dict:
    """Load config.yaml if present; silently return empty dict if not."""
    if not CONFIG_FILE.exists():
        return {}
    try:
        import yaml
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        log.warning("Could not load config.yaml: %s", e)
        return {}


# -----------------------------------------------------------------------
# .env FILE LOADER (stdlib only — no python-dotenv needed)
# Loads .env before config.yaml so credentials stay out of the repo.
# -----------------------------------------------------------------------
def _load_dotenv():
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    try:
        with open(env_file, "r", encoding="utf-8") as f:
            for raw_line in f:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and key not in os.environ:  # env vars still win
                    os.environ[key] = val
    except Exception as e:
        log.warning("Could not load .env: %s", e)


_load_dotenv()

_cfg = _load_yaml_config()


def _get(key: str, default, cast=None):
    """Resolve: env var > config.yaml > default."""
    env_val = os.environ.get(key.upper())
    if env_val is not None:
        val = env_val
    else:
        val = _cfg.get(key.lower(), default)
    if cast and val is not None:
        try:
            return cast(val)
        except Exception:
            return default
    return val


# --- Delivery ---
ZAPIER_WEBHOOK_URL = _get("ZAPIER_WEBHOOK_URL", "PASTE_YOUR_ZAPIER_CATCH_HOOK_URL_HERE")
GMAIL_ADDRESS = _get("GMAIL_ADDRESS", "")
GMAIL_APP_PASSWORD = _get("GMAIL_APP_PASSWORD", "")
GMAIL_TO_ADDRESS = _get("GMAIL_TO_ADDRESS", "") or GMAIL_ADDRESS
FORCE_DIRECT_GMAIL = str(_get("FORCE_DIRECT_GMAIL", "false")).lower() == "true"
TELEGRAM_BOT_TOKEN = _get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = _get("TELEGRAM_CHAT_ID", "")
DISCORD_WEBHOOK_URL = _get("DISCORD_WEBHOOK_URL", "")

# --- Matching ---
RESUME_PATH = str(_get("RESUME_PATH", str(BASE_DIR / "resume.pdf")))
SIMILARITY_THRESHOLD = _get("SIMILARITY_THRESHOLD", 0.12, float)
TITLE_BOOST_MULTIPLIER = _get("TITLE_BOOST_MULTIPLIER", 1.4, float)
LEVEL_FILTERS = _get("LEVEL_FILTERS", ["intern", "internship", "entry level", "junior", "new grad", "graduate"])
if isinstance(LEVEL_FILTERS, str):
    LEVEL_FILTERS = [x.strip() for x in LEVEL_FILTERS.split(",")]

# --- Scheduling ---
CHECK_INTERVAL_HOURS = _get("CHECK_INTERVAL_HOURS", 2.0, float)

# --- Location ---
PREFER_REMOTE = str(_get("PREFER_REMOTE", "true")).lower() == "true"
PREFERRED_LOCATIONS = _get("PREFERRED_LOCATIONS", [])
if isinstance(PREFERRED_LOCATIONS, str):
    PREFERRED_LOCATIONS = [x.strip() for x in PREFERRED_LOCATIONS.split(",") if x.strip()]

# --- Company watchlists ---
GREENHOUSE_COMPANY_SLUGS = _get("GREENHOUSE_COMPANY_SLUGS", [])
if isinstance(GREENHOUSE_COMPANY_SLUGS, str):
    GREENHOUSE_COMPANY_SLUGS = [x.strip() for x in GREENHOUSE_COMPANY_SLUGS.split(",") if x.strip()]
LEVER_COMPANY_SLUGS = _get("LEVER_COMPANY_SLUGS", [])
if isinstance(LEVER_COMPANY_SLUGS, str):
    LEVER_COMPANY_SLUGS = [x.strip() for x in LEVER_COMPANY_SLUGS.split(",") if x.strip()]

# --- Browser scrapers (Internshala / Indeed / Unstop) ---
BROWSER_ENABLED = str(_get("BROWSER_ENABLED", "false")).lower() == "true"
BROWSER_HEADLESS = str(_get("BROWSER_HEADLESS", "true")).lower() == "true"
INDEED_QUERY = str(_get("INDEED_QUERY", "software engineer intern"))
INDEED_LOCATION = str(_get("INDEED_LOCATION", "India"))
UNSTOP_INCLUDE_HACKATHONS = str(_get("UNSTOP_INCLUDE_HACKATHONS", "true")).lower() == "true"

# --- Auto-Apply ---
AUTO_APPLY_ENABLED = str(_get("AUTO_APPLY_ENABLED", "false")).lower() == "true"
AUTO_APPLY_THRESHOLD = _get("AUTO_APPLY_THRESHOLD", 0.25, float)
AUTO_APPLY_PLATFORMS = _get("AUTO_APPLY_PLATFORMS", ["internshala", "indeed", "unstop"])
if isinstance(AUTO_APPLY_PLATFORMS, str):
    AUTO_APPLY_PLATFORMS = [x.strip() for x in AUTO_APPLY_PLATFORMS.split(",") if x.strip()]
COVER_LETTER_TEMPLATE = str(_get(
    "COVER_LETTER_TEMPLATE",
    "Hi, I am a {level} student with strong skills in {top_skills}. "
    "I am very interested in the {title} role at {company} and believe my background "
    "makes me a strong candidate. Please find my resume attached. "
    "Looking forward to hearing from you!"
))

# --- Subscription ---
SUBSCRIPTION_REQUIRED = str(_get("SUBSCRIPTION_REQUIRED", "true")).lower() == "true"
SUBSCRIPTIONS_FILE = BASE_DIR / "subscriptions.json"

# --- Job feed URLs ---
WWR_RSS_FEEDS = [
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-full-stack-programming-jobs.rss",
]

FALLBACK_KEYWORDS_TEXT = (
    # Core roles
    "software engineer intern software engineering intern data science intern "
    "machine learning intern backend developer frontend developer full stack developer "
    "python developer javascript developer react developer node developer "
    "devops intern cloud intern data engineer intern data analyst intern "
    "computer science intern web developer intern product intern research intern "
    "entry level junior new grad graduate fresher trainee "
    # Skills
    "python javascript typescript react nextjs nodejs express fastapi django flask "
    "sql postgresql mysql mongodb redis docker kubernetes git github linux "
    "machine learning deep learning tensorflow pytorch scikit-learn pandas numpy "
    "api rest graphql microservices ci cd aws gcp azure cloud "
    "data structures algorithms system design object oriented programming "
    # India-specific
    "bangalore mumbai delhi hyderabad pune chennai india remote work from home "
    "internship stipend 6 months 3 months summer internship winter internship "
    "b.tech btech mtech msc bsc computer science engineering information technology "
    # Certifications / buzzwords
    "open source contribution competitive programming problem solving agile scrum "
    "communication teamwork analytical skills research publication ieee acm "
)

# -----------------------------------------------------------------------
# SYNONYM EXPANSION TABLE
# Expands abbreviations/aliases before TF-IDF so they score correctly.
# -----------------------------------------------------------------------
SYNONYM_MAP = {
    r"\bml\b": "machine learning",
    r"\bai\b": "artificial intelligence",
    r"\bswe\b": "software engineer",
    r"\bsde\b": "software development engineer",
    r"\bfe\b": "frontend",
    r"\bbe\b": "backend",
    r"\bfs\b": "full stack",
    r"\bds\b": "data science",
    r"\bnlp\b": "natural language processing",
    r"\bcv\b": "computer vision",
    r"\bllm\b": "large language model",
    r"\bapi\b": "application programming interface",
    r"\bdb\b": "database",
    r"\bos\b": "operating system",
    r"\bci\b": "continuous integration",
    r"\bcd\b": "continuous deployment",
    r"\bk8s\b": "kubernetes",
    r"\baws\b": "amazon web services",
    r"\bgcp\b": "google cloud platform",
    r"\bjs\b": "javascript",
    r"\bts\b": "typescript",
    r"\bpy\b": "python",
}

_URL_RE = re.compile(r"^https?://", re.IGNORECASE)

# -----------------------------------------------------------------------
# SUBSCRIPTION GATE
# -----------------------------------------------------------------------
def check_subscription() -> bool:
    """
    Returns True if an active subscription is found in subscriptions.json,
    or if SUBSCRIPTION_REQUIRED is False (dev bypass).
    Logs a clear warning if subscription is missing/expired.
    """
    if not SUBSCRIPTION_REQUIRED:
        return True
    if not SUBSCRIPTIONS_FILE.exists():
        log.warning(
            "[Subscription] No subscription found. "
            "Visit http://localhost:8765 → 💳 Subscription tab to subscribe for ₹75/month. "
            "Notifications are paused until an active subscription exists."
        )
        return False
    try:
        with open(SUBSCRIPTIONS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        for sub in reversed(data.get("subscribers", [])):
            if sub.get("status") != "active":
                continue
            try:
                sub_date = datetime.fromisoformat(sub["subscribed_at"])
                if now - sub_date <= timedelta(days=31):
                    log.info(
                        "[Subscription] Active - %s (subscribed %s)",
                        sub.get("email", ""),
                        sub_date.strftime("%Y-%m-%d"),
                    )
                    return True
            except Exception:
                log.warning("[Subscription] Ignoring malformed subscription record: %r", sub)
        # All records expired
        log.warning(
            "[Subscription] Subscription expired. "
            "Visit http://localhost:8765 → 💳 Subscription tab to renew for ₹75/month."
        )
        return False
    except Exception as e:
        log.warning(f"[Subscription] Could not read subscriptions.json: {e}")
        return False


# -----------------------------------------------------------------------
# SIGNAL HANDLING
# -----------------------------------------------------------------------
_RUNNING = True
_DRY_RUN = "--dry-run" in sys.argv


def _handle_stop(signum, frame):
    global _RUNNING
    log.info("Stop signal received - finishing current cycle then exiting.")
    _RUNNING = False


signal.signal(signal.SIGINT, _handle_stop)
signal.signal(signal.SIGTERM, _handle_stop)


# -----------------------------------------------------------------------
# LOGGING SETUP
# -----------------------------------------------------------------------
def setup_logging():
    LOG_DIR.mkdir(exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    # Rotating file handler — max 5 MB, keep 3 backups
    fh = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    root.addHandler(fh)

    # Console handler — force UTF-8 on Windows to avoid cp1252 errors
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    root.addHandler(ch)


log = logging.getLogger(__name__)
setup_logging()
log = logging.getLogger(__name__)


# -----------------------------------------------------------------------
# CYCLE STATS
# -----------------------------------------------------------------------
def load_cycle_stats() -> list:
    return db.load_cycle_stats()


def save_cycle_stats(stats: list):
    db.save_cycle_stats(stats)


# -----------------------------------------------------------------------
# RESUME TEXT EXTRACTION
# -----------------------------------------------------------------------
def extract_resume_text(path: str) -> str:
    if not path or not os.path.exists(path):
        return ""
    ext = path.lower().rsplit(".", 1)[-1]
    try:
        if ext == "pdf":
            import pdfplumber
            text_parts = []
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text_parts.append(page.extract_text() or "")
            return "\n".join(text_parts)
        elif ext == "docx":
            import docx
            d = docx.Document(path)
            return "\n".join(p.text for p in d.paragraphs)
        else:
            log.warning(f"Unsupported resume format: .{ext} — use .pdf or .docx")
            return ""
    except Exception as e:
        log.warning(f"Could not parse resume ({path}): {e}")
        return ""


def get_resume_text() -> str:
    text = extract_resume_text(RESUME_PATH)
    if text.strip():
        log.info(f"Loaded resume: {len(text)} chars from {RESUME_PATH}")
        return text
    log.info("No resume found/parsed — using fallback keyword profile.")
    return FALLBACK_KEYWORDS_TEXT


# -----------------------------------------------------------------------
# SYNONYM EXPANSION
# -----------------------------------------------------------------------
def expand_synonyms(text: str) -> str:
    """Expand abbreviations/aliases so TF-IDF matches them correctly."""
    lower = text.lower()
    for pattern, replacement in SYNONYM_MAP.items():
        lower = re.sub(pattern, replacement, lower)
    return lower


# -----------------------------------------------------------------------
# SOURCE FETCHERS
# -----------------------------------------------------------------------
def _build_retry_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0 (compatible; job-search-agent/3.0)"})
    retry = Retry(
        total=3,
        read=3,
        connect=3,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


_SESSION = _build_retry_session()


def _safe_get(url: str, timeout: int = 15, **kwargs):
    """GET with error logging; returns Response or None."""
    try:
        r = _SESSION.get(url, timeout=timeout, **kwargs)
        r.raise_for_status()
        return r
    except Exception as e:
        log.warning("GET %s failed: %s", url, e)
        return None


def _sanitize_external_url(url: str) -> str:
    candidate = (url or "").strip()
    return candidate if _URL_RE.match(candidate) else "#"


def fetch_remoteok() -> list:
    jobs = []
    r = _safe_get("https://remoteok.com/api")
    if not r:
        return jobs
    for item in r.json():
        if not isinstance(item, dict) or "id" not in item:
            continue
        jobs.append({
            "id": f"remoteok_{item.get('id')}",
            "title": item.get("position", ""),
            "company": item.get("company", ""),
            "url": item.get("url", ""),
            "location": "Remote",
            "description": (item.get("description") or "")[:3000],
            "source": "RemoteOK",
            "posted_at": item.get("date", ""),
        })
    return jobs


def fetch_arbeitnow() -> list:
    jobs = []
    r = _safe_get("https://www.arbeitnow.com/api/job-board-api")
    if not r:
        return jobs
    for item in r.json().get("data", []):
        jobs.append({
            "id": f"arbeitnow_{item.get('slug')}",
            "title": item.get("title", ""),
            "company": item.get("company_name", ""),
            "url": item.get("url", ""),
            "location": item.get("location", ""),
            "description": (item.get("description") or "")[:3000],
            "source": "Arbeitnow",
            "posted_at": item.get("created_at", ""),
        })
    return jobs


def fetch_jobicy() -> list:
    jobs = []
    r = _safe_get("https://jobicy.com/api/v2/remote-jobs")
    if not r:
        return jobs
    for item in r.json().get("jobs", []):
        jobs.append({
            "id": f"jobicy_{item.get('id')}",
            "title": item.get("jobTitle", ""),
            "company": item.get("companyName", ""),
            "url": item.get("url", ""),
            "location": item.get("jobGeo", "Remote"),
            "description": (item.get("jobExcerpt") or item.get("jobDescription") or "")[:3000],
            "source": "Jobicy",
            "posted_at": item.get("pubDate", ""),
        })
    return jobs


def fetch_himalayas() -> list:
    jobs = []
    r = _safe_get("https://himalayas.app/jobs/api")
    if not r:
        return jobs
    data = r.json()
    listings = data.get("jobs", data) if isinstance(data, dict) else data
    for item in listings:
        jobs.append({
            "id": f"himalayas_{item.get('guid', item.get('id'))}",
            "title": item.get("title", ""),
            "company": item.get("companyName", ""),
            "url": item.get("applicationLink", item.get("url", "")),
            "location": item.get("location", "Remote"),
            "description": (item.get("description") or "")[:3000],
            "source": "Himalayas",
            "posted_at": item.get("pubDate", ""),
        })
    return jobs


def fetch_remotive() -> list:
    """Remotive — popular remote-first board with category filtering."""
    jobs = []
    r = _safe_get("https://remotive.com/api/remote-jobs")
    if not r:
        return jobs
    for item in r.json().get("jobs", []):
        jobs.append({
            "id": f"remotive_{item.get('id')}",
            "title": item.get("title", ""),
            "company": item.get("company_name", ""),
            "url": item.get("url", ""),
            "location": item.get("candidate_required_location", "Remote"),
            "description": re.sub(r"<[^<]+?>", " ", item.get("description") or "")[:3000],
            "source": "Remotive",
            "posted_at": item.get("publication_date", ""),
        })
    return jobs


def fetch_hn_whoishiring() -> list:
    jobs = []
    r = _safe_get(
        "https://hn.algolia.com/api/v1/search_by_date"
        "?tags=story,author_whoishiring&query=Who%20is%20hiring"
    )
    if not r:
        return jobs
    hits = r.json().get("hits", [])
    if not hits:
        return jobs
    story_id = hits[0]["objectID"]
    r2 = _safe_get(f"https://hn.algolia.com/api/v1/items/{story_id}")
    if not r2:
        return jobs
    for c in r2.json().get("children", []):
        text = c.get("text") or ""
        if not text:
            continue
        clean = re.sub(r"<[^<]+?>", " ", text).strip()
        jobs.append({
            "id": f"hn_{c.get('id')}",
            "title": clean.split("\n")[0][:200],
            "company": "(see post)",
            "url": f"https://news.ycombinator.com/item?id={c.get('id')}",
            "location": "",
            "description": clean[:3000],
            "source": "HN Who's Hiring",
            "posted_at": "",
        })
    return jobs


def fetch_wwr_rss() -> list:
    jobs = []
    for feed_url in WWR_RSS_FEEDS:
        try:
            feed = feedparser.parse(feed_url)
            for entry in feed.entries:
                jobs.append({
                    "id": f"wwr_{entry.get('id', entry.get('link'))}",
                    "title": entry.get("title", ""),
                    "company": "",
                    "url": entry.get("link", ""),
                    "location": "Remote",
                    "description": entry.get("summary", "")[:3000],
                    "source": "We Work Remotely",
                    "posted_at": "",
                })
        except Exception as e:
            log.warning(f"WWR RSS fetch failed for {feed_url}: {e}")
    return jobs


def fetch_linkedin_rss() -> list:
    """
    LinkedIn public job RSS — no login required.
    Format: https://www.linkedin.com/jobs/search/?keywords=<q>&location=<loc>&f_TPR=r86400&f_JT=I
    Returns an RSS feed parseable by feedparser.
    f_TPR=r86400  = posted in last 24 h
    f_JT=I        = Internship job type
    """
    jobs = []
    import urllib.parse
    queries = [
        ("software engineer intern", "India"),
        ("data science intern", "India"),
        ("developer internship", "India"),
        ("machine learning intern", "India"),
    ]
    seen_ids: set = set()
    for q, loc in queries:
        params = urllib.parse.urlencode({
            "keywords": q,
            "location": loc,
            "f_TPR": "r604800",   # last 7 days
            "f_JT": "I",           # Internship type
            "position": 1,
            "pageNum": 0,
        })
        feed_url = f"https://www.linkedin.com/jobs/search/?{params}"
        # LinkedIn serves HTML for browser; use RSS variant
        rss_url = f"https://www.linkedin.com/jobs/search/?{params}&trk=public_jobs_jobs-search-bar_search-submit&redirect=false&position=1&pageNum=0"
        # The true public RSS endpoint
        rss_params = urllib.parse.urlencode({
            "keywords": q,
            "location": loc,
            "f_TPR": "r604800",
        })
        rss_endpoint = f"https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search?{rss_params}&start=0"
        try:
            r = _safe_get(
                rss_endpoint,
                timeout=15,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Accept": "text/html,application/xhtml+xml",
                    "Accept-Language": "en-IN,en;q=0.9",
                    "Referer": "https://www.linkedin.com/",
                }
            )
            if not r:
                continue
            html = r.text
            # Extract job card data from LinkedIn's public guest API response
            ids   = re.findall(r'data-entity-urn="urn:li:jobPosting:([0-9]+)"', html)
            titles   = re.findall(r'class="base-search-card__title"[^>]*>\s*([^<]+)\s*<', html)
            companies = re.findall(r'class="base-search-card__subtitle"[^>]*>\s*<[^>]+>\s*([^<]+)\s*<', html)
            locations = re.findall(r'class="job-search-card__location"[^>]*>\s*([^<]+)\s*<', html)
            for i, jid in enumerate(ids):
                uid = f"linkedin_{jid}"
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)
                title = titles[i].strip() if i < len(titles) else f"Internship ({jid})"
                company = companies[i].strip() if i < len(companies) else ""
                location = locations[i].strip() if i < len(locations) else "India"
                jobs.append({
                    "id": uid,
                    "title": title,
                    "company": company,
                    "url": f"https://www.linkedin.com/jobs/view/{jid}/",
                    "location": location,
                    "description": f"{title} at {company}. Location: {location}. Found via LinkedIn India.",
                    "source": "LinkedIn",
                    "posted_at": "",
                })
            log.debug(f"[LinkedIn] {q!r}: {len(ids)} listings")
        except Exception as e:
            log.debug(f"[LinkedIn] RSS error for {q!r}: {e}")
    return jobs


def fetch_greenhouse(slugs: list) -> list:
    jobs = []
    for slug in slugs:
        r = _safe_get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true")
        if not r:
            continue
        for item in r.json().get("jobs", []):
            jobs.append({
                "id": f"greenhouse_{item.get('id')}",
                "title": item.get("title", ""),
                "company": slug,
                "url": item.get("absolute_url", ""),
                "location": item.get("location", {}).get("name", ""),
                "description": re.sub(r"<[^<]+?>", " ", item.get("content") or "")[:3000],
                "source": f"Greenhouse ({slug})",
                "posted_at": item.get("updated_at", ""),
            })
    return jobs


def fetch_lever(slugs: list) -> list:
    jobs = []
    for slug in slugs:
        r = _safe_get(f"https://api.lever.co/v0/postings/{slug}?mode=json")
        if not r:
            continue
        for item in r.json():
            jobs.append({
                "id": f"lever_{item.get('id')}",
                "title": item.get("text", ""),
                "company": slug,
                "url": item.get("hostedUrl", ""),
                "location": item.get("categories", {}).get("location", ""),
                "description": re.sub(
                    r"<[^<]+?>", " ",
                    item.get("descriptionPlain") or item.get("description") or ""
                )[:3000],
                "source": f"Lever ({slug})",
                "posted_at": "",
            })
    return jobs


def fetch_browser_sources() -> tuple:
    """
    Run Playwright-based scrapers for Internshala, Indeed, and Unstop.
    Only runs if BROWSER_ENABLED=true in config.yaml or environment.
    Returns (jobs_list, source_counts_dict).
    """
    jobs = []
    source_counts = {}

    if not BROWSER_ENABLED:
        return jobs, source_counts

    try:
        from scrapers.browser_base import check_playwright
        if not check_playwright():
            log.warning(
                "[Browser] playwright not installed. "
                "Run: pip install playwright && playwright install chromium"
            )
            return jobs, source_counts
    except ImportError:
        log.warning("[Browser] scrapers package not found.")
        return jobs, source_counts

    # --- Internshala ---
    try:
        from scrapers.internshala import fetch_internshala
        log.info("[Browser] Fetching Internshala...")
        fetched = fetch_internshala(headless=BROWSER_HEADLESS)
        source_counts["Internshala"] = len(fetched)
        jobs += fetched
        log.info(f"  Internshala: {len(fetched)} listings")
    except Exception as e:
        log.warning(f"[Browser] Internshala scraper failed: {e}")

    # --- Indeed ---
    try:
        from scrapers.indeed import fetch_indeed
        log.info(f"[Browser] Fetching Indeed ({INDEED_QUERY!r} in {INDEED_LOCATION!r})...")
        fetched = fetch_indeed(
            query=INDEED_QUERY,
            location=INDEED_LOCATION,
            headless=BROWSER_HEADLESS,
        )
        source_counts["Indeed"] = len(fetched)
        jobs += fetched
        log.info(f"  Indeed: {len(fetched)} listings")
    except Exception as e:
        log.warning(f"[Browser] Indeed scraper failed: {e}")

    # --- Unstop ---
    try:
        from scrapers.unstop import fetch_unstop
        log.info("[Browser] Fetching Unstop...")
        fetched = fetch_unstop(
            include_hackathons=UNSTOP_INCLUDE_HACKATHONS,
            headless=BROWSER_HEADLESS,
        )
        source_counts["Unstop"] = len(fetched)
        jobs += fetched
        log.info(f"  Unstop: {len(fetched)} listings")
    except Exception as e:
        log.warning(f"[Browser] Unstop scraper failed: {e}")

    return jobs, source_counts


def _fetch_source_safe(name: str, fn):
    try:
        fetched = fn()
        return name, fetched or [], None
    except Exception as exc:
        return name, [], exc


def _dedupe_jobs(jobs: list) -> list:
    unique_jobs = []
    seen_ids = set()
    seen_urls = set()
    for job in jobs:
        job_id = job.get("id")
        job_url = job.get("url", "").strip().lower()
        dedupe_key = job_id or job_url
        if not dedupe_key:
            continue
        if job_id and job_id in seen_ids:
            continue
        if not job_id and job_url and job_url in seen_urls:
            continue
        if job_id:
            seen_ids.add(job_id)
        if job_url:
            seen_urls.add(job_url)
        unique_jobs.append(job)
    return unique_jobs


def fetch_all_sources() -> list:
    sources = [
        ("RemoteOK", fetch_remoteok),
        ("Arbeitnow", fetch_arbeitnow),
        ("Jobicy", fetch_jobicy),
        ("Himalayas", fetch_himalayas),
        ("Remotive", fetch_remotive),
        ("HN Who's Hiring", fetch_hn_whoishiring),
        ("We Work Remotely", fetch_wwr_rss),
    ]
    jobs = []
    source_counts = {}
    max_workers = min(8, len(sources)) or 1
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(_fetch_source_safe, name, fn): name for name, fn in sources}
        for future in as_completed(future_map):
            name, fetched, error = future.result()
            source_counts[name] = len(fetched)
            jobs += fetched
            if error:
                log.warning("%s fetch failed: %s", name, error)
            log.info("  %s: %s listings", name, len(fetched))

    if GREENHOUSE_COMPANY_SLUGS:
        fetched = fetch_greenhouse(GREENHOUSE_COMPANY_SLUGS)
        source_counts["Greenhouse"] = len(fetched)
        jobs += fetched
    if LEVER_COMPANY_SLUGS:
        fetched = fetch_lever(LEVER_COMPANY_SLUGS)
        source_counts["Lever"] = len(fetched)
        jobs += fetched

    # Browser-based scrapers (Internshala, Indeed, Unstop)
    if BROWSER_ENABLED:
        b_jobs, b_counts = fetch_browser_sources()
        jobs += b_jobs
        source_counts.update(b_counts)

    return _dedupe_jobs(jobs), source_counts


# -----------------------------------------------------------------------
# LOCAL AI MATCHING — TF-IDF + cosine similarity + title boost
# -----------------------------------------------------------------------
def rank_by_similarity(resume_text: str, jobs: list) -> list:
    """
    Scores every job's (title + description) against the resume using
    TF-IDF cosine similarity. Applies synonym expansion first so
    abbreviations like "ML" or "SWE" match correctly. Then applies a
    title-boost multiplier if the job title contains a key term from the
    resume profile. Returns sorted, filtered list with 'score' field.
    """
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity

    if not jobs:
        return []

    expanded_resume = expand_synonyms(resume_text)
    corpus = [expanded_resume] + [
        expand_synonyms(f"{j['title']} {j['description']}") for j in jobs
    ]

    vectorizer = TfidfVectorizer(stop_words="english", max_features=25000, ngram_range=(1, 2))
    matrix = vectorizer.fit_transform(corpus)
    resume_vec = matrix[0:1]
    job_vecs = matrix[1:]
    sims = cosine_similarity(resume_vec, job_vecs)[0]

    # Extract top-weight terms from resume for title-boost check
    feature_names = vectorizer.get_feature_names_out()
    resume_arr = resume_vec.toarray()[0]
    top_idx = resume_arr.argsort()[-50:][::-1]
    resume_keywords = set(feature_names[i] for i in top_idx if resume_arr[i] > 0)

    scored = []
    for job, score in zip(jobs, sims):
        title_lower = expand_synonyms(job.get("title", ""))
        # Title boost: if any top resume keyword appears in the job title
        if any(kw in title_lower for kw in resume_keywords):
            score = min(score * TITLE_BOOST_MULTIPLIER, 1.0)
        job["score"] = round(float(score), 4)
        scored.append(job)

    scored = [j for j in scored if j["score"] >= SIMILARITY_THRESHOLD]
    scored.sort(key=lambda j: j["score"], reverse=True)
    return scored


def passes_level_filter(job: dict) -> bool:
    if not LEVEL_FILTERS:
        return True
    blob = f"{job.get('title', '')} {job.get('description', '')}".lower()
    return any(lvl in blob for lvl in LEVEL_FILTERS)


def passes_location_filter(job: dict) -> bool:
    """Pass if: remote preferred and job is remote, or location in preferred list."""
    if not PREFER_REMOTE and not PREFERRED_LOCATIONS:
        return True
    loc = (job.get("location") or "").lower()
    if PREFER_REMOTE and ("remote" in loc or not loc):
        return True
    if PREFERRED_LOCATIONS:
        return any(pl.lower() in loc for pl in PREFERRED_LOCATIONS)
    return PREFER_REMOTE and not loc


# -----------------------------------------------------------------------
# DEDUPE STATE (augmented: stores score + timestamp)
# -----------------------------------------------------------------------
def load_seen() -> dict:
    return db.load_seen()


def save_seen(seen: dict):
    db.save_seen(seen)


# -----------------------------------------------------------------------
# NOTIFICATIONS
# -----------------------------------------------------------------------
_SCORE_COLORS = [
    (0.40, "#22c55e"),   # green  — excellent
    (0.25, "#f59e0b"),   # amber  — good
    (0.12, "#6366f1"),   # indigo — decent
    (0.00, "#94a3b8"),   # slate  — low
]


def _score_color(score: float) -> str:
    for threshold, color in _SCORE_COLORS:
        if score >= threshold:
            return color
    return "#94a3b8"


def _score_bar(score: float) -> str:
    pct = int(min(score * 250, 100))  # scale 0-0.4 → 0-100%
    return "█" * (pct // 10) + "░" * (10 - pct // 10)


def build_html_email(job: dict) -> str:
    score = job.get("score", 0)
    color = _score_color(score)
    bar = _score_bar(score)
    title = html.escape(job.get("title", "Unknown"))
    company = html.escape(job.get("company", "Unknown"))
    source = html.escape(job.get("source", ""))
    url = html.escape(_sanitize_external_url(job.get("url", "#")))
    location = html.escape(job.get("location", "") or "Not specified")
    found_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><style>
  body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #0f172a; color: #e2e8f0; margin: 0; padding: 0; }}
  .card {{ max-width: 600px; margin: 32px auto; background: #1e293b; border-radius: 16px; overflow: hidden; box-shadow: 0 8px 32px rgba(0,0,0,0.4); }}
  .header {{ background: linear-gradient(135deg, #1d4ed8, #7c3aed); padding: 28px 32px; }}
  .header h1 {{ margin: 0 0 4px 0; font-size: 22px; color: #fff; }}
  .header p {{ margin: 0; color: #bfdbfe; font-size: 14px; }}
  .body {{ padding: 28px 32px; }}
  .score-badge {{ display: inline-block; background: {color}22; border: 1px solid {color}; color: {color}; padding: 4px 14px; border-radius: 99px; font-size: 13px; font-weight: 600; margin-bottom: 18px; }}
  .field {{ margin-bottom: 12px; }}
  .label {{ font-size: 11px; text-transform: uppercase; letter-spacing: .08em; color: #64748b; margin-bottom: 3px; }}
  .value {{ font-size: 15px; color: #f1f5f9; }}
  .bar {{ font-family: monospace; color: {color}; letter-spacing: 2px; }}
  .apply-btn {{ display: block; text-align: center; margin: 28px 0 0; padding: 14px; background: linear-gradient(90deg, #1d4ed8, #7c3aed); color: #fff; text-decoration: none; border-radius: 10px; font-size: 16px; font-weight: 700; letter-spacing: .03em; }}
  .footer {{ padding: 16px 32px; background: #0f172a; font-size: 11px; color: #475569; text-align: center; }}
</style></head>
<body>
<div class="card">
  <div class="header">
    <h1>🎯 New Job Match Found</h1>
    <p>JENT Job Search Agent · {found_at}</p>
  </div>
  <div class="body">
    <div class="score-badge">Match Score: {score:.1%}</div>
    <div class="field"><div class="label">Position</div><div class="value" style="font-size:19px;font-weight:700;">{title}</div></div>
    <div class="field"><div class="label">Company</div><div class="value">{company}</div></div>
    <div class="field"><div class="label">Location</div><div class="value">{location}</div></div>
    <div class="field"><div class="label">Source</div><div class="value">{source}</div></div>
    <div class="field"><div class="label">Match Strength</div><div class="bar">{bar}</div></div>
    <a href="{url}" class="apply-btn">Apply Now →</a>
  </div>
  <div class="footer">JENT v3 · This match was found by your local job search agent</div>
</div>
</body>
</html>"""


def send_to_zapier(job: dict) -> bool:
    if not ZAPIER_WEBHOOK_URL or "PASTE_YOUR" in ZAPIER_WEBHOOK_URL:
        return False
    payload = {
        "title": job.get("title", ""),
        "company": job.get("company", ""),
        "url": job.get("url", ""),
        "location": job.get("location", ""),
        "source": job.get("source", ""),
        "match_score": job.get("score", 0),
        "found_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = _SESSION.post(ZAPIER_WEBHOOK_URL, json=payload, timeout=15)
        resp.raise_for_status()
        log.info("[OK] Zapier: %.2f  %s @ %s", job["score"], job["title"], job.get("company", ""))
        return True
    except Exception as e:
        log.warning(f"Zapier send failed: {e}")
        return False


def send_via_gmail(job: dict) -> bool:
    if not GMAIL_ADDRESS or not GMAIL_APP_PASSWORD or not GMAIL_TO_ADDRESS:
        return False

    subject = f"Job Match ({job.get('score', 0):.0%}): {job.get('title', '')} @ {job.get('company', '')}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = GMAIL_TO_ADDRESS

    # Plain-text fallback
    plain = (
        f"New job match!\n\n"
        f"Title:    {job.get('title', '')}\n"
        f"Company:  {job.get('company', '')}\n"
        f"Location: {job.get('location', '')}\n"
        f"Source:   {job.get('source', '')}\n"
        f"Score:    {job.get('score', 0):.3f}\n"
        f"Link:     {job.get('url', '')}\n"
        f"Found at: {datetime.now(timezone.utc).isoformat()}\n"
    )
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(build_html_email(job), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as server:
            server.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_ADDRESS, [GMAIL_TO_ADDRESS], msg.as_string())
        log.info(f"[OK] Gmail: {job['score']:.2f}  {job['title']} @ {job.get('company', '')}")
        return True
    except Exception as e:
        log.warning(f"Gmail send failed: {e}")
        return False


def send_via_telegram(job: dict) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    score = job.get("score", 0)
    text = (
        f"New Job Match ({score:.0%})\n\n"
        f"{job.get('title', '')}\n"
        f"Company: {job.get('company', '')}\n"
        f"Location: {job.get('location', '') or 'Remote'}\n"
        f"Source: {job.get('source', '')}\n"
        f"Apply: {_sanitize_external_url(job.get('url', ''))}"
    )
    try:
        resp = _SESSION.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT_ID, "text": text},
            timeout=15,
        )
        resp.raise_for_status()
        log.info(f"[OK] Telegram: {job['title']}")
        return True
    except Exception as e:
        log.warning(f"Telegram send failed: {e}")
        return False


def send_via_discord(job: dict) -> bool:
    if not DISCORD_WEBHOOK_URL:
        return False
    score = job.get("score", 0)
    color = int(_score_color(score).lstrip("#"), 16)
    embed = {
        "title": f"New Match: {job.get('title', '')}",
        "url": _sanitize_external_url(job.get("url", "")),
        "color": color,
        "fields": [
            {"name": "Company", "value": job.get("company", "—"), "inline": True},
            {"name": "Location", "value": job.get("location", "") or "Remote", "inline": True},
            {"name": "Source", "value": job.get("source", ""), "inline": True},
            {"name": "Match Score", "value": f"{score:.1%}", "inline": True},
        ],
        "footer": {"text": "JENT Job Search Agent"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        resp = _SESSION.post(DISCORD_WEBHOOK_URL, json={"embeds": [embed]}, timeout=15)
        resp.raise_for_status()
        log.info(f"[OK] Discord: {job['title']}")
        return True
    except Exception as e:
        log.warning(f"Discord send failed: {e}")
        return False


def notify(job: dict):
    """
    Delivery priority:
      FORCE_DIRECT_GMAIL → Gmail only
      Otherwise: Zapier → Gmail → Telegram → Discord → dry-run print
    Telegram and Discord always fire alongside the primary channel (additive).
    """
    if _DRY_RUN:
        log.info(f"[DRY RUN] {job['score']:.2f}  {job['title']} @ {job.get('company', '')}")
        return

    sent = False
    if FORCE_DIRECT_GMAIL:
        sent = send_via_gmail(job)
    else:
        sent = send_to_zapier(job) or send_via_gmail(job)

    # Always also fire supplemental channels
    send_via_telegram(job)
    send_via_discord(job)

    if not sent and not TELEGRAM_BOT_TOKEN and not DISCORD_WEBHOOK_URL:
        log.info(
            f"[NO DELIVERY CONFIGURED] {job['score']:.2f}  {job['title']} @ {job.get('company', '')} — "
            f"set GMAIL_ADDRESS/GMAIL_APP_PASSWORD, TELEGRAM_BOT_TOKEN, or DISCORD_WEBHOOK_URL"
        )


# -----------------------------------------------------------------------
# ONE CYCLE
# -----------------------------------------------------------------------
def run_once():
    cycle_start = datetime.now(timezone.utc)
    log.info("-" * 60)
    log.info(f"Cycle start: {cycle_start.isoformat()}" + (" [DRY RUN]" if _DRY_RUN else ""))

    resume_text = get_resume_text()
    seen = load_seen()

    log.info("Fetching all sources...")
    all_jobs, source_counts = fetch_all_sources()
    log.info(f"Fetched {len(all_jobs)} total listings.")
    db.save_raw_jobs(all_jobs)

    unseen = [j for j in all_jobs if j["id"] not in seen]
    level_ok = [j for j in unseen if passes_level_filter(j)]
    loc_ok = [j for j in level_ok if passes_location_filter(j)]
    ranked = rank_by_similarity(resume_text, loc_ok)

    log.info(
        f"{len(unseen)} unseen -> {len(level_ok)} pass level filter -> "
        f"{len(loc_ok)} pass location filter -> {len(ranked)} above similarity bar"
    )

    # ── Subscription gate ─────────────────────────────────────────────────────
    # Only send notifications if an active ₹75/month subscription exists.
    if not check_subscription():
        log.info("Notifications skipped — no active subscription.\n")
        # Still persist seen_jobs so we don't re-process the same listings next cycle
        if not _DRY_RUN:
            now_skip = datetime.now(timezone.utc).isoformat()
            for job in all_jobs:
                if job["id"] not in seen:
                    seen[job["id"]] = {
                        "score": job.get("score", 0),
                        "found_at": now_skip,
                        "title": job.get("title", ""),
                        "company": job.get("company", ""),
                        "url": job.get("url", ""),
                        "source": job.get("source", ""),
                        "location": job.get("location", ""),
                    }
            save_seen(seen)
    log.info("Cycle complete at %s.\n", datetime.now(timezone.utc).isoformat())
        return 0

    for job in ranked:
        notify(job)
        # Auto-apply if enabled, platform supported, and score above threshold
        if AUTO_APPLY_ENABLED and job.get("score", 0) >= AUTO_APPLY_THRESHOLD:
            try:
                from scrapers.auto_apply import auto_apply
                auto_apply(
                    job=job,
                    resume_text=resume_text,
                    cover_letter_template=COVER_LETTER_TEMPLATE,
                    enabled_platforms=AUTO_APPLY_PLATFORMS,
                    dry_run=_DRY_RUN,
                )
            except Exception as e:
                log.warning(f"[AutoApply] Error: {e}")
        time.sleep(1)

    now_iso = datetime.now(timezone.utc).isoformat()

    if not _DRY_RUN:
        # Update seen_jobs with augmented info
        for job in all_jobs:
            if job["id"] not in seen:
                seen[job["id"]] = {
                    "score": job.get("score", 0),
                    "found_at": now_iso,
                    "title": job.get("title", ""),
                    "company": job.get("company", ""),
                    "url": job.get("url", ""),
                    "source": job.get("source", ""),
                    "location": job.get("location", ""),
                }
        save_seen(seen)

        # Save cycle stats
        stats = load_cycle_stats()
        stats.append({
            "timestamp": now_iso,
            "total_fetched": len(all_jobs),
            "unseen": len(unseen),
            "passed_level_filter": len(level_ok),
            "passed_location_filter": len(loc_ok),
            "new_matches": len(ranked),
            "sources": source_counts,
        })
        save_cycle_stats(stats)

    log.info("Cycle complete at %s.\n", datetime.now(timezone.utc).isoformat())
    return len(ranked)


# -----------------------------------------------------------------------
# 24/7 LOOP
# -----------------------------------------------------------------------
def run_forever(interval_hours: float):
    log.info(f"Starting 24/7 loop — checking every {interval_hours}h. Ctrl+C to stop.")
    while _RUNNING:
        try:
            run_once()
        except Exception as e:
            log.error(f"Cycle failed, will retry next interval: {e}", exc_info=True)
        for _ in range(int(interval_hours * 3600)):
            if not _RUNNING:
                break
            time.sleep(1)
    log.info("Agent stopped.")


# -----------------------------------------------------------------------
# ENTRY POINT
# -----------------------------------------------------------------------
if __name__ == "__main__":
    if "--help" in sys.argv or "-h" in sys.argv:
        print(__doc__)
        sys.exit(0)

    if _DRY_RUN:
        log.info("DRY RUN mode: no emails sent, seen_jobs.json not updated.")

    if "--once" in sys.argv:
        run_once()
    elif _DRY_RUN:
        # --dry-run without --once: do a single dry-run cycle and exit
        run_once()
    else:
        run_forever(CHECK_INTERVAL_HOURS)

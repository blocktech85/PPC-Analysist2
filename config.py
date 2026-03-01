"""Load configuration from environment.

Config is read from:
- Environment variables (e.g. SERPAPI_API_KEY)
- .env file in project root (same folder as this file), if present

See .env.example and README.md for required/optional keys.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

# Load .env from project root (same folder as config.py). Works regardless of cwd.
BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

# Paths
_db_url = os.environ.get("DATABASE_URL", f"sqlite:///{BASE_DIR / 'ppc.db'}")
if _db_url.startswith("sqlite"):
    DB_PATH = _db_url.replace("sqlite:///", "")
else:
    DB_PATH = str(BASE_DIR / "ppc.db")

# API keys
SERPAPI_API_KEY = os.environ.get("SERPAPI_API_KEY", "")
PAGESPEED_API_KEY = os.environ.get("PAGESPEED_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY", "")

# Flask
SECRET_KEY = os.environ.get("SECRET_KEY", "dev-secret-change-in-production")
DEBUG = os.environ.get("FLASK_DEBUG", "0").lower() in ("1", "true", "yes")

# Scheduler
SCHEDULER_API_ENABLED = os.environ.get("SCHEDULER_API_ENABLED", "0").lower() in ("1", "true", "yes")

"""One-off script to dump raw SerpAPI Ads Transparency Center response. Run from project root with .env set.
   Usage: python scripts/dump_atc_response.py "Nike"
   Then check scripts/atc_response_sample.json for the structure.
"""
import json
import os
import sys

# Load .env and project config
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()

from config import SERPAPI_API_KEY
import requests

def main():
    text = sys.argv[1] if len(sys.argv) > 1 else "Nike"
    region = os.environ.get("ATC_REGION", "US")
    if not SERPAPI_API_KEY:
        print("Set SERPAPI_API_KEY in .env")
        return 1
    url = "https://serpapi.com/search"
    params = {
        "engine": "google_ads_transparency_center",
        "text": text,
        "region": region,
        "num": 100,
        "api_key": SERPAPI_API_KEY,
    }
    print(f"Requesting ATC list: text={text!r}, region={region!r} ...")
    r = requests.get(url, params=params, timeout=60)
    print(f"Status: {r.status_code}")
    try:
        data = r.json()
    except Exception as e:
        print(f"Response is not JSON: {e}")
        print("Body (first 500 chars):", r.text[:500])
        return 1
    keys = list(data.keys()) if isinstance(data, dict) else []
    print(f"Top-level keys: {keys}")
    out_path = os.path.join(os.path.dirname(__file__), "atc_response_sample.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"Full response written to {out_path}")
    return 0

if __name__ == "__main__":
    sys.exit(main())

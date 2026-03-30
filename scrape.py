#!/usr/bin/env python3
"""
GTA Real Estate Market Tracker — Weekly Scraper
Collects market indicators for Oakville and Mississauga.
 
Data Sources:
  - Bank of Canada Valet API  →  policy rate, 5yr bond yield
  - Statistics Canada API     →  national unemployment rate
  - Wahi.com / Zoocasa        →  city-level real estate stats
  - TRREB press releases      →  aggregate GTA stats (fallback)
 
Usage:
  pip install -r requirements.txt
  python scraper/scrape.py
"""
 
import requests
import json
import datetime
import re
import sys
import time
from pathlib import Path
from bs4 import BeautifulSoup
 
DATA_FILE = Path(__file__).parent.parent / "data" / "market_data.json"
 
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}
 
# ─────────────────────────────────────────────────────────────────────────────
# Macro indicators (official APIs)
# ─────────────────────────────────────────────────────────────────────────────
 
def get_boc_rate() -> float | None:
    """
    Bank of Canada target overnight rate.
    Official Valet API — V39079 = Target for the Overnight Rate
    """
    try:
        url = "https://www.bankofcanada.ca/valet/observations/V39079/json?recent=5"
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        obs = r.json()["observations"]
        rate = float(obs[-1]["V39079"]["v"])
        print(f"  BoC rate: {rate}%")
        return rate
    except Exception as e:
        print(f"  [warn] BoC rate fetch failed: {e}")
        return None
 
 
def get_five_year_bond() -> float | None:
    """
    Government of Canada 5-year benchmark bond yield.
    V39063 series from BoC Valet.
    """
    try:
        url = "https://www.bankofcanada.ca/valet/observations/V39063/json?recent=5"
        r = requests.get(url, headers=HEADERS, timeout=12)
        r.raise_for_status()
        obs = r.json()["observations"]
        rate = float(obs[-1]["V39063"]["v"])
        print(f"  5yr bond: {rate}%")
        return rate
    except Exception as e:
        print(f"  [warn] 5yr bond fetch failed: {e}")
        return None
 
 
def get_canada_unemployment() -> float | None:
    """
    Statistics Canada Labour Force Survey — national unemployment rate.
    Table 14-10-0017-01 (seasonally adjusted).
    """
    try:
        # StatsCan JSON API endpoint
        url = (
            "https://www150.statcan.gc.ca/t1/tbl1/en/dtbl!downloadTbl/"
            "csvDownload/14100017.zip"
        )
        # Simpler: scrape their summary page
        page_url = "https://www150.statcan.gc.ca/n1/pub/71-607-x/2018014/lfs-ena.htm"
        r = requests.get(page_url, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.text, "html.parser")
        # Look for unemployment rate in the page
        text = soup.get_text()
        match = re.search(r"unemployment rate[^\d]*(\d+\.\d+)", text, re.I)
        if match:
            rate = float(match.group(1))
            print(f"  Canada unemployment: {rate}%")
            return rate
    except Exception as e:
        print(f"  [warn] StatsCan unemployment fetch failed: {e}")
 
    # Fallback: try the direct table
    try:
        url = "https://www150.statcan.gc.ca/t1/tbl1/en/dtbl!downloadTbl/csvDownload/14100287.zip"
        r = requests.get(url, headers=HEADERS, timeout=20, stream=True)
        # Parse the ZIP/CSV
        import zipfile, io, csv
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = [n for n in zf.namelist() if n.endswith(".csv")][0]
        reader = csv.DictReader(io.TextIOWrapper(zf.open(csv_name), encoding="utf-8-sig"))
        rows = list(reader)
        # Find national, both sexes, unemployment rate, most recent
        for row in reversed(rows):
            geo = row.get("GEO", "")
            stat = row.get("Labour force characteristics", "")
            sex = row.get("Sex", "")
            if "Canada" in geo and "Unemployment" in stat and "Both" in sex:
                val = row.get("VALUE", "")
                if val:
                    rate = float(val)
                    print(f"  Canada unemployment (StatsCan CSV): {rate}%")
                    return rate
    except Exception as e:
        print(f"  [warn] StatsCan CSV fallback failed: {e}")
 
    return None
 
 
# ─────────────────────────────────────────────────────────────────────────────
# City-level real estate data
# ─────────────────────────────────────────────────────────────────────────────
 
def scrape_wahi(city_slug: str) -> dict | None:
    """
    Scrape Wahi.com market trends page for a given city.
    Returns dict with avg_price, sales, new_listings, dom, etc.
    """
    url = f"https://wahi.com/market-trends/ontario/{city_slug}/"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
        text = soup.get_text(" ", strip=True)
 
        stats = {}
 
        # Average price pattern: "$1,234,567" or "$923,456"
        price_match = re.search(r"Average(?:\s+Sale)?\s+Price[^\$]*\$([\d,]+)", text, re.I)
        if price_match:
            stats["avg_price"] = int(price_match.group(1).replace(",", ""))
 
        # Days on market
        dom_match = re.search(r"Days?\s+on\s+Market[^\d]*(\d+)", text, re.I)
        if dom_match:
            stats["dom"] = int(dom_match.group(1))
 
        # Active listings
        active_match = re.search(r"Active\s+Listings?[^\d]*(\d[\d,]*)", text, re.I)
        if active_match:
            stats["active_listings"] = int(active_match.group(1).replace(",", ""))
 
        # New listings
        new_match = re.search(r"New\s+Listings?[^\d]*(\d[\d,]*)", text, re.I)
        if new_match:
            stats["new_listings"] = int(new_match.group(1).replace(",", ""))
 
        # Sales / transactions
        sales_match = re.search(r"(?:Sales|Transactions)[^\d]*(\d[\d,]*)", text, re.I)
        if sales_match:
            stats["sales"] = int(sales_match.group(1).replace(",", ""))
 
        if stats:
            # Derive SNLR and MOI if we have the inputs
            if "sales" in stats and "new_listings" in stats and stats["new_listings"] > 0:
                stats["snlr"] = round(stats["sales"] / stats["new_listings"], 3)
            if "active_listings" in stats and "sales" in stats and stats["sales"] > 0:
                stats["moi"] = round(stats["active_listings"] / stats["sales"], 1)
 
            print(f"  Wahi {city_slug}: {stats}")
            return stats
 
    except Exception as e:
        print(f"  [warn] Wahi {city_slug} failed: {e}")
    return None
 
 
def scrape_zoocasa(city_slug: str) -> dict | None:
    """
    Fallback: scrape Zoocasa community page for market stats.
    """
    url = f"https://www.zoocasa.com/{city_slug}-on-real-estate"
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        soup = BeautifulSoup(r.text, "html.parser")
 
        stats = {}
 
        # Look for JSON-LD structured data
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                data = json.loads(script.string)
                if "priceRange" in str(data):
                    # Try to extract price data
                    text = json.dumps(data)
                    price_m = re.search(r'"price"[:\s]*"?\$?([\d,]+)"?', text)
                    if price_m:
                        stats["avg_price"] = int(price_m.group(1).replace(",", ""))
            except Exception:
                pass
 
        # Fallback: text scraping
        text = soup.get_text(" ", strip=True)
        price_m = re.search(r"Avg(?:erage)?\s+(?:Sale\s+)?Price[^\$]*\$([\d,]+)", text, re.I)
        if price_m:
            stats["avg_price"] = int(price_m.group(1).replace(",", ""))
 
        dom_m = re.search(r"(\d+)\s+days?\s+on\s+market", text, re.I)
        if dom_m:
            stats["dom"] = int(dom_m.group(1))
 
        if stats:
            print(f"  Zoocasa {city_slug}: {stats}")
            return stats
 
    except Exception as e:
        print(f"  [warn] Zoocasa {city_slug} failed: {e}")
    return None
 
 
CITY_SCRAPE_CONFIGS = {
    "oakville": {
        "wahi_slug": "oakville",
        "zoocasa_slug": "oakville",
    },
    "mississauga": {
        "wahi_slug": "mississauga",
        "zoocasa_slug": "mississauga",
    },
}
 
 
def get_city_stats(city: str, last_entry: dict | None) -> dict:
    """
    Try multiple sources to get city-level real estate stats.
    Falls back to last known data + flag as estimated.
    """
    cfg = CITY_SCRAPE_CONFIGS[city]
    data = None
 
    # 1. Try Wahi
    print(f"\n  Trying Wahi for {city}...")
    data = scrape_wahi(cfg["wahi_slug"])
    time.sleep(1.5)
 
    # 2. Try Zoocasa
    if not data or len(data) < 3:
        print(f"  Trying Zoocasa for {city}...")
        data = scrape_zoocasa(cfg["zoocasa_slug"])
        time.sleep(1.5)
 
    # 3. Fall back to last entry with a note
    if not data and last_entry and city in last_entry:
        print(f"  [warn] Using last known data for {city} (mark as estimated)")
        data = dict(last_entry[city])
        data["_estimated"] = True
        return data
 
    if not data:
        data = {}
 
    # Fill any missing fields from last entry
    if last_entry and city in last_entry:
        prev = last_entry[city]
        for key in ["avg_price", "new_listings", "sales", "active_listings", "dom", "snlr", "moi"]:
            if key not in data and key in prev:
                data[key] = prev[key]
                data["_estimated"] = True
 
    return data
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Data file management
# ─────────────────────────────────────────────────────────────────────────────
 
def load_data() -> dict:
    """Load existing data JSON, or return empty structure."""
    if DATA_FILE.exists():
        with open(DATA_FILE) as f:
            return json.load(f)
    return {
        "metadata": {
            "last_updated": "",
            "sources": ["Bank of Canada Valet API", "Statistics Canada LFS",
                        "Wahi.com", "Zoocasa.com"],
            "version": "1.0",
            "note": "Monthly data. SNLR = sales/new_listings. MOI = active_listings/sales."
        },
        "monthly": []
    }
 
 
def save_data(data: dict):
    """Save data JSON, pretty-printed."""
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nSaved → {DATA_FILE}")
 
 
def get_current_month() -> str:
    return datetime.date.today().strftime("%Y-%m")
 
 
def entry_exists(data: dict, month: str) -> bool:
    return any(e["month"] == month for e in data.get("monthly", []))
 
 
# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
 
def main():
    print("=" * 60)
    print("GTA Market Tracker — Weekly Scrape")
    print(f"Date: {datetime.date.today()}")
    print("=" * 60)
 
    data = load_data()
    month = get_current_month()
 
    if entry_exists(data, month):
        print(f"\nEntry for {month} already exists. Updating...")
        # Remove existing entry to refresh it
        data["monthly"] = [e for e in data["monthly"] if e["month"] != month]
 
    last_entry = data["monthly"][-1] if data["monthly"] else None
 
    # ── Macro indicators ──────────────────────────────────────
    print("\n[1/3] Fetching macro indicators...")
    boc_rate = get_boc_rate()
    five_yr = get_five_year_bond()
    unemployment = get_canada_unemployment()
 
    # Fall back to last known if APIs fail
    if boc_rate is None and last_entry:
        boc_rate = last_entry.get("boc_rate")
        print(f"  Using last known BoC rate: {boc_rate}%")
    if unemployment is None and last_entry:
        unemployment = last_entry.get("unemployment")
        print(f"  Using last known unemployment: {unemployment}%")
 
    # ── City-level data ───────────────────────────────────────
    print("\n[2/3] Fetching Oakville stats...")
    oakville = get_city_stats("oakville", last_entry)
 
    print("\n[3/3] Fetching Mississauga stats...")
    mississauga = get_city_stats("mississauga", last_entry)
 
    # ── Build new entry ───────────────────────────────────────
    new_entry = {
        "month": month,
        "boc_rate": boc_rate,
        "five_yr_bond": five_yr,
        "unemployment": unemployment,
        "oakville": oakville,
        "mississauga": mississauga,
    }
 
    data["monthly"].append(new_entry)
    data["monthly"].sort(key=lambda x: x["month"])
    data["metadata"]["last_updated"] = datetime.date.today().isoformat()
 
    save_data(data)
 
    # ── Print signal summary ──────────────────────────────────
    print("\n" + "=" * 60)
    print("SIGNAL SUMMARY — Oakville")
    print("=" * 60)
    ok = oakville
    signals_ok = 0
    def sig(name, val, threshold, direction="above"):
        nonlocal signals_ok
        if val is None:
            print(f"  {name}: N/A")
            return
        hit = val > threshold if direction == "above" else val < threshold
        icon = "🟢" if hit else "🔴"
        if hit: signals_ok += 1
        print(f"  {icon} {name}: {val} (threshold: {'>' if direction=='above' else '<'}{threshold})")
 
    sig("SNLR", ok.get("snlr"), 0.50, "above")
    sig("MOI", ok.get("moi"), 3.0, "below")
    sig("DOM", ok.get("dom"), 22, "below")
    print(f"\n  Bottom signals lit: {signals_ok}/5")
    print("=" * 60)
    print("Done.\n")
 
 
if __name__ == "__main__":
    main()
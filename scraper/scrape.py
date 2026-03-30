#!/usr/bin/env python3
"""
GTA Real Estate Market Tracker — Scraper v3
Fixes: StatsCan URL, Wahi garbage values, strict validation.

Usage:
  python scrape.py              # auto (BoC + best-effort RE scrape)
  python scrape.py --manual     # recommended: enter TRREB data manually
  python scrape.py --diagnose   # test each source, no save
  python scrape.py --month 2026-04
"""

import requests, json, datetime, re, time, zipfile, io, csv, argparse
from pathlib import Path

DATA_FILE = Path(__file__).parent.parent / "data" / "market_data.json"

S = requests.Session()
S.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-CA,en;q=0.9",
    "Referer": "https://www.google.ca/",
})

def ok(msg):   print(f"    [OK  ] {msg}")
def warn(msg): print(f"    [WARN] {msg}")
def fail(msg): print(f"    [FAIL] {msg}")
def info(msg): print(f"    [    ] {msg}")

# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers — reject obviously wrong scraped values
# ─────────────────────────────────────────────────────────────────────────────

def valid_price(v):
    """Oakville/Mississauga prices: $400K–$4M"""
    return isinstance(v, (int, float)) and 400_000 <= v <= 4_000_000

def valid_dom(v):
    """Days on market: 1–180"""
    return isinstance(v, (int, float)) and 1 <= v <= 180

def valid_listings(v):
    """Listing counts: 10–9999"""
    return isinstance(v, (int, float)) and 10 <= v <= 9_999

def valid_sales(v):
    """Sales: 10–9999"""
    return isinstance(v, (int, float)) and 10 <= v <= 9_999

def sanitize(stats: dict) -> dict:
    """Remove fields that failed validation."""
    out = {}
    checks = {
        "avg_price":      valid_price,
        "dom":            valid_dom,
        "new_listings":   valid_listings,
        "active_listings":valid_listings,
        "sales":          valid_sales,
    }
    for k, v in stats.items():
        if k.startswith("_"):
            out[k] = v
        elif k in checks:
            if checks[k](v):
                out[k] = v
            else:
                warn(f"  rejected {k}={v} (out of valid range)")
        else:
            out[k] = v
    return out

def derive_ratios(stats: dict):
    if "sales" in stats and "new_listings" in stats and stats["new_listings"] > 0:
        stats["snlr"] = round(stats["sales"] / stats["new_listings"], 3)
    if "active_listings" in stats and "sales" in stats and stats["sales"] > 0:
        stats["moi"] = round(stats["active_listings"] / stats["sales"], 1)


# ─────────────────────────────────────────────────────────────────────────────
# Bank of Canada Valet API  (reliable — keep as-is)
# ─────────────────────────────────────────────────────────────────────────────

def boc_series(sid: str, label: str) -> float | None:
    url = f"https://www.bankofcanada.ca/valet/observations/{sid}/json?recent=10"
    try:
        r = S.get(url, timeout=12); r.raise_for_status()
        for entry in reversed(r.json().get("observations", [])):
            cell = entry.get(sid, {}); raw = cell.get("v") if isinstance(cell, dict) else None
            if raw and str(raw).strip() not in ("", ".", "nan"):
                v = float(raw); ok(f"BoC {label}: {v}%  [{sid}]"); return v
        warn(f"BoC {label}: no non-null observations")
    except Exception as e:
        fail(f"BoC {label}: {e}")
    return None

def get_boc_rate():    return boc_series("V39079",    "overnight rate")
def get_bond_5yr():    return boc_series("V122487",   "5yr bond")

# ─────────────────────────────────────────────────────────────────────────────
# Statistics Canada — unemployment rate
# Uses WDS getLatestNDataPointsForVector (JSON, no ZIP needed)
# Vector 2062815 = Canada, both sexes, unemployment rate, SA
# ─────────────────────────────────────────────────────────────────────────────

STATSCAN_VECTORS = [
    ("2062815",  "LFS unemp SA (both sexes)"),
    ("62426466", "LFS unemp SA alt vector"),
]

def get_unemployment() -> float | None:
    # Method 1: WDS vector JSON API (no ZIP, no auth needed)
    for vec_id, label in STATSCAN_VECTORS:
        url = (
            "https://www150.statcan.gc.ca/t1/tbl1/en/"
            f"dtbl!downloadTbl/csvDownload/14100287.zip"  # kept as fallback trigger
        )
        # Use the proper vector endpoint instead
        api_url = (
            f"https://www150.statcan.gc.ca/t1/tbl1/en/"
            f"dtbl!getLatestNDataPointsForVector/{vec_id}/1"
        )
        try:
            r = S.get(api_url, timeout=15); r.raise_for_status()
            data = r.json()
            # Response: {"status":"SUCCESS","object":[{"refPer":"...","value":6.7}]}
            items = data.get("object", [])
            if items:
                val = float(items[0]["value"])
                period = items[0].get("refPer", "")
                ok(f"StatsCan {label}: {val}%  (period: {period})")
                return val
        except Exception as e:
            warn(f"StatsCan WDS vector {vec_id}: {e}")

    # Method 2: StatsCan CSV download with corrected table ID format
    for table_id in ["14100017", "14100287"]:
        url = (
            f"https://www150.statcan.gc.ca/t1/tbl1/en/"
            f"dtbl!downloadTbl/csvDownload/{table_id}.zip"
        )
        try:
            info(f"StatsCan: trying CSV zip table {table_id}...")
            r = S.get(url, timeout=30); r.raise_for_status()
            if "html" in r.headers.get("Content-Type", ""):
                raise ValueError("Got HTML instead of ZIP")
            zf = zipfile.ZipFile(io.BytesIO(r.content))
            csvf = [n for n in zf.namelist() if n.endswith(".csv") and "Meta" not in n]
            if not csvf: raise ValueError("No CSV in ZIP")
            rows = []
            reader = csv.DictReader(io.TextIOWrapper(zf.open(csvf[0]), encoding="utf-8-sig"))
            for row in reader:
                if (
                    "Canada" in row.get("GEO", "")
                    and "Unemployment" in row.get("Labour force characteristics", "")
                    and "Both" in row.get("Sex", "")
                    and "Seasonally" in row.get("Data type", "")
                ):
                    v = row.get("VALUE", "").strip()
                    if v: rows.append((row.get("REF_DATE",""), float(v)))
            if rows:
                rows.sort(); date, val = rows[-1]
                ok(f"StatsCan CSV table {table_id}: {val}%  (period: {date})")
                return val
        except zipfile.BadZipFile:
            warn(f"StatsCan table {table_id}: not a ZIP — may be blocked")
        except Exception as e:
            warn(f"StatsCan table {table_id}: {e}")

    # Method 3: Scrape a simple public page with the number
    try:
        from bs4 import BeautifulSoup
        # Trading Economics Canada unemployment page (public, no auth)
        r = S.get(
            "https://tradingeconomics.com/canada/unemployment-rate",
            timeout=15,
            headers={**S.headers, "Accept": "text/html"},
        )
        soup = BeautifulSoup(r.text, "html.parser")
        # TE shows the rate prominently in a <span> with id or class
        for tag in soup.find_all(["span", "td", "div"]):
            txt = tag.get_text(strip=True)
            if re.match(r"^\d+\.\d+$", txt):
                val = float(txt)
                if 3.0 <= val <= 15.0:  # plausible unemployment range
                    ok(f"Unemployment (Trading Economics): {val}%")
                    return val
    except Exception as e:
        warn(f"Trading Economics scrape: {e}")

    fail("All unemployment sources failed — will carry forward last known value")
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Real estate scrapers — strict validation applied to all
# ─────────────────────────────────────────────────────────────────────────────

def _price_from_text(text: str) -> int | None:
    """Extract avg sale price — must look like a GTA home price ($400K–$3M)."""
    patterns = [
        r"[Aa]verage\s+(?:[Ss]ale\s+|[Ss]old\s+)?[Pp]rice[^$\d]{0,30}\$([\d,]+)",
        r"[Aa]vg\.?\s+[Pp]rice[^$\d]{0,20}\$([\d,]+)",
        r"\$([\d,]+)\s*(?:is\s+the\s+)?[Aa]verage\s+(?:home\s+|sale\s+|sold\s+)?[Pp]rice",
        r"[Mm]edian\s+[Pp]rice[^$\d]{0,20}\$([\d,]+)",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text):
            v = int(m.group(1).replace(",", ""))
            if valid_price(v):
                return v
    return None

def _dom_from_text(text: str) -> int | None:
    """Extract days on market — must be 1–180."""
    patterns = [
        r"[Aa]verage\s+[Dd]ays?\s+[Oo]n\s+[Mm]arket[^\d]{0,15}(\d{1,3})",
        r"[Dd]ays?\s+[Oo]n\s+[Mm]arket[^\d]{0,10}(\d{1,3})",
        r"(\d{1,3})\s+[Dd]ays?\s+[Oo]n\s+[Mm]arket",
        r"\bDOM\b[^\d]{0,10}(\d{1,3})",
    ]
    for pat in patterns:
        for m in re.finditer(pat, text):
            v = int(m.group(1))
            if valid_dom(v):
                return v
    return None

def _count_from_text(text: str, field: str) -> int | None:
    """Extract listing/sales counts — must be 10–9999."""
    PAT_MAP = {
        "new_listings":    [
            r"[Nn]ew\s+[Ll]istings?[^\d]{0,15}(\d[\d,]{1,4})",
            r"(\d[\d,]{1,4})\s+[Nn]ew\s+[Ll]istings?",
        ],
        "active_listings": [
            r"[Aa]ctive\s+[Ll]istings?[^\d]{0,15}(\d[\d,]{1,4})",
            r"[Hh]omes?\s+[Ff]or\s+[Ss]ale[^\d]{0,15}(\d[\d,]{1,4})",
        ],
        "sales": [
            r"[Tt]otal\s+[Ss]ales?[^\d]{0,10}(\d[\d,]{1,4})",
            r"(\d[\d,]{1,4})\s+[Hh]omes?\s+[Ss]old",
            r"[Ss]ales?[^\d]{0,10}(\d[\d,]{1,4})\b",
        ],
    }
    for pat in PAT_MAP.get(field, []):
        for m in re.finditer(pat, text):
            v = int(m.group(1).replace(",", ""))
            if valid_listings(v):
                return v
    return None

def _extract_next_data(soup, stats: dict):
    """Pull from Next.js __NEXT_DATA__ JSON blob."""
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string: return
    blob = script.string
    PAIRS = {
        "avg_price":       [r'"averagePrice"\s*:\s*(\d+)', r'"avgSalePrice"\s*:\s*(\d+)'],
        "dom":             [r'"averageDom"\s*:\s*(\d+)',   r'"avgDaysOnMarket"\s*:\s*(\d+)'],
        "new_listings":    [r'"newListings"\s*:\s*(\d+)'],
        "active_listings": [r'"activeListings"\s*:\s*(\d+)'],
        "sales":           [r'"(?:totalSales|numSales|salesCount)"\s*:\s*(\d+)'],
    }
    VALID = {
        "avg_price": valid_price, "dom": valid_dom,
        "new_listings": valid_listings, "active_listings": valid_listings, "sales": valid_sales,
    }
    for key, pats in PAIRS.items():
        if key in stats: continue
        for pat in pats:
            m = re.search(pat, blob)
            if m:
                v = int(m.group(1))
                if VALID[key](v):
                    stats[key] = v
                break

def _parse_page(city: str, r, source: str) -> dict | None:
    from bs4 import BeautifulSoup
    if r.status_code != 200:
        warn(f"{source} {city}: HTTP {r.status_code}")
        return None
    soup = BeautifulSoup(r.text, "html.parser")
    text = soup.get_text(" ", strip=True)
    if len(text) < 300 or "Access Denied" in text or "captcha" in text.lower():
        warn(f"{source} {city}: blocked / page too short ({len(text)} chars)")
        return None

    stats: dict = {}
    p = _price_from_text(text)
    if p: stats["avg_price"] = p
    d = _dom_from_text(text)
    if d: stats["dom"] = d
    for field in ("new_listings", "active_listings", "sales"):
        v = _count_from_text(text, field)
        if v: stats[field] = v
    _extract_next_data(soup, stats)
    derive_ratios(stats)

    good = len([k for k in stats if not k.startswith("_")])
    if good >= 2:
        ok(f"{source} {city}: {stats}")
        return stats
    warn(f"{source} {city}: only {good} valid field(s) after validation — {stats}")
    return None


SOURCES = {
    "oakville": [
        ("Wahi",        "https://wahi.com/market-trends/ontario/oakville/"),
        ("Zolo",        "https://www.zolo.ca/ontario/oakville/real-estate-statistics"),
        ("HouseSigma",  "https://housesigma.com/ontario/oakville/"),
        ("Realtor.ca",  "https://www.realtor.ca/map#ZoomLevel=12&Center=43.467517,-79.687439&LatitudeMax=43.585939&LongitudeMax=-79.499969&LatitudeMin=43.349087&LongitudeMin=-79.874909&view=List&ContentType=All&TransactionTypeId=2&PropertyTypeGroupID=1&Currency=CAD"),
    ],
    "mississauga": [
        ("Wahi",        "https://wahi.com/market-trends/ontario/mississauga/"),
        ("Zolo",        "https://www.zolo.ca/ontario/mississauga/real-estate-statistics"),
        ("HouseSigma",  "https://housesigma.com/ontario/mississauga/"),
    ],
}

def get_city_stats(city: str, last_entry: dict | None) -> dict:
    best: dict = {}
    for source_name, url in SOURCES[city]:
        print(f"      -> {source_name}...")
        try:
            r = S.get(url, timeout=18, allow_redirects=True)
        except Exception as e:
            fail(f"{source_name} {city}: {type(e).__name__}: {e}")
            time.sleep(1.0)
            continue

        result = _parse_page(city, r, source_name) or {}
        time.sleep(1.5)

        if len(result) > len(best):
            best = result

        # Sufficient: have price + (snlr or moi) + dom
        if best.get("avg_price") and best.get("dom") and (best.get("snlr") or best.get("moi")):
            break

    # Fill remaining gaps from last known month
    if last_entry and city in last_entry:
        prev = last_entry[city]
        for key in ("avg_price", "new_listings", "sales", "active_listings", "dom", "snlr", "moi"):
            if key not in best and key in prev:
                best[key] = prev[key]
                best["_has_estimated_fields"] = True

    # All scrapers failed — carry forward entire last record
    if not best.get("avg_price"):
        warn(f"{city}: all scrapers failed — carrying forward last month")
        if last_entry and city in last_entry:
            best = dict(last_entry[city])
            best["_estimated"] = True

    return best


# ─────────────────────────────────────────────────────────────────────────────
# Data file helpers
# ─────────────────────────────────────────────────────────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE) as f: return json.load(f)
    return {"metadata": {"last_updated": "", "version": "3.0"}, "monthly": []}

def save_data(data: dict):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w") as f: json.dump(data, f, indent=2)
    ok(f"Saved -> {DATA_FILE}")


# ─────────────────────────────────────────────────────────────────────────────
# Manual entry (recommended when sites are blocking)
# ─────────────────────────────────────────────────────────────────────────────

def manual_entry(month: str) -> dict:
    print(f"""
{'='*60}
MANUAL ENTRY — {month}
{'='*60}
Get data from TRREB Market Watch:
  https://trreb.ca/index.php/market-news/market-watch
  -> Download the latest PDF, find Oakville (W13) and Mississauga (W05) rows

BoC rate (auto): https://www.bankofcanada.ca/core-functions/monetary-policy/key-interest-rate/
Unemployment   : https://www150.statcan.gc.ca/n1/pub/71-607-x/2018014/lfs-ena.htm
                 OR Google "Canada unemployment rate {month}"
{'='*60}
Press Enter to skip any field.
""")

    def ask(prompt, typ=float):
        while True:
            s = input(f"  {prompt}: ").strip()
            if not s: return None
            try:   return typ(s)
            except ValueError: print("    Invalid — try again or press Enter to skip")

    boc    = ask("BoC overnight rate % (e.g. 2.25)")
    bond   = ask("5yr GoC bond yield % (e.g. 3.60)")
    unemp  = ask("Canada unemployment % (e.g. 6.7)")
    result = {"boc_rate": boc, "five_yr_bond": bond, "unemployment": unemp}

    for city in ("oakville", "mississauga"):
        print(f"\n  ── {city.title()} ──")
        cd: dict = {}
        p  = ask(f"  Avg sold price $ (e.g. 1280000)", int)
        nl = ask(f"  New listings",   int)
        sa = ask(f"  Sales (closed)", int)
        al = ask(f"  Active listings", int)
        dm = ask(f"  Avg days on market", int)
        if p:  cd["avg_price"]       = p
        if nl: cd["new_listings"]    = nl
        if sa: cd["sales"]           = sa
        if al: cd["active_listings"] = al
        if dm: cd["dom"]             = dm
        derive_ratios(cd)
        result[city] = cd
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Diagnose — test each source, print result, do not save
# ─────────────────────────────────────────────────────────────────────────────

def diagnose():
    print(f"\n{'='*60}\nDIAGNOSE MODE — testing all sources\n{'='*60}")
    print("\n[BoC overnight rate]");  get_boc_rate()
    print("\n[BoC 5yr bond]");        get_bond_5yr()
    print("\n[StatsCan unemployment]"); get_unemployment()
    for city in ("oakville", "mississauga"):
        for name, url in SOURCES[city]:
            print(f"\n[{name} — {city}]  {url}")
            try:
                r = S.get(url, timeout=18)
                result = _parse_page(city, r, name)
                if not result: print(f"    (no usable data)")
            except Exception as e:
                fail(f"{name} {city}: {e}")
            time.sleep(1.0)
    print(f"\n{'='*60}\nDiagnose complete.")


# ─────────────────────────────────────────────────────────────────────────────
# Signal summary
# ─────────────────────────────────────────────────────────────────────────────

def print_summary(entry: dict):
    print(f"\n{'='*60}\nSIGNAL SUMMARY\n{'='*60}")
    total = 0
    for city in ("oakville", "mississauga"):
        cd = entry.get(city, {})
        print(f"\n  {city.title()}")
        def sig(name, val, thr, op=">="):
            nonlocal total
            if val is None: print(f"    [--] {name}: N/A"); return
            hit = val >= thr if op == ">=" else val <= thr
            icon = "[GO]" if hit else "[NO]"
            if hit: total += 1
            print(f"    {icon} {name}: {val}  (target: {op}{thr})")
        sig("SNLR", cd.get("snlr"), 0.50, ">=")
        sig("MOI",  cd.get("moi"),  3.0,  "<=")
        sig("DOM",  cd.get("dom"),  22,   "<=")

    boc = entry.get("boc_rate")
    print(f"\n  Macro")
    icon = "[GO]" if (boc and boc < 2.0) else "[NO]"
    print(f"    {icon} BoC rate: {boc}%  (target: <=2.0%)")
    print(f"\n  Bottom signals: {total}/7  (need 5+ for high confidence)")

    estimated = [c for c in ("oakville","mississauga")
                 if entry.get(c,{}).get("_estimated") or entry.get(c,{}).get("_has_estimated_fields")]
    if estimated:
        print(f"\n  NOTE: {', '.join(c.title() for c in estimated)} data is estimated.")
        print(f"  For accurate data run:  python scrape.py --manual")
        print(f"  TRREB source: https://trreb.ca/index.php/market-news/market-watch")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manual",   action="store_true", help="Interactive manual data entry")
    ap.add_argument("--diagnose", action="store_true", help="Test all sources, no save")
    ap.add_argument("--month",    type=str, default=None, help="Override month (YYYY-MM)")
    args = ap.parse_args()

    if args.diagnose:
        diagnose(); return

    print("=" * 60)
    print("GTA Market Tracker — Scraper v3")
    print(f"Date: {datetime.date.today()}")
    print("=" * 60)

    data       = load_data()
    month      = args.month or datetime.date.today().strftime("%Y-%m")
    last_entry = data["monthly"][-1] if data["monthly"] else None
    data["monthly"] = [e for e in data["monthly"] if e["month"] != month]

    if args.manual:
        fields    = manual_entry(month)
        new_entry = {"month": month, **fields}
    else:
        print(f"\nTarget month: {month}")

        print("\n[1/3] Macro indicators")
        boc_rate = get_boc_rate()
        five_yr  = get_bond_5yr()
        unemp    = get_unemployment()

        if boc_rate is None and last_entry:
            boc_rate = last_entry.get("boc_rate"); warn(f"BoC: carrying forward {boc_rate}%")
        if unemp is None and last_entry:
            unemp = last_entry.get("unemployment"); warn(f"Unemployment: carrying forward {unemp}%")

        print("\n[2/3] Oakville")
        oakville = get_city_stats("oakville", last_entry)

        print("\n[3/3] Mississauga")
        mississauga = get_city_stats("mississauga", last_entry)

        new_entry = {
            "month": month, "boc_rate": boc_rate,
            "five_yr_bond": five_yr, "unemployment": unemp,
            "oakville": oakville, "mississauga": mississauga,
        }

    data["monthly"].append(new_entry)
    data["monthly"].sort(key=lambda x: x["month"])
    data["metadata"]["last_updated"] = datetime.date.today().isoformat()
    save_data(data)
    print_summary(new_entry)


if __name__ == "__main__":
    main()
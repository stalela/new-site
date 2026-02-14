#!/usr/bin/env python3
"""
Yep.co.za Store Scraper — Adaptive OLC Grid Edition
====================================================
Uses Open Location Code (Plus Codes) to recursively subdivide geographic areas
when the API's 10,000-result cap is hit. Each OLC cell is queried individually;
if a cell still exceeds 10K results it's subdivided into finer cells.

Resolution ladder (code_length → approx cell size):
  4 → ~111 km   (1° × 1°)
  6 → ~5.5 km   (0.05° × 0.05°)
  8 → ~278 m    (0.0025° × 0.0025°)

API constraints:
  - Max distance: 200 km
  - Results cap: ~10,000 per query (returns dupes beyond that)
"""

import requests
import json
import time
import os
import math
import csv
from datetime import datetime
from openlocationcode import openlocationcode as olc

# ── API config ────────────────────────────────────────────────────────────────

BASE_URL = "https://fm.mall.yep.co.za"
SEARCH_URL = f"{BASE_URL}/api/seller/searchStore"
HEADERS = {
    "Content-Type": "application/json",
    "Origin": "https://mall.yep.co.za",
    "Referer": "https://mall.yep.co.za/",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
}
PAGE_SIZE = 100
MAX_API_RESULTS = 10_000  # API stops returning new results after ~10K
MAX_DISTANCE_KM = 200

# ── OLC resolution ladder ────────────────────────────────────────────────────
# Start coarse, refine when a cell exceeds MAX_API_RESULTS.
# code_length 4 = ~111 km cell,  6 = ~5.5 km,  8 = ~278 m
OLC_LEVELS = [4, 6, 8]
MAX_OLC_DEPTH = len(OLC_LEVELS) - 1  # deepest index into OLC_LEVELS

# ── Entry-point centers covering South Africa ────────────────────────────────
GEO_CENTERS = [
    {"name": "Johannesburg", "lat": -26.1834392, "lng": 28.0630169},
    {"name": "Cape Town",    "lat": -33.9249,    "lng": 18.4241},
    {"name": "Durban",       "lat": -29.8587,    "lng": 31.0218},
    {"name": "Port Elizabeth","lat": -33.9608,    "lng": 25.6022},
    {"name": "Bloemfontein", "lat": -29.0852,    "lng": 26.1596},
    {"name": "Polokwane",    "lat": -23.9045,    "lng": 29.4689},
    {"name": "Nelspruit",    "lat": -25.4745,    "lng": 30.9703},
    {"name": "Upington",     "lat": -28.4478,    "lng": 21.2561},
    {"name": "George",       "lat": -33.9631,    "lng": 22.4617},
    {"name": "Kimberley",    "lat": -28.7282,    "lng": 24.7499},
]

# ── Output paths ──────────────────────────────────────────────────────────────
OUTPUT_DIR = "output"
JSON_OUT = os.path.join(OUTPUT_DIR, "yep_stores_full.json")
CSV_OUT = os.path.join(OUTPUT_DIR, "yep_stores_full.csv")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "yep_olc_progress.json")

# ── Global state ──────────────────────────────────────────────────────────────
unique_stores: dict = {}  # sellerId → store dict
api_calls = 0
start_time = None
last_save_count = 0  # track when we last saved (every 1000 new stores)
SAVE_EVERY = 1000


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def haversine_km(lat1, lon1, lat2, lon2):
    """Distance in km between two lat/lng points."""
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def cell_radius_km(code_area):
    """Approximate radius (half-diagonal) of an OLC CodeArea in km."""
    diag = haversine_km(
        code_area.latitudeLo, code_area.longitudeLo,
        code_area.latitudeHi, code_area.longitudeHi,
    )
    return diag / 2


def api_post(url, payload, retries=3, backoff=2):
    """POST with retries and exponential backoff."""
    global api_calls
    for attempt in range(retries):
        try:
            api_calls += 1
            resp = requests.post(url, json=payload, headers=HEADERS, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get("code") == "10000":
                return data
            else:
                print(f"  ⚠ API error: {data.get('message', 'unknown')}")
                return None
        except Exception as e:
            wait = backoff ** (attempt + 1)
            print(f"  ⚠ Request failed ({e}), retry {attempt+1}/{retries} in {wait}s")
            time.sleep(wait)
    return None


# ═══════════════════════════════════════════════════════════════════════════════
# Core API calls
# ═══════════════════════════════════════════════════════════════════════════════

def probe_count(lat, lng, distance_km):
    """
    Hit page 1 to get the reported total for this center + radius.
    Returns (total, first_page_stores).
    """
    payload = {
        "page": {"pageNo": 1, "pageSize": PAGE_SIZE},
        "categoryId": "",
        "keyword": "",
        "distance": int(distance_km),
        "longitude": float(lng),
        "latitude": float(lat),
    }
    data = api_post(SEARCH_URL, payload)
    if not data:
        return 0, []
    result = data.get("data", {})
    total = int(result.get("total", 0))
    stores = result.get("list", [])
    return total, stores


def scrape_all_pages(lat, lng, distance_km, label=""):
    """
    Paginate through ALL pages for a given center+radius.
    Returns list of store dicts (may contain dupes across calls;
    dedup happens in the global dict).
    """
    collected = []
    page_no = 1
    total = None
    max_pages = MAX_API_RESULTS // PAGE_SIZE  # 100 pages max useful

    while page_no <= max_pages:
        payload = {
            "page": {"pageNo": page_no, "pageSize": PAGE_SIZE},
            "categoryId": "",
            "keyword": "",
            "distance": int(distance_km),
            "longitude": float(lng),
            "latitude": float(lat),
        }
        data = api_post(SEARCH_URL, payload)
        if not data:
            break
        result = data.get("data", {})
        if total is None:
            total = int(result.get("total", 0))
        rows = result.get("list", [])
        if not rows:
            break
        collected.extend(rows)
        page_no += 1
        time.sleep(0.15)  # polite delay

    return collected


def ingest_stores(stores, source_label=""):
    """Add stores to the global unique dict. Returns count of NEW stores added.
    Auto-saves every SAVE_EVERY (1000) new stores."""
    global last_save_count
    new = 0
    for s in stores:
        sid = s.get("sellerId")
        if sid and sid not in unique_stores:
            unique_stores[sid] = s
            new += 1
    # Auto-save every 1000 new stores
    current = len(unique_stores)
    if current - last_save_count >= SAVE_EVERY:
        save_progress()
        last_save_count = current
        print(f"    [AUTO-SAVE] {current:,} stores saved to disk")
    return new


# ═══════════════════════════════════════════════════════════════════════════════
# OLC grid generation
# ═══════════════════════════════════════════════════════════════════════════════

def generate_olc_cells(south, west, north, east, code_length):
    """
    Tile a bounding box with OLC cells at the given code_length.
    Returns list of CodeArea objects covering the bbox.
    """
    # Determine step size from the OLC resolution
    # Pair resolutions: [20.0, 1.0, 0.05, 0.0025, 0.000125] for positions 0-4
    pair_index = (code_length // 2) - 1  # code_length 4 → index 1, 6 → 2, 8 → 3
    resolutions = [20.0, 1.0, 0.05, 0.0025, 0.000125]
    lat_step = resolutions[pair_index]
    lng_step = resolutions[pair_index]

    cells = []
    lat = south
    while lat < north:
        lng = west
        while lng < east:
            # Encode the center-ish point then decode to get the cell boundaries
            code = olc.encode(lat + lat_step / 2, lng + lng_step / 2, code_length)
            area = olc.decode(code)
            cells.append(area)
            lng += lng_step
        lat += lat_step

    return cells


# ═══════════════════════════════════════════════════════════════════════════════
# Recursive scraper
# ═══════════════════════════════════════════════════════════════════════════════

def scrape_cell(code_area, depth=0):
    """
    Recursively scrape an OLC cell.
    1. Probe the count at the cell center with a radius covering the cell.
    2. If total ≤ MAX_API_RESULTS → paginate & collect.
    3. If total > MAX_API_RESULTS and we can go deeper → subdivide into
       finer OLC cells and recurse.
    4. If at max depth and still > 10K → scrape what we can (best effort).
    """
    lat = code_area.latitudeCenter
    lng = code_area.longitudeCenter
    radius = min(cell_radius_km(code_area) + 1, MAX_DISTANCE_KM)  # +1 km buffer
    cl = code_area.codeLength
    indent = "  " * depth

    total, first_page = probe_count(lat, lng, radius)
    new_from_probe = ingest_stores(first_page)

    if total == 0:
        return

    code_str = olc.encode(lat, lng, cl)
    print(f"{indent}[OLC {code_str}  depth={depth}  cl={cl}  "
          f"radius={radius:.1f}km]  total={total:,}  "
          f"new_probe={new_from_probe}  global={len(unique_stores):,}")

    if total <= MAX_API_RESULTS:
        # Safe to paginate fully
        stores = scrape_all_pages(lat, lng, radius,
                                  label=f"OLC-{code_str}")
        new = ingest_stores(stores)
        print(f"{indent}  → scraped {len(stores)} rows, {new} new  "
              f"(global {len(unique_stores):,})")
    elif depth < MAX_OLC_DEPTH:
        # Subdivide into finer cells
        next_cl = OLC_LEVELS[depth + 1]
        subcells = generate_olc_cells(
            code_area.latitudeLo, code_area.longitudeLo,
            code_area.latitudeHi, code_area.longitudeHi,
            next_cl,
        )
        print(f"{indent}  → too many ({total:,}), subdividing into "
              f"{len(subcells)} cells at cl={next_cl}")
        for i, sub in enumerate(subcells):
            scrape_cell(sub, depth + 1)
            # Periodic save every 50 subcells
            if (i + 1) % 50 == 0:
                save_progress()
    else:
        # Max depth reached — scrape best effort
        print(f"{indent}  → MAX DEPTH, scraping best-effort ({total:,} reported)")
        stores = scrape_all_pages(lat, lng, radius,
                                  label=f"OLC-{code_str}-maxdepth")
        new = ingest_stores(stores)
        print(f"{indent}  → got {len(stores)} rows, {new} new  "
              f"(global {len(unique_stores):,})")


# ═══════════════════════════════════════════════════════════════════════════════
# Entry-point orchestration
# ═══════════════════════════════════════════════════════════════════════════════

def build_initial_cells(center, code_length=4):
    """
    Build OLC cells at code_length covering a 200 km radius around a center.
    Returns list of CodeArea objects.
    """
    # 200 km ≈ 1.8° latitude, ~2.2° longitude at SA latitudes
    lat, lng = center["lat"], center["lng"]
    lat_span = 2.0  # degrees (~222 km)
    lng_span = 2.5  # degrees (~220 km at -30° lat)

    south = lat - lat_span
    north = lat + lat_span
    west = lng - lng_span
    east = lng + lng_span

    return generate_olc_cells(south, west, north, east, code_length)


def scrape_center(center):
    """Scrape all stores around one geographic center using OLC grid."""
    name = center["name"]
    lat, lng = center["lat"], center["lng"]

    print(f"\n{'='*70}")
    print(f"CENTER: {name} ({lat}, {lng})")
    print(f"{'='*70}")

    # First do a quick probe to see if we even need subdivision
    total, first_page = probe_count(lat, lng, MAX_DISTANCE_KM)
    new = ingest_stores(first_page)
    print(f"  Quick probe: {total:,} stores in 200km radius, {new} new")

    if total <= MAX_API_RESULTS:
        # Simple case: just paginate
        print(f"  ≤ {MAX_API_RESULTS:,} — direct pagination")
        stores = scrape_all_pages(lat, lng, MAX_DISTANCE_KM, label=name)
        new = ingest_stores(stores)
        print(f"  → {len(stores)} rows, {new} new (global {len(unique_stores):,})")
    else:
        # Need OLC subdivision
        print(f"  > {MAX_API_RESULTS:,} — starting OLC grid subdivision")
        initial_cells = build_initial_cells(center, code_length=OLC_LEVELS[0])
        print(f"  Generated {len(initial_cells)} initial cells at cl={OLC_LEVELS[0]}")

        for i, cell in enumerate(initial_cells):
            scrape_cell(cell, depth=0)
            if (i + 1) % 10 == 0:
                save_progress()
                elapsed = time.time() - start_time
                rate = len(unique_stores) / (elapsed / 60) if elapsed > 0 else 0
                print(f"\n  [Progress] {len(unique_stores):,} unique stores | "
                      f"{api_calls:,} API calls | {elapsed/60:.1f} min | "
                      f"{rate:.0f} stores/min\n")

    save_progress()


# ═══════════════════════════════════════════════════════════════════════════════
# Persistence
# ═══════════════════════════════════════════════════════════════════════════════

def save_progress():
    """Save current state to JSON and CSV."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stores_list = list(unique_stores.values())

    # JSON
    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(stores_list, f, ensure_ascii=False, indent=2)

    # CSV
    if stores_list:
        keys = [
            "sellerId", "storeName", "primaryContactNumber",
            "alternativeContactNumber", "storeAddress",
            "storeAddressLatitude", "storeAddressLongitude",
            "premiumSeller", "subscriptionStatus",
            "serviceRange", "storeLogo",
        ]
        with open(CSV_OUT, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(stores_list)

    # Progress metadata
    meta = {
        "last_save": datetime.now().isoformat(),
        "unique_stores": len(unique_stores),
        "api_calls": api_calls,
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(meta, f, indent=2)


def load_existing():
    """Load previously scraped stores to allow resumption."""
    global unique_stores, last_save_count
    if os.path.exists(JSON_OUT):
        try:
            with open(JSON_OUT, "r", encoding="utf-8") as f:
                existing = json.load(f)
            for s in existing:
                sid = s.get("sellerId")
                if sid:
                    unique_stores[sid] = s
            last_save_count = len(unique_stores)
            print(f"Loaded {len(unique_stores):,} existing stores from {JSON_OUT}")
        except json.JSONDecodeError as e:
            print(f"⚠ Existing JSON is corrupted ({e}), starting fresh")
            unique_stores = {}


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    global start_time
    start_time = time.time()

    print("Yep.co.za Adaptive OLC Grid Scraper")
    print(f"OLC levels: {OLC_LEVELS}")
    print(f"Max API results per query: {MAX_API_RESULTS:,}")
    print(f"Centers: {len(GEO_CENTERS)}")
    print()

    # Resume from previous run if available
    load_existing()

    for center in GEO_CENTERS:
        scrape_center(center)

    # Final save
    save_progress()

    elapsed = time.time() - start_time
    print(f"\n{'='*70}")
    print(f"DONE")
    print(f"  Unique stores: {len(unique_stores):,}")
    print(f"  API calls:     {api_calls:,}")
    print(f"  Time:          {elapsed/60:.1f} minutes")
    print(f"  Output:        {JSON_OUT}, {CSV_OUT}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()

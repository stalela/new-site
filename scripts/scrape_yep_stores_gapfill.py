#!/usr/bin/env python3
"""
Yep.co.za Store Scraper — Multi-threaded Gap-Filler
=====================================================
Fills gaps from the OLC scraper by doing direct pagination for cities
under 10K stores, and multi-threaded OLC subdivision for cities over 10K.

Loads existing stores from yep_stores_full.json and only adds new ones.
Uses 10 threads for parallel page fetching.
"""

import requests
import json
import time
import os
import csv
import math
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from openlocationcode import openlocationcode as olc

# ── Config ────────────────────────────────────────────────────────────────────

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
MAX_API_RESULTS = 10_000
MAX_DISTANCE_KM = 200
WORKERS = 10

OUTPUT_DIR = "output"
JSON_OUT = os.path.join(OUTPUT_DIR, "yep_stores_full.json")
CSV_OUT = os.path.join(OUTPUT_DIR, "yep_stores_full.csv")
PROGRESS_FILE = os.path.join(OUTPUT_DIR, "yep_olc_progress.json")

# OLC levels for subdivision
OLC_LEVELS = [4, 6, 8]

# All city centers
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


# ── Thread-safe state ────────────────────────────────────────────────────────

_thread_local = threading.local()
unique_stores = {}  # sellerId → store dict
store_lock = threading.Lock()
api_calls = 0
api_lock = threading.Lock()


def get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


def inc_api():
    global api_calls
    with api_lock:
        api_calls += 1


# ── API helpers ───────────────────────────────────────────────────────────────

def probe_count(lat, lng, distance_km):
    """Quick probe: returns (total, first_page_stores)."""
    session = get_session()
    payload = {
        "page": {"pageNo": 1, "pageSize": PAGE_SIZE},
        "categoryId": "", "keyword": "",
        "distance": int(distance_km),
        "longitude": float(lng), "latitude": float(lat),
    }
    try:
        inc_api()
        resp = session.post(SEARCH_URL, json=payload, timeout=30)
        data = resp.json()
        if data.get("code") == "10000":
            result = data.get("data", {})
            return int(result.get("total", 0)), result.get("list", [])
    except Exception as e:
        print(f"  ⚠ Probe failed ({lat:.4f}, {lng:.4f}): {e}")
    return 0, []


def fetch_page(lat, lng, distance_km, page_no):
    """Fetch a single page. Returns list of store dicts."""
    session = get_session()
    payload = {
        "page": {"pageNo": page_no, "pageSize": PAGE_SIZE},
        "categoryId": "", "keyword": "",
        "distance": int(distance_km),
        "longitude": float(lng), "latitude": float(lat),
    }
    try:
        inc_api()
        resp = session.post(SEARCH_URL, json=payload, timeout=30)
        data = resp.json()
        if data.get("code") == "10000":
            return data.get("data", {}).get("list", [])
    except Exception as e:
        print(f"  ⚠ Page {page_no} failed ({lat:.4f}, {lng:.4f}): {e}")
    return []


def ingest_stores(stores):
    """Thread-safe ingestion. Returns count of NEW stores."""
    new = 0
    with store_lock:
        for s in stores:
            sid = s.get("sellerId")
            if sid and sid not in unique_stores:
                unique_stores[sid] = s
                new += 1
    return new


# ── Scraping strategies ──────────────────────────────────────────────────────

def scrape_pages_parallel(lat, lng, distance_km, total, label=""):
    """
    Paginate all pages for a center using thread pool.
    Pages are independent and can be fetched in parallel.
    """
    max_pages = min(total // PAGE_SIZE + 1, MAX_API_RESULTS // PAGE_SIZE)
    pages = list(range(1, max_pages + 1))

    print(f"  [{label}] Fetching {len(pages)} pages with {WORKERS} threads...")

    all_new = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {
            executor.submit(fetch_page, lat, lng, distance_km, p): p
            for p in pages
        }
        done = 0
        for future in as_completed(futures):
            stores = future.result()
            new = ingest_stores(stores)
            all_new += new
            done += 1
            if done % 20 == 0:
                print(f"    pages: {done}/{len(pages)} | new: {all_new} | "
                      f"global: {len(unique_stores):,}")

    print(f"  [{label}] Done: {all_new} new stores (global: {len(unique_stores):,})")
    return all_new


def haversine_km(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def cell_radius_km(code_area):
    diag = haversine_km(
        code_area.latitudeLo, code_area.longitudeLo,
        code_area.latitudeHi, code_area.longitudeHi,
    )
    return diag / 2


def generate_olc_cells(south, west, north, east, code_length):
    pair_index = (code_length // 2) - 1
    resolutions = [20.0, 1.0, 0.05, 0.0025, 0.000125]
    lat_step = resolutions[pair_index]
    lng_step = resolutions[pair_index]

    cells = []
    lat = south
    while lat < north:
        lng = west
        while lng < east:
            code = olc.encode(lat + lat_step / 2, lng + lng_step / 2, code_length)
            area = olc.decode(code)
            cells.append(area)
            lng += lng_step
        lat += lat_step
    return cells


def scrape_olc_cell_task(cell_info):
    """
    Worker task: scrape one OLC leaf cell.
    cell_info = (code_area, depth)
    Returns count of new stores found.
    """
    code_area, depth = cell_info
    lat = code_area.latitudeCenter
    lng = code_area.longitudeCenter
    radius = min(cell_radius_km(code_area) + 1, MAX_DISTANCE_KM)

    total, first_page = probe_count(lat, lng, radius)
    new = ingest_stores(first_page)

    if total == 0:
        return new

    if total <= MAX_API_RESULTS:
        # Paginate all pages for this cell
        max_pages = min(total // PAGE_SIZE + 1, MAX_API_RESULTS // PAGE_SIZE)
        for page_no in range(2, max_pages + 1):  # page 1 already fetched in probe
            stores = fetch_page(lat, lng, radius, page_no)
            new += ingest_stores(stores)
            time.sleep(0.05)
    # If still > 10K at max depth, just scrape what we can
    return new


def scrape_center_olc_parallel(center):
    """
    For cities > 10K: discover OLC leaf cells, then scrape them in parallel.
    """
    name = center["name"]
    lat, lng = center["lat"], center["lng"]

    # Build OLC grid at code_length 6 (5.5km cells) covering 200km radius
    lat_span, lng_span = 2.0, 2.5
    cells_cl6 = generate_olc_cells(
        lat - lat_span, lng - lng_span,
        lat + lat_span, lng + lng_span,
        code_length=6
    )
    print(f"  [{name}] Generated {len(cells_cl6)} OLC cl=6 cells")

    # Phase 1: Probe all cl=6 cells to find which need subdivision
    leaf_tasks = []   # (code_area, depth) pairs ready to scrape
    need_subdiv = []  # cells that need cl=8 subdivision

    print(f"  [{name}] Probing cells to classify...")
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        future_to_cell = {}
        for cell in cells_cl6:
            lat_c = cell.latitudeCenter
            lng_c = cell.longitudeCenter
            radius = min(cell_radius_km(cell) + 1, MAX_DISTANCE_KM)
            future_to_cell[executor.submit(probe_count, lat_c, lng_c, radius)] = cell

        probed = 0
        for future in as_completed(future_to_cell):
            cell = future_to_cell[future]
            total, first_page = future.result()
            ingest_stores(first_page)
            probed += 1

            if total == 0:
                pass  # skip empty
            elif total <= MAX_API_RESULTS:
                leaf_tasks.append((cell, 1))
            else:
                need_subdiv.append((cell, total))

            if probed % 100 == 0:
                print(f"    probed: {probed}/{len(cells_cl6)} | "
                      f"leaves: {len(leaf_tasks)} | subdiv: {len(need_subdiv)} | "
                      f"global: {len(unique_stores):,}")

    # Subdivide dense cells into cl=8
    for cell, total in need_subdiv:
        sub_cells = generate_olc_cells(
            cell.latitudeLo, cell.longitudeLo,
            cell.latitudeHi, cell.longitudeHi,
            code_length=8
        )
        code_str = olc.encode(cell.latitudeCenter, cell.longitudeCenter, 6)
        print(f"    Subdividing {code_str} ({total:,} stores) → {len(sub_cells)} cl=8 cells")
        for sc in sub_cells:
            leaf_tasks.append((sc, 2))

    print(f"  [{name}] Total leaf tasks: {len(leaf_tasks):,}")

    # Phase 2: Scrape all leaf cells in parallel
    completed = 0
    total_new = 0
    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(scrape_olc_cell_task, task): task
                   for task in leaf_tasks}

        for future in as_completed(futures):
            new = future.result()
            total_new += new
            completed += 1
            if completed % 100 == 0:
                print(f"    scraped: {completed}/{len(leaf_tasks)} | "
                      f"new: {total_new:,} | global: {len(unique_stores):,}")

    print(f"  [{name}] Completed: {total_new:,} new stores")
    save_progress()


# ── Persistence ───────────────────────────────────────────────────────────────

def save_progress():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    stores_list = list(unique_stores.values())

    with open(JSON_OUT, "w", encoding="utf-8") as f:
        json.dump(stores_list, f, ensure_ascii=False, indent=2)

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

    meta = {
        "last_save": datetime.now().isoformat(),
        "unique_stores": len(unique_stores),
        "api_calls": api_calls,
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(meta, f, indent=2)
    print(f"  [SAVED] {len(unique_stores):,} stores, {api_calls:,} API calls")


def load_existing():
    if os.path.exists(JSON_OUT):
        try:
            with open(JSON_OUT, "r", encoding="utf-8") as f:
                existing = json.load(f)
            for s in existing:
                sid = s.get("sellerId")
                if sid:
                    unique_stores[sid] = s
            print(f"Loaded {len(unique_stores):,} existing stores")
        except json.JSONDecodeError as e:
            print(f"⚠ JSON corrupted ({e}), starting fresh")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    start_time = time.time()

    print("Yep.co.za Multi-threaded Gap-Fill Scraper")
    print(f"Workers: {WORKERS}")
    print()

    load_existing()
    initial = len(unique_stores)

    for center in GEO_CENTERS:
        name = center["name"]
        lat, lng = center["lat"], center["lng"]

        print(f"\n{'='*60}")
        print(f"CENTER: {name} ({lat}, {lng})")
        print(f"{'='*60}")

        # Probe to see total
        total, first_page = probe_count(lat, lng, MAX_DISTANCE_KM)
        new = ingest_stores(first_page)
        print(f"  API reports: {total:,} stores in 200km radius")

        if total <= MAX_API_RESULTS:
            # Simple: parallel pagination
            scrape_pages_parallel(lat, lng, MAX_DISTANCE_KM, total, label=name)
        else:
            # Complex: OLC subdivision + parallel scraping
            scrape_center_olc_parallel(center)

        save_progress()

    elapsed = time.time() - start_time
    final = len(unique_stores)
    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"  Started with:  {initial:,}")
    print(f"  Final total:   {final:,}")
    print(f"  New stores:    {final - initial:,}")
    print(f"  API calls:     {api_calls:,}")
    print(f"  Time:          {elapsed/60:.1f} minutes")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

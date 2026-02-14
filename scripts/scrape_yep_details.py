#!/usr/bin/env python3
"""
Yep.co.za Store Detail Scraper (Multi-threaded)
=================================================
Reads seller IDs from yep_stores_full.json, fetches rich detail for each store
via POST /api/seller/detail using a thread pool, and saves in batch files of
100 stores each.

Output: output/yep_details/batch_0001.json, batch_0002.json, etc.
Each batch file is a JSON array of up to 100 detail records.

Resumable — skips seller IDs already present in existing batch files.
"""

import requests
import json
import time
import os
import glob
import threading
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ── Config ────────────────────────────────────────────────────────────────────

DETAIL_URL = "https://fm.mall.yep.co.za/api/seller/detail"
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

STORES_INPUT = "output/yep_stores_full.json"
BATCH_DIR = "output/yep_details"
BATCH_SIZE = 100
PROGRESS_FILE = os.path.join(BATCH_DIR, "_progress.json")
WORKERS = 10        # concurrent threads
DELAY = 0.05        # per-thread delay (effective: WORKERS * DELAY concurrency)


# ── Thread-safe session pool ─────────────────────────────────────────────────

_thread_local = threading.local()

def get_session():
    """One requests.Session per thread for connection reuse."""
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update(HEADERS)
    return _thread_local.session


# ── Helpers ───────────────────────────────────────────────────────────────────

def api_post(seller_id, retries=3, backoff=2):
    """Fetch detail for one seller. Returns (seller_id, data_dict) or (seller_id, None)."""
    session = get_session()
    payload = {"sellerId": str(seller_id)}
    for attempt in range(retries):
        try:
            resp = session.post(DETAIL_URL, json=payload, timeout=30)
            resp.raise_for_status()
            body = resp.json()
            if body.get("code") == "10000" and body.get("data"):
                return seller_id, body["data"]
            else:
                if attempt == retries - 1:
                    msg = body.get("message", "unknown")
                    print(f"  ⚠ sellerId={seller_id}: API error '{msg}'")
                return seller_id, None
        except Exception as e:
            wait = backoff ** (attempt + 1)
            if attempt < retries - 1:
                time.sleep(wait)
            else:
                print(f"  ⚠ sellerId={seller_id}: request failed ({e})")
    return seller_id, None


def fetch_one(seller_id):
    """Wrapper for thread pool: fetch + small delay."""
    result = api_post(seller_id)
    time.sleep(DELAY)
    return result


def batch_filename(batch_num):
    return os.path.join(BATCH_DIR, f"batch_{batch_num:04d}.json")


def save_batch(batch_num, records):
    """Write a batch of detail records to disk."""
    path = batch_filename(batch_num)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)
    print(f"  [SAVED] {path} ({len(records)} records)")


def load_existing_ids():
    """Scan existing batch files and return set of already-scraped seller IDs."""
    done = set()
    if not os.path.isdir(BATCH_DIR):
        return done
    for path in sorted(glob.glob(os.path.join(BATCH_DIR, "batch_*.json"))):
        try:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)
            for r in records:
                sid = r.get("sellerId") or r.get("id")
                if not sid and "sellerShopVO" in r:
                    sid = r["sellerShopVO"].get("sellerId")
                if sid:
                    done.add(str(sid))
        except Exception as e:
            print(f"  ⚠ Error reading {path}: {e}")
    return done


def count_batches():
    """Return the next batch number based on existing files."""
    if not os.path.isdir(BATCH_DIR):
        return 1
    existing = glob.glob(os.path.join(BATCH_DIR, "batch_*.json"))
    if not existing:
        return 1
    nums = []
    for p in existing:
        base = os.path.basename(p)
        try:
            num = int(base.replace("batch_", "").replace(".json", ""))
            nums.append(num)
        except ValueError:
            pass
    return max(nums) + 1 if nums else 1


def save_progress(total, done, failed, elapsed):
    meta = {
        "last_save": datetime.now().isoformat(),
        "total_stores": total,
        "details_scraped": done,
        "failed": failed,
        "elapsed_minutes": round(elapsed / 60, 1),
        "workers": WORKERS,
    }
    with open(PROGRESS_FILE, "w") as f:
        json.dump(meta, f, indent=2)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(BATCH_DIR, exist_ok=True)

    # Load store list
    print(f"Loading stores from {STORES_INPUT}...")
    with open(STORES_INPUT, "r", encoding="utf-8") as f:
        stores = json.load(f)
    all_ids = [str(s.get("sellerId")) for s in stores if s.get("sellerId")]
    print(f"  Total stores: {len(all_ids):,}")

    # Load already-done IDs
    done_ids = load_existing_ids()
    print(f"  Already scraped: {len(done_ids):,}")

    # Filter to remaining
    remaining = [sid for sid in all_ids if sid not in done_ids]
    print(f"  Remaining: {len(remaining):,}")
    print(f"  Workers: {WORKERS}")

    if not remaining:
        print("Nothing to do — all stores already have details.")
        return

    # Scrape with thread pool
    batch_num = count_batches()
    batch_buffer = []
    scraped = 0
    failed = 0
    processed = 0
    start_time = time.time()

    print(f"\n  Starting {WORKERS}-thread scrape of {len(remaining):,} stores...\n")

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        futures = {executor.submit(fetch_one, sid): sid for sid in remaining}

        for future in as_completed(futures):
            seller_id, detail = future.result()
            processed += 1

            if detail:
                batch_buffer.append(detail)
                scraped += 1
            else:
                failed += 1

            # Save batch when full
            if len(batch_buffer) >= BATCH_SIZE:
                save_batch(batch_num, batch_buffer)
                batch_num += 1
                batch_buffer = []
                elapsed = time.time() - start_time
                save_progress(len(all_ids), len(done_ids) + scraped, failed, elapsed)

            # Progress every 500
            if processed % 500 == 0:
                elapsed = time.time() - start_time
                rate = processed / (elapsed / 60) if elapsed > 0 else 0
                pct = processed / len(remaining) * 100
                eta_min = (len(remaining) - processed) / rate if rate > 0 else 0
                print(f"  [{processed:,}/{len(remaining):,}] {pct:.1f}% | "
                      f"scraped={scraped:,} failed={failed:,} | "
                      f"{rate:.0f} req/min | ETA {eta_min:.0f} min")

    # Save any remaining in buffer
    if batch_buffer:
        save_batch(batch_num, batch_buffer)

    elapsed = time.time() - start_time
    save_progress(len(all_ids), len(done_ids) + scraped, failed, elapsed)

    print(f"\n{'='*60}")
    print(f"DONE")
    print(f"  Details scraped: {scraped:,}")
    print(f"  Failed:          {failed:,}")
    print(f"  Total in batches:{len(done_ids) + scraped:,}")
    print(f"  Time:            {elapsed/60:.1f} minutes")
    print(f"  Workers:         {WORKERS}")
    print(f"  Output:          {BATCH_DIR}/")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

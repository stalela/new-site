#!/usr/bin/env python3
"""
Import merged company data into Supabase `companies` table.
============================================================

Reads output/merged_companies.json and upserts in batches of 500
using source + source_id as the conflict key.

Usage:
  python scripts/import_companies_to_supabase.py

Requires env vars:
  NEXT_PUBLIC_SUPABASE_URL
  SUPABASE_SERVICE_ROLE_KEY
"""

import json
import os
import sys
import time
from datetime import datetime

try:
    import httpx
except ImportError:
    print("Installing httpx...")
    os.system(f"{sys.executable} -m pip install httpx")
    import httpx

SUPABASE_URL = os.environ.get("NEXT_PUBLIC_SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("ERROR: Set NEXT_PUBLIC_SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars")
    sys.exit(1)

REST_URL = f"{SUPABASE_URL}/rest/v1/companies"
HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

MERGED_FILE = os.path.join("output", "merged_companies.json")
BATCH_SIZE = 500
PROGRESS_FILE = os.path.join("output", "import_supabase_progress.json")


def clean_record(raw: dict) -> dict:
    """Convert a merged JSON record to the Supabase row format."""
    # Map _source to source
    record = {
        "source": raw.get("_source", ""),
        "source_id": str(raw.get("_source_id", "")),
        "name": (raw.get("name") or "")[:500],  # Truncate very long names
        "description": raw.get("description") or None,
        "category": raw.get("category") or None,
        "categories": raw.get("categories") or [],
        "type": raw.get("type") or None,
        "phone": raw.get("phone") or None,
        "alt_phone": raw.get("alt_phone") or None,
        "mobile": raw.get("mobile") or None,
        "whatsapp": raw.get("whatsapp") or None,
        "email": raw.get("email") or None,
        "contact_email": raw.get("contact_email") or None,
        "contact_name": raw.get("contact_name") or None,
        "address": raw.get("address") or None,
        "address_line1": raw.get("address_line1") or None,
        "suburb": raw.get("suburb") or None,
        "city": raw.get("city") or None,
        "province": raw.get("province") or None,
        "postal_code": raw.get("postal_code") or None,
        "country": raw.get("country") or "South Africa",
        "latitude": float(raw["latitude"]) if raw.get("latitude") else None,
        "longitude": float(raw["longitude"]) if raw.get("longitude") else None,
        "website": raw.get("website") or None,
        "logo": raw.get("logo") or None,
        "source_url": raw.get("source_url") or None,
        "registration_number": raw.get("registration_number") or None,
        "vat_number": raw.get("vat_number") or None,
        "seller_id": raw.get("seller_id") or None,
        "is_open": raw.get("is_open") if isinstance(raw.get("is_open"), bool) else None,
        "service_range_km": int(raw["service_range_km"]) if raw.get("service_range_km") else None,
        "premium_seller": raw.get("premium_seller") if isinstance(raw.get("premium_seller"), bool) else None,
        "subscription_status": int(raw["subscription_status"]) if raw.get("subscription_status") else None,
        "operation_hours": raw.get("operation_hours") or None,
        "short_description": raw.get("short_description") or None,
    }

    # Ensure source_id isn't empty
    if not record["source_id"]:
        record["source_id"] = record["name"][:200]

    return record


def load_progress():
    """Load import progress to resume after interruption."""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, "r") as f:
            return json.load(f)
    return {"imported": 0, "errors": 0, "last_batch": 0}


def save_progress(progress):
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def main():
    print(f"Loading {MERGED_FILE}...")
    with open(MERGED_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)
    print(f"Loaded {len(records):,} records")

    progress = load_progress()
    start_batch = progress["last_batch"]
    if start_batch > 0:
        print(f"Resuming from batch {start_batch} ({start_batch * BATCH_SIZE:,} records already imported)")

    client = httpx.Client(timeout=60.0)

    total = len(records)
    imported = progress["imported"]
    errors = progress["errors"]
    t0 = time.time()

    for batch_idx in range(start_batch, (total + BATCH_SIZE - 1) // BATCH_SIZE):
        batch_start = batch_idx * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = records[batch_start:batch_end]

        cleaned = []
        for raw in batch:
            try:
                cleaned.append(clean_record(raw))
            except Exception as e:
                errors += 1
                if errors <= 10:
                    print(f"  ⚠ Clean error at index {batch_start}: {e}")

        if not cleaned:
            continue

        try:
            resp = client.post(REST_URL, headers=HEADERS, json=cleaned)
            resp.raise_for_status()
            imported += len(cleaned)
        except Exception as e:
            errors += len(cleaned)
            err_msg = str(e)[:200]
            # Try to get response body for better error info
            if hasattr(e, 'response') and e.response is not None:
                err_msg = e.response.text[:200]
            print(f"  ✗ Batch {batch_idx} error: {err_msg}")
            # Try inserting one-by-one to salvage what we can
            for row in cleaned:
                try:
                    resp = client.post(REST_URL, headers=HEADERS, json=row)
                    resp.raise_for_status()
                    imported += 1
                    errors -= 1
                except Exception:
                    pass

        # Progress report every 1000 records
        if imported % 1000 < BATCH_SIZE or batch_end == total:
            elapsed = time.time() - t0
            rate = imported / elapsed if elapsed > 0 else 0
            eta = (total - imported) / rate if rate > 0 else 0
            print(
                f"  [{imported:,}/{total:,}] "
                f"{imported/total*100:.1f}% | "
                f"{rate:.0f} rec/s | "
                f"ETA {eta/60:.1f}m | "
                f"errors: {errors}"
            )

            progress = {
                "imported": imported,
                "errors": errors,
                "last_batch": batch_idx + 1,
                "timestamp": datetime.now().isoformat(),
            }
            save_progress(progress)

    elapsed = time.time() - t0
    print(f"\n{'='*60}")
    print(f"IMPORT COMPLETE")
    print(f"  Imported:  {imported:,}")
    print(f"  Errors:    {errors:,}")
    print(f"  Time:      {elapsed/60:.1f} minutes")
    print(f"  Rate:      {imported/elapsed:.0f} records/sec")
    print(f"{'='*60}")

    # Clean up progress file on success
    if errors == 0 and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)


if __name__ == "__main__":
    main()

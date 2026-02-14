#!/usr/bin/env python3
"""
Merge All Company Data Sources
================================
Merges three data sources into a single unified dataset:
  1. output/sa_companies.json           — 2,484 records (bestdirectory)
  2. output/yep_details/batch_*.json    — ~161K records (yep.co.za)
  3. output/bizcommunity_batches/batch_*.json — ~10.9K records (bizcommunity)

Union of all columns — every field from every source is preserved.
Each record gets a `_source` tag and a `_source_id` for traceability.

Output:
  output/merged_companies.json   — full merged dataset
  output/merged_companies.csv    — flat CSV (nested fields JSON-encoded)
  output/merge_stats.json        — merge statistics
"""

import json
import os
import csv
import glob
import re
from datetime import datetime

OUTPUT_DIR = "output"
MERGED_JSON = os.path.join(OUTPUT_DIR, "merged_companies.json")
MERGED_CSV = os.path.join(OUTPUT_DIR, "merged_companies.csv")
STATS_FILE = os.path.join(OUTPUT_DIR, "merge_stats.json")


# ═══════════════════════════════════════════════════════════════════════════════
# Unified schema — all columns across all sources
# ═══════════════════════════════════════════════════════════════════════════════

UNIFIED_COLUMNS = [
    # Identity
    "_source",              # "yep", "bizcommunity", "bestdirectory"
    "_source_id",           # unique ID from source (sellerId, source_url, etc.)

    # Core business info
    "name",
    "description",
    "category",
    "categories",           # list of category names
    "type",                 # business type (Standard, Premium, etc.)

    # Contact
    "phone",
    "alt_phone",
    "mobile",
    "whatsapp",
    "email",
    "contact_email",
    "contact_name",
    "contact_role",
    "employment_position",

    # Address
    "address",
    "address_line1",
    "suburb",
    "city",
    "province",
    "postal_code",
    "country",
    "latitude",
    "longitude",

    # Online presence
    "website",
    "logo",
    "source_url",

    # Business registration
    "registration_number",
    "vat_number",

    # Yep-specific
    "seller_id",
    "is_open",
    "service_range_km",
    "premium_seller",
    "subscription_status",
    "operation_hours",      # JSON array of operating hours

    # Bizcommunity-specific
    "short_description",

    # Metadata
    "created_at",           # from source if available
    "modified_at",          # from source if available
]


# ═══════════════════════════════════════════════════════════════════════════════
# Source parsers — normalize each source into the unified schema
# ═══════════════════════════════════════════════════════════════════════════════

def normalize_phone(phone):
    """Clean up phone number."""
    if not phone:
        return ""
    phone = str(phone).strip()
    # Remove common prefixes and formatting
    phone = re.sub(r'[^\d+]', '', phone)
    return phone


def parse_yep_record(raw: dict) -> dict:
    """Normalize a Yep detail record."""
    shop = raw.get("sellerShopVO", {})
    features = shop.get("features", {})
    license_vo = raw.get("businessLicenseVO", {})
    categories = raw.get("businessCategoryVOList", [])

    # Parse address from features or fallback to full address
    address = shop.get("storeAddress", "") or raw.get("sellerAddress", "")

    cat_names = [c.get("categoryName", "") for c in categories if c.get("categoryName")]

    # Timestamps
    created = raw.get("gmtCreate")
    modified = raw.get("gmtModified")

    return {
        "_source": "yep",
        "_source_id": str(raw.get("id") or shop.get("sellerId", "")),
        "name": shop.get("storeName") or raw.get("sellerName", ""),
        "description": shop.get("storeDescription", ""),
        "category": cat_names[0] if cat_names else "",
        "categories": cat_names,
        "type": "Premium" if raw.get("subscriptionStatus") == 2 else "Standard",
        "phone": normalize_phone(shop.get("primaryContactNumber")),
        "alt_phone": normalize_phone(shop.get("alternativeContactNumber")),
        "mobile": normalize_phone(raw.get("mobileNumber") or raw.get("contactMobileNumber", "")),
        "whatsapp": "",
        "email": raw.get("email", "") or "",
        "contact_email": raw.get("contactEmail", "") or "",
        "contact_name": raw.get("contactName", "") or "",
        "contact_role": "",
        "employment_position": raw.get("employmentPosition", "") or "",
        "address": address,
        "address_line1": features.get("addressLine1", ""),
        "suburb": features.get("suburb", ""),
        "city": features.get("city", ""),
        "province": features.get("province", ""),
        "postal_code": features.get("postalCode", ""),
        "country": features.get("country", "South Africa"),
        "latitude": shop.get("storeAddressLatitude", ""),
        "longitude": shop.get("storeAddressLongitude", ""),
        "website": raw.get("websiteAddress", "") or "",
        "logo": shop.get("storeLogo", "") or "",
        "source_url": f"https://mall.yep.co.za/store/{shop.get('sellerId', '')}",
        "registration_number": license_vo.get("registrationNumber", "") if license_vo else "",
        "vat_number": license_vo.get("vatNumber", "") if license_vo else "",
        "seller_id": str(shop.get("sellerId", "")),
        "is_open": shop.get("isOpen", False),
        "service_range_km": shop.get("serviceRange", 0),
        "premium_seller": raw.get("subscriptionStatus") == 2,
        "subscription_status": raw.get("subscriptionStatus", 0),
        "operation_hours": shop.get("operationHours", []),
        "short_description": "",
        "created_at": created,
        "modified_at": modified,
    }


def parse_bizcommunity_record(raw: dict) -> dict:
    """Normalize a Bizcommunity record."""
    return {
        "_source": "bizcommunity",
        "_source_id": raw.get("source_url", ""),
        "name": raw.get("name", ""),
        "description": "",
        "category": raw.get("category", ""),
        "categories": [raw["category"]] if raw.get("category") else [],
        "type": raw.get("type", ""),
        "phone": normalize_phone(raw.get("phone")),
        "alt_phone": "",
        "mobile": "",
        "whatsapp": "",
        "email": raw.get("email", "") or "",
        "contact_email": "",
        "contact_name": "",
        "contact_role": "",
        "employment_position": "",
        "address": raw.get("physical_address", "") or "",
        "address_line1": "",
        "suburb": "",
        "city": "",
        "province": "",
        "postal_code": "",
        "country": "South Africa",
        "latitude": "",
        "longitude": "",
        "website": raw.get("website", "") or "",
        "logo": "",
        "source_url": raw.get("source_url", ""),
        "registration_number": "",
        "vat_number": "",
        "seller_id": "",
        "is_open": None,
        "service_range_km": None,
        "premium_seller": None,
        "subscription_status": None,
        "operation_hours": [],
        "short_description": raw.get("short_description", "") or "",
        "created_at": None,
        "modified_at": None,
    }


def parse_bestdirectory_record(raw: dict) -> dict:
    """Normalize a bestdirectory (sa_companies) record."""
    return {
        "_source": "bestdirectory",
        "_source_id": raw.get("name", ""),
        "name": raw.get("name", ""),
        "description": "",
        "category": "",
        "categories": [],
        "type": "",
        "phone": normalize_phone(raw.get("phone")),
        "alt_phone": "",
        "mobile": "",
        "whatsapp": normalize_phone(raw.get("whatsapp")),
        "email": raw.get("email", "") or "",
        "contact_email": "",
        "contact_name": "",
        "contact_role": "",
        "employment_position": "",
        "address": raw.get("address", "") or "",
        "address_line1": "",
        "suburb": "",
        "city": "",
        "province": "",
        "postal_code": "",
        "country": "South Africa",
        "latitude": "",
        "longitude": "",
        "website": raw.get("website", "") or "",
        "logo": "",
        "source_url": "",
        "registration_number": "",
        "vat_number": "",
        "seller_id": "",
        "is_open": None,
        "service_range_km": None,
        "premium_seller": None,
        "subscription_status": None,
        "operation_hours": [],
        "short_description": "",
        "created_at": None,
        "modified_at": None,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# Loaders
# ═══════════════════════════════════════════════════════════════════════════════

def load_yep_details() -> list:
    """Load all yep detail batch files."""
    batches = sorted(glob.glob(os.path.join(OUTPUT_DIR, "yep_details", "batch_*.json")))
    print(f"  Yep: {len(batches)} batch files")

    records = []
    seen_ids = set()
    for bf in batches:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                batch = json.load(f)
            for raw in batch:
                parsed = parse_yep_record(raw)
                sid = parsed["_source_id"]
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    records.append(parsed)
        except Exception as e:
            print(f"    ⚠ Error loading {bf}: {e}")

    print(f"  Yep: {len(records):,} unique records loaded")
    return records


def load_bizcommunity() -> list:
    """Load all bizcommunity batch files."""
    batches = sorted(glob.glob(os.path.join(OUTPUT_DIR, "bizcommunity_batches", "batch_*.json")))
    print(f"  Bizcommunity: {len(batches)} batch files")

    records = []
    seen_ids = set()
    for bf in batches:
        try:
            with open(bf, "r", encoding="utf-8") as f:
                batch = json.load(f)
            for raw in batch:
                parsed = parse_bizcommunity_record(raw)
                sid = parsed["_source_id"]
                if sid and sid not in seen_ids:
                    seen_ids.add(sid)
                    records.append(parsed)
        except Exception as e:
            print(f"    ⚠ Error loading {bf}: {e}")

    print(f"  Bizcommunity: {len(records):,} unique records loaded")
    return records


def load_bestdirectory() -> list:
    """Load sa_companies.json."""
    path = os.path.join(OUTPUT_DIR, "sa_companies.json")
    if not os.path.exists(path):
        print("  Bestdirectory: file not found")
        return []

    with open(path, "r", encoding="utf-8") as f:
        raw_list = json.load(f)

    records = []
    for raw in raw_list:
        parsed = parse_bestdirectory_record(raw)
        records.append(parsed)

    print(f"  Bestdirectory: {len(records):,} records loaded")
    return records


# ═══════════════════════════════════════════════════════════════════════════════
# CSV export helper
# ═══════════════════════════════════════════════════════════════════════════════

def flatten_for_csv(record: dict) -> dict:
    """Flatten complex fields to strings for CSV export."""
    flat = {}
    for k, v in record.items():
        if isinstance(v, list):
            if k == "operation_hours":
                # Compact summary: "Mon 09:00-17:00, Tue 09:00-17:00, ..."
                parts = []
                for oh in v:
                    day = oh.get("operatingDayType", "")[:3]
                    status = oh.get("operatingStatus", "")
                    if status == "open":
                        parts.append(f"{day} {oh.get('openTime','')}-{oh.get('closeTime','')}")
                    else:
                        parts.append(f"{day} closed")
                flat[k] = "; ".join(parts)
            elif k == "categories":
                flat[k] = "; ".join(v)
            else:
                flat[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, dict):
            flat[k] = json.dumps(v, ensure_ascii=False)
        elif isinstance(v, bool):
            flat[k] = str(v)
        elif v is None:
            flat[k] = ""
        else:
            flat[k] = v
    return flat


# ═══════════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    print("Merging all company data sources...")
    print()

    # Load all sources
    print("Loading sources:")
    yep = load_yep_details()
    biz = load_bizcommunity()
    best = load_bestdirectory()

    # Merge — simple union (no dedup across sources, they're different platforms)
    merged = yep + biz + best
    print(f"\nTotal merged records: {len(merged):,}")
    print(f"  Yep:           {len(yep):,}")
    print(f"  Bizcommunity:  {len(biz):,}")
    print(f"  Bestdirectory: {len(best):,}")

    # Save JSON
    print(f"\nSaving JSON to {MERGED_JSON}...")
    with open(MERGED_JSON, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    json_size = os.path.getsize(MERGED_JSON) / (1024 * 1024)
    print(f"  JSON: {json_size:.1f} MB")

    # Save CSV
    print(f"Saving CSV to {MERGED_CSV}...")
    flat_records = [flatten_for_csv(r) for r in merged]
    with open(MERGED_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=UNIFIED_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flat_records)
    csv_size = os.path.getsize(MERGED_CSV) / (1024 * 1024)
    print(f"  CSV: {csv_size:.1f} MB")

    # Stats
    stats = {
        "merged_at": datetime.now().isoformat(),
        "total_records": len(merged),
        "sources": {
            "yep": {"records": len(yep), "batch_files": len(glob.glob("output/yep_details/batch_*.json"))},
            "bizcommunity": {"records": len(biz), "batch_files": len(glob.glob("output/bizcommunity_batches/batch_*.json"))},
            "bestdirectory": {"records": len(best), "file": "sa_companies.json"},
        },
        "columns": UNIFIED_COLUMNS,
        "output_files": {
            "json": MERGED_JSON,
            "csv": MERGED_CSV,
            "json_size_mb": round(json_size, 1),
            "csv_size_mb": round(csv_size, 1),
        },
        "coverage": {
            "with_phone": sum(1 for r in merged if r.get("phone")),
            "with_email": sum(1 for r in merged if r.get("email")),
            "with_website": sum(1 for r in merged if r.get("website")),
            "with_address": sum(1 for r in merged if r.get("address")),
            "with_gps": sum(1 for r in merged if r.get("latitude") and r.get("longitude")),
            "with_description": sum(1 for r in merged if r.get("description")),
            "with_category": sum(1 for r in merged if r.get("category")),
        },
    }
    with open(STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2)

    print(f"\n{'='*60}")
    print(f"MERGE COMPLETE")
    print(f"  Total:    {len(merged):,} records")
    print(f"  JSON:     {MERGED_JSON} ({json_size:.1f} MB)")
    print(f"  CSV:      {MERGED_CSV} ({csv_size:.1f} MB)")
    print(f"  Stats:    {STATS_FILE}")
    print(f"\n  Coverage:")
    for field, count in stats["coverage"].items():
        pct = count / len(merged) * 100 if merged else 0
        print(f"    {field}: {count:,} ({pct:.1f}%)")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()

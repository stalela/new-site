"""
Yellow Pages South Africa — Company Scraper
============================================
Scrapes company details (name, phone, WhatsApp, address, email, website)
from https://www.yellowpages-south-africa.com and saves to both JSON and Excel.

Usage:
    python scripts/scrape_companies.py
    python scripts/scrape_companies.py --pages 5        # scrape first 5 pages only
    python scripts/scrape_companies.py --output mydata   # custom output name (mydata.json + mydata.xlsx)

Handles Cloudflare email obfuscation automatically.
"""

import argparse
import json
import re
import sys
import time
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup


# ── Cloudflare email decoder ────────────────────────────────────────────────
def decode_cf_email(cf_email: str) -> str | None:
    """Reverse the XOR encoding Cloudflare uses to hide email addresses."""
    if not cf_email:
        return None
    try:
        r = int(cf_email[:2], 16)
        return "".join(
            chr(int(cf_email[i : i + 2], 16) ^ r)
            for i in range(2, len(cf_email), 2)
        )
    except (ValueError, IndexError):
        return None


# ── Extraction helpers ───────────────────────────────────────────────────────
LABEL_MAP = {
    "phone": "phone",
    "tel": "phone",
    "telephone": "phone",
    "whatsapp": "whatsapp",
    "address": "address",
    "physical address": "address",
    "email": "email",
    "e-mail": "email",
    "website": "website",
    "web": "website",
    "url": "website",
}


def normalise_label(raw: str) -> str:
    """Map raw label text to a consistent field name."""
    cleaned = raw.strip().rstrip(":").lower()
    return LABEL_MAP.get(cleaned, cleaned)


def extract_company(card) -> dict | None:
    """Pull structured data from a single company card element."""
    body = card.find("div", class_="card-body")
    if not body:
        return None

    data: dict[str, str | None] = {
        "name": None,
        "phone": None,
        "whatsapp": None,
        "address": None,
        "email": None,
        "website": None,
    }

    # Name — typically in card-header > h2
    header = card.find("div", class_="card-header")
    if header:
        h2 = header.find("h2")
        if h2:
            data["name"] = h2.get_text(strip=True)

    # Rows inside card-body
    rows = body.find_all("div", class_="row")
    for row in rows:
        cols = row.find_all("div", class_="col") or row.find_all("div")
        if len(cols) < 2:
            continue

        label = normalise_label(cols[0].get_text(strip=True))
        value_div = cols[1]

        # Handle Cloudflare-protected emails
        cf_span = value_div.find("span", class_="__cf_email__")
        if cf_span:
            data["email"] = decode_cf_email(cf_span.get("data-cfemail"))
            continue

        # Handle regular mailto links as fallback
        mailto = value_div.find("a", href=re.compile(r"^mailto:", re.I))
        if mailto and label in ("email", "e-mail"):
            data["email"] = mailto.get_text(strip=True)
            continue

        # Handle website links
        if label == "website":
            link = value_div.find("a", href=True)
            data["website"] = link["href"] if link else value_div.get_text(strip=True)
            continue

        # Everything else
        text = value_div.get_text(strip=True)
        if text and label in data:
            data[label] = text

    # Only return if we got at least a name
    if not data["name"]:
        return None
    return data


# ── Main scraper ─────────────────────────────────────────────────────────────
def scrape(base_url: str, max_pages: int) -> list[dict]:
    """Scrape company listings page by page."""
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/125.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
    )

    all_companies: list[dict] = []

    for page in range(1, max_pages + 1):
        url = f"{base_url}?page={page}" if page > 1 else base_url
        print(f"[{page}/{max_pages}] {url}")

        try:
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as exc:
            print(f"  ⚠  Request failed: {exc}")
            continue

        soup = BeautifulSoup(resp.content, "html.parser")
        cards = soup.find_all("div", class_="card")

        page_count = 0
        for card in cards:
            company = extract_company(card)
            if company:
                all_companies.append(company)
                page_count += 1

        print(f"  → {page_count} companies extracted")

        if page_count == 0:
            print("  ⚠  No companies found — stopping (may have hit last page).")
            break

        # Respectful delay between requests
        time.sleep(1.5)

    return all_companies


def detect_max_pages(base_url: str) -> int:
    """Try to detect the total page count from pagination links."""
    try:
        resp = requests.get(
            base_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36"
                )
            },
            timeout=15,
        )
        soup = BeautifulSoup(resp.content, "html.parser")

        # Look for pagination links with ?page=N
        page_nums: list[int] = []
        for a in soup.find_all("a", href=re.compile(r"[?&]page=\d+")):
            match = re.search(r"[?&]page=(\d+)", a["href"])
            if match:
                page_nums.append(int(match.group(1)))

        return max(page_nums) if page_nums else 51
    except Exception:
        return 51  # fallback


# ── Entry point ──────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Scrape Yellow Pages SA companies")
    parser.add_argument(
        "--url",
        default="https://www.yellowpages-south-africa.com",
        help="Base URL to scrape (default: yellowpages-south-africa.com)",
    )
    parser.add_argument(
        "--pages",
        type=int,
        default=0,
        help="Max pages to scrape (0 = auto-detect, default: 0)",
    )
    parser.add_argument(
        "--output",
        default="sa_companies",
        help="Output filename without extension (default: sa_companies)",
    )
    args = parser.parse_args()

    base_url = args.url.rstrip("/")
    max_pages = args.pages or detect_max_pages(base_url)
    out_stem = args.output

    print(f"Target: {base_url}")
    print(f"Pages:  {max_pages}")
    print()

    companies = scrape(base_url, max_pages)

    if not companies:
        print("\nNo companies extracted. Check the URL or HTML structure.")
        sys.exit(1)

    out_dir = Path(__file__).resolve().parent.parent / "output"
    out_dir.mkdir(exist_ok=True)

    # JSON
    json_path = out_dir / f"{out_stem}.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(companies, f, indent=2, ensure_ascii=False)

    # Excel
    xlsx_path = out_dir / f"{out_stem}.xlsx"
    df = pd.DataFrame(companies)
    # Reorder columns for readability
    col_order = ["name", "phone", "whatsapp", "email", "website", "address"]
    cols = [c for c in col_order if c in df.columns] + [
        c for c in df.columns if c not in col_order
    ]
    df = df[cols]
    df.to_excel(xlsx_path, index=False, engine="openpyxl")

    print(f"\n✓ {len(companies)} companies saved:")
    print(f"  JSON:  {json_path}")
    print(f"  Excel: {xlsx_path}")


if __name__ == "__main__":
    main()

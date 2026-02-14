"""
BestDirectory.co.za Scraper
============================
Step 1: Crawl listing pages 0–701 to collect company names + profile URLs.
Step 2: Visit each profile page to extract full contact details (multithreaded).

Usage:
    python scrape_bestdirectory.py step1          # listing pages only
    python scrape_bestdirectory.py step2          # profile details (requires step1 output)
    python scrape_bestdirectory.py export         # create JSON + Excel from contacts CSV
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import sys
import time
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Semaphore

# --- CONFIGURATION ---
BASE_URL = "https://www.bestdirectory.co.za"
LISTINGS_URL = f"{BASE_URL}/free-business-listings-free-business-directory.html"
MAX_PAGE = 701                      # 0-indexed, last page is 701
LISTINGS_CSV = "output/bestdirectory_listings.csv"
CONTACTS_CSV = "output/bestdirectory_contacts.csv"
PROGRESS_FILE = "output/bd_contacts_progress.json"
STEP1_WORKERS = 1                   # sequential — server can't handle concurrency
STEP2_WORKERS = 3                   # gentle for profile pages
TIMEOUT = 20
SAVE_EVERY = 50


HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
}

lock = Lock()
results = []
completed_urls = set()
errors_count = 0


def make_session(pool_size=5) -> requests.Session:
    """Create a requests session with automatic retries."""
    s = requests.Session()
    retries = Retry(total=1, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    s.mount('https://', HTTPAdapter(max_retries=retries, pool_maxsize=pool_size + 2))
    s.headers.update(HEADERS)
    return s


# ============================================================
#  STEP 1 — Listing Pages
# ============================================================

def scrape_listing_page(page_num: int, session: requests.Session = None) -> list[dict]:
    """Scrape a single listing page and return list of companies."""
    url = f"{LISTINGS_URL}?page={page_num}"
    getter = session or requests
    resp = getter.get(url, headers=HEADERS, timeout=TIMEOUT)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, 'html.parser')
    companies = []

    # Company names appear in <h3> tags containing <a> links
    for h3 in soup.find_all('h3'):
        link = h3.find('a')
        if not link or not link.get('href'):
            continue

        href = link['href']
        # Skip navigation / non-company links
        if href.startswith('http') and 'bestdirectory.co.za' not in href:
            continue
        if href in ('#', '/'):
            continue
        # Must be a company profile, not a category/location page
        if '/business-directory' in href and href.count('/') > 2:
            continue  # category links like /business-directory/healthcare-health-beauty
        if '/alphabetical-' in href:
            continue

        name = link.get_text(strip=True)
        if not name:
            continue

        # Make absolute URL
        if href.startswith('/'):
            href = BASE_URL + href

        # Extract address snippet from the text following the h3
        address = ""
        # The address is usually in the parent container text after the h3
        parent = h3.parent
        if parent:
            full_text = parent.get_text(" ", strip=True)
            # Remove the company name from the beginning
            idx = full_text.find(name)
            if idx >= 0:
                after = full_text[idx + len(name):].strip()
                # Take address part (everything before the long description)
                # Address is typically in ALL CAPS at the start
                lines = after.split('  ')
                if lines:
                    address = lines[0].strip()

        companies.append({
            "Name": name,
            "Address": address,
            "Profile_URL": href,
        })

    return companies


def run_step1():
    """Crawl all listing pages (0 to MAX_PAGE) sequentially and save to CSV."""
    all_companies = []
    seen_urls = set()
    failed_pages = []
    session = make_session(1)
    progress_file = "output/bd_step1_progress.json"
    start_page = 0

    # Resume from existing CSV + progress file
    if os.path.exists(LISTINGS_CSV):
        existing = pd.read_csv(LISTINGS_CSV)
        all_companies = existing.to_dict('records')
        seen_urls = set(existing['Profile_URL'].tolist())
        print(f"  Resuming — {len(all_companies)} companies already collected", flush=True)
    if os.path.exists(progress_file):
        with open(progress_file, 'r') as f:
            prog = json.load(f)
            start_page = prog.get('last_page', 0) + 1
            print(f"  Resuming from page {start_page}", flush=True)

    print(f"Crawling pages {start_page}-{MAX_PAGE} sequentially...", flush=True)
    start = time.time()

    for page_num in range(start_page, MAX_PAGE + 1):
        try:
            companies = scrape_listing_page(page_num, session)
            new = 0
            for c in companies:
                if c["Profile_URL"] not in seen_urls:
                    seen_urls.add(c["Profile_URL"])
                    all_companies.append(c)
                    new += 1

            if (page_num + 1) % 10 == 0:
                elapsed = time.time() - start
                rate = (page_num - start_page + 1) / elapsed * 60 if elapsed > 0 else 0
                print(f"  [p{page_num}] {len(all_companies)} total (+{new} new) "
                      f"| {rate:.0f} pages/min", flush=True)

            # Save checkpoint every 25 pages
            if (page_num + 1) % 25 == 0:
                df = pd.DataFrame(all_companies)
                df.to_csv(LISTINGS_CSV, index=False)
                with open(progress_file, 'w') as f:
                    json.dump({'last_page': page_num}, f)

            time.sleep(random.uniform(0.5, 1.0))

        except Exception as e:
            print(f"  [p{page_num}] ERROR: {e}", flush=True)
            failed_pages.append(page_num)
            time.sleep(2)  # longer delay after error

    # Retry failed pages
    if failed_pages:
        print(f"\nRetrying {len(failed_pages)} failed pages...", flush=True)
        for page in sorted(failed_pages):
            try:
                time.sleep(3)
                companies = scrape_listing_page(page, session)
                for c in companies:
                    if c["Profile_URL"] not in seen_urls:
                        seen_urls.add(c["Profile_URL"])
                        all_companies.append(c)
                print(f"  [p{page}] RETRY OK: +{len(companies)}", flush=True)
            except Exception as e:
                print(f"  [p{page}] RETRY FAILED: {e}", flush=True)

    # Final save
    df = pd.DataFrame(all_companies)
    df.to_csv(LISTINGS_CSV, index=False)
    if os.path.exists(progress_file):
        os.remove(progress_file)
    elapsed = time.time() - start
    print(f"\nStep 1 complete - {len(all_companies)} unique companies -> {LISTINGS_CSV}",
          flush=True)
    print(f"  Time: {elapsed/60:.1f} min, Failed: {len(failed_pages)}", flush=True)


# ============================================================
#  STEP 2 — Profile Detail Scraping
# ============================================================

def clean_text(text: str) -> str:
    """Clean whitespace from extracted text."""
    text = re.sub(r'[\u00a0\u25a1]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def parse_profile(url: str, name: str, session: requests.Session = None) -> dict:
    """Fetch and parse a single company profile page."""
    global errors_count
    record = {
        "Name": name,
        "Profile_URL": url,
        "Phone": "",
        "Secondary_Phone": "",
        "Website": "",
        "Facebook": "",
        "Twitter": "",
        "GPS_Lat": "",
        "GPS_Lon": "",
        "Street_Address": "",
        "Suburb": "",
        "City": "",
        "Province": "",
        "Postal_Code": "",
        "Trading_Hours": "",
        "Categories": "",
        "Introduction": "",
        "About": "",
        "Keywords": "",
        "Logo_URL": "",
        "Status": "OK",
    }

    try:
        getter = session or requests
        resp = getter.get(url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 403:
            record["Status"] = "403_Forbidden"
            return record
        resp.raise_for_status()
    except requests.RequestException as e:
        record["Status"] = f"Error: {str(e)[:80]}"
        with lock:
            errors_count += 1
        return record

    soup = BeautifulSoup(resp.content, 'html.parser')

    # --- Contact Details ---
    contact_section = None
    for h3 in soup.find_all(['h3', 'h2']):
        if 'Contact Details' in h3.get_text():
            contact_section = h3.parent
            break

    if contact_section:
        text = contact_section.get_text(" ", strip=True)

        # Phone
        phone_match = re.search(r'Phone\s*Number[:\s]*([0-9 +()-]+)', text)
        if phone_match:
            record["Phone"] = clean_text(phone_match.group(1))

        # Secondary phone
        sec_match = re.search(r'Secondary\s*Number[:\s]*([0-9 +()-]+)', text)
        if sec_match:
            record["Secondary_Phone"] = clean_text(sec_match.group(1))

        # GPS
        gps_match = re.search(r'GPS\s*Coordinates[:\s]*([-\d.]+)\s*,\s*([-\d.]+)', text)
        if gps_match:
            record["GPS_Lat"] = gps_match.group(1)
            record["GPS_Lon"] = gps_match.group(2)

        # Website — look for <a> with "Visit Website"
        for a in contact_section.find_all('a'):
            link_text = a.get_text(strip=True).lower()
            href = a.get('href', '')
            if 'visit website' in link_text and href.startswith('http'):
                record["Website"] = href
            elif 'facebook' in link_text and href.startswith('http'):
                record["Facebook"] = href
            elif 'twitter' in link_text and href.startswith('http'):
                record["Twitter"] = href

    # --- Physical Address ---
    for h3 in soup.find_all(['h3', 'h2']):
        if 'Physical Address' in h3.get_text():
            addr_section = h3.parent
            if addr_section:
                # Province, City, Suburb are often links
                links = addr_section.find_all('a')
                texts = [a.get_text(strip=True) for a in links if a.get_text(strip=True)]
                # Last link is usually Province, second-to-last City, etc.
                if len(texts) >= 1:
                    record["Province"] = texts[-1]
                if len(texts) >= 2:
                    record["City"] = texts[-2]
                if len(texts) >= 3:
                    record["Suburb"] = texts[-3]

                # Full text for street address and postal code
                full_addr = addr_section.get_text(" ", strip=True)
                full_addr = full_addr.replace("Physical Address", "").strip()
                # Extract postal code (4-digit number at end)
                postal_match = re.search(r'\b(\d{4})\s*$', full_addr)
                if postal_match:
                    record["Postal_Code"] = postal_match.group(1)
                    full_addr = full_addr[:postal_match.start()].strip()

                # Street = everything that's not province/city/suburb
                street = full_addr
                for t in texts:
                    street = street.replace(t, "")
                record["Street_Address"] = clean_text(street).strip(",").strip()
            break

    # --- Trading Hours ---
    for h3 in soup.find_all(['h3', 'h2']):
        if 'Trading Hours' in h3.get_text() or 'Office Hours' in h3.get_text():
            hours_section = h3.parent
            if hours_section:
                hours_text = hours_section.get_text(" ", strip=True)
                hours_text = re.sub(r'^.*?(?:Trading Hours|Office Hours)\s*', '', hours_text)
                record["Trading_Hours"] = clean_text(hours_text)
            break

    # --- Categories ---
    for h3 in soup.find_all(['h3', 'h2']):
        if h3.get_text(strip=True) == 'Categories':
            cat_section = h3.parent
            if cat_section:
                cats = [a.get_text(strip=True) for a in cat_section.find_all('a')]
                record["Categories"] = " > ".join(cats) if cats else ""
            break

    # --- Introduction ---
    for h3 in soup.find_all(['h3', 'h2']):
        if 'Introduction' in h3.get_text():
            intro_section = h3.parent
            if intro_section:
                intro_text = intro_section.get_text(" ", strip=True)
                intro_text = re.sub(r'^.*?Introduction\s+of\s+.+?\s{2,}', '', intro_text)
                if intro_text == intro_section.get_text(" ", strip=True):
                    intro_text = re.sub(r'^.*?Introduction.*?\s{2,}', '', intro_text)
                record["Introduction"] = clean_text(intro_text)[:500]
            break

    # --- About ---
    for h3 in soup.find_all(['h3', 'h2']):
        if 'About' in h3.get_text() and 'Introduction' not in h3.get_text():
            about_section = h3.parent
            if about_section:
                about_text = about_section.get_text(" ", strip=True)
                about_text = re.sub(r'^.*?About\s+.+?\s{2,}', '', about_text)
                record["About"] = clean_text(about_text)[:1000]
            break

    # --- Keywords ---
    for h3 in soup.find_all(['h3', 'h2']):
        if 'Related Keywords' in h3.get_text():
            kw_section = h3.parent
            if kw_section:
                kw_text = kw_section.get_text(" ", strip=True)
                kw_text = kw_text.replace("Related Keywords", "").strip()
                record["Keywords"] = clean_text(kw_text)
            break

    # --- Logo ---
    logo_img = soup.find('img', alt=lambda x: x and 'Logo' in x)
    if logo_img and logo_img.get('src'):
        src = logo_img['src']
        if src.startswith('/'):
            src = BASE_URL + src
        record["Logo_URL"] = src

    return record


def save_progress():
    """Save results and progress to disk."""
    with lock:
        if not results:
            return
        df = pd.DataFrame(results)
        df.to_csv(CONTACTS_CSV, index=False)
        progress = {"completed_urls": list(completed_urls)}
        with open(PROGRESS_FILE, 'w') as f:
            json.dump(progress, f)


def load_progress():
    """Load previous progress for resume capability."""
    global completed_urls, results
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            data = json.load(f)
            completed_urls = set(data.get("completed_urls", []))
        print(f"  Resuming — {len(completed_urls)} already completed")
    if os.path.exists(CONTACTS_CSV):
        df = pd.read_csv(CONTACTS_CSV)
        results = df.to_dict('records')
        print(f"  Loaded {len(results)} existing records from CSV")


def run_step2():
    """Multithreaded scraping of company profile pages."""
    global errors_count

    if not os.path.exists(LISTINGS_CSV):
        print(f"ERROR: {LISTINGS_CSV} not found. Run step1 first.")
        return

    listings = pd.read_csv(LISTINGS_CSV)
    total = len(listings)
    print(f"Loaded {total} companies from {LISTINGS_CSV}")

    load_progress()

    # Filter out already completed
    pending = [(row['Profile_URL'], row['Name'])
               for _, row in listings.iterrows()
               if row['Profile_URL'] not in completed_urls]
    print(f"Pending: {len(pending)} companies")

    if not pending:
        print("All companies already scraped!")
        return

    done_count = len(completed_urls)
    start_time = time.time()
    session = make_session(STEP2_WORKERS)

    def process(item):
        nonlocal done_count
        url, name = item
        record = parse_profile(url, name, session)

        with lock:
            results.append(record)
            completed_urls.add(url)
            done_count += 1

            if done_count % SAVE_EVERY == 0:
                save_progress()
                elapsed = time.time() - start_time
                rate = done_count / elapsed * 3600 if elapsed > 0 else 0
                print(f"  [{done_count}/{total}] {done_count/total*100:.1f}% "
                      f"| {rate:.0f}/hr | errors: {errors_count} "
                      f"| last: {name[:40]}", flush=True)

        return record

    with ThreadPoolExecutor(max_workers=STEP2_WORKERS) as executor:
        futures = {executor.submit(process, item): item for item in pending}
        try:
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    item = futures[future]
                    print(f"  THREAD ERROR [{item[1]}]: {e}")
        except KeyboardInterrupt:
            print("\n\nInterrupted! Saving progress...")
            executor.shutdown(wait=False, cancel_futures=True)

    save_progress()
    elapsed = time.time() - start_time
    print(f"\n✓ Step 2 complete — {len(results)} records → {CONTACTS_CSV}")
    print(f"  Errors: {errors_count}, Time: {elapsed/60:.1f} min")


# ============================================================
#  EXPORT — JSON + Excel
# ============================================================

def run_export():
    """Create JSON and Excel files from the contacts CSV."""
    if not os.path.exists(CONTACTS_CSV):
        print(f"ERROR: {CONTACTS_CSV} not found. Run step2 first.")
        return

    df = pd.read_csv(CONTACTS_CSV)
    print(f"Loaded {len(df)} records from {CONTACTS_CSV}")

    # JSON
    json_path = CONTACTS_CSV.replace('.csv', '.json')
    df.to_json(json_path, orient='records', indent=2, force_ascii=False)
    print(f"  → {json_path}")

    # Excel
    xlsx_path = CONTACTS_CSV.replace('.csv', '.xlsx')
    df.to_excel(xlsx_path, index=False, engine='openpyxl')
    print(f"  → {xlsx_path}")

    # Stats
    print(f"\n--- Stats ---")
    print(f"Total records: {len(df)}")
    for col in ['Phone', 'Secondary_Phone', 'Website', 'Facebook', 'GPS_Lat',
                'Trading_Hours', 'Categories', 'Introduction', 'Keywords']:
        if col in df.columns:
            filled = df[col].notna() & (df[col].astype(str).str.strip() != '')
            pct = filled.sum() / len(df) * 100
            print(f"  {col}: {filled.sum()} ({pct:.1f}%)")


# ============================================================
#  MAIN
# ============================================================

if __name__ == "__main__":
    os.makedirs("output", exist_ok=True)

    if len(sys.argv) < 2:
        print("Usage: python scrape_bestdirectory.py [step1|step2|export]")
        sys.exit(1)

    cmd = sys.argv[1].lower()

    if cmd == "step1":
        run_step1()
    elif cmd == "step2":
        run_step2()
    elif cmd == "export":
        run_export()
    else:
        print(f"Unknown command: {cmd}")
        print("Usage: python scrape_bestdirectory.py [step1|step2|export]")
        sys.exit(1)

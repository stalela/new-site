"""
Bizcommunity Company Detail Scraper (Multithreaded)
====================================================
Reads bizcommunity_companies.json, visits each company profile/contact URL
in parallel, extracts detailed info (email, phone, address, services, etc.),
and saves batches of 50 to output/bizcommunity_batches/batch_XXXX.json.

After all scraping is done, merges all batch files into output/bizcommunity_profiles.json.

Supports resume — scans existing batch files to skip already-scraped companies.
"""

import requests
from bs4 import BeautifulSoup
import json
import os
import glob
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

# --- CONFIGURATION ---
INPUT_JSON = "output/bizcommunity_companies.json"
BATCH_DIR = "output/bizcommunity_batches"
MERGED_OUTPUT = "output/bizcommunity_profiles.json"
BASE_URL = "https://www.bizcommunity.com"
MAX_WORKERS = 8          # Number of parallel threads
BATCH_SIZE = 50          # Companies per batch file
REQUEST_TIMEOUT = 15     # Seconds per request

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

# Thread-safe lock for writing results
lock = threading.Lock()
batch_counter = 0


def _save_batch(batch_data, batch_num):
    """Save a small batch of companies to its own JSON file."""
    os.makedirs(BATCH_DIR, exist_ok=True)
    path = os.path.join(BATCH_DIR, f"batch_{batch_num:04d}.json")
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(batch_data, f, indent=2, ensure_ascii=False)
    return path


def _load_existing_batches():
    """Load all existing batch files and return (results_list, done_urls_set)."""
    results = []
    done_urls = set()
    if not os.path.isdir(BATCH_DIR):
        return results, done_urls, 0

    batch_files = sorted(glob.glob(os.path.join(BATCH_DIR, "batch_*.json")))
    max_num = 0
    for bf in batch_files:
        try:
            with open(bf, 'r', encoding='utf-8') as f:
                batch = json.load(f)
            results.extend(batch)
            for co in batch:
                if co.get('source_url'):
                    done_urls.add(co['source_url'])
            # Extract batch number from filename
            fname = os.path.basename(bf)
            num = int(fname.replace('batch_', '').replace('.json', ''))
            if num > max_num:
                max_num = num
        except Exception as e:
            print(f"Warning: skipping corrupt batch {bf}: {e}")
    return results, done_urls, max_num


def _merge_all_batches():
    """Merge all batch files into one final output file."""
    all_data = []
    batch_files = sorted(glob.glob(os.path.join(BATCH_DIR, "batch_*.json")))
    for bf in batch_files:
        try:
            with open(bf, 'r', encoding='utf-8') as f:
                all_data.extend(json.load(f))
        except Exception as e:
            print(f"Warning: skipping corrupt batch {bf}: {e}")

    with open(MERGED_OUTPUT, 'w', encoding='utf-8') as f:
        json.dump(all_data, f, indent=2, ensure_ascii=False)
    return len(all_data)


def extract_rtl_email(tag):
    """Reverse an RTL-obfuscated email if direction:rtl is in the style."""
    if not tag:
        return None
    raw = tag.get_text(strip=True)
    style = tag.get('style', '')
    if 'rtl' in style:
        return raw[::-1]
    return raw


def extract_standard_company(soup, data):
    """
    Extract data from a Standard company page (/Company/...).
    Structure: div.kInstanceCompany > h1 + <p> description + <table> rows.
    """
    container = soup.find('div', class_='kInstanceCompany')
    if not container:
        return data

    # Name from <h1>
    h1 = container.find('h1')
    if h1:
        data['name'] = h1.get_text(strip=True)

    # Description — first <p> inside the container (before the table)
    first_p = container.find('p')
    if first_p:
        data['short_description'] = first_p.get_text(strip=True)

    # Details table — rows are <tr><td>Key:</td><td>Value</td></tr>
    data['details'] = {}
    table = container.find('table')
    if table:
        for row in table.find_all('tr', recursive=False):
            cells = row.find_all('td', recursive=False)
            if len(cells) == 2:
                key = cells[0].get_text(strip=True).rstrip(':').strip()
                if not key:
                    continue

                # Check for links (e.g. Services list)
                links = cells[1].find_all('a')
                if key == 'Web address' or key == 'Website':
                    link = cells[1].find('a')
                    if link:
                        data['website'] = link.get('href', '')
                elif links and key != 'Web address':
                    data['details'][key] = [l.get_text(strip=True) for l in links]
                else:
                    val = cells[1].get_text(strip=True)
                    if val:
                        data['details'][key] = val

    # Contact info from nested kIC-Contact table
    contact_table = container.find('table', class_='kIC-Contact')
    if contact_table:
        locations = []
        for td in contact_table.find_all('td', style=True):
            location = {}

            # Extract RTL email BEFORE modifying the DOM
            email_link = td.find('a', class_='jxRevEL')
            if email_link:
                data['email'] = extract_rtl_email(email_link)
                location['email'] = data['email']

            # Use get_text with newline separator (don't modify DOM)
            block_text = td.get_text('\n', strip=True)
            lines = [l.strip() for l in block_text.split('\n') if l.strip()]

            if not lines:
                continue

            location = {}

            # First line is often the city/branch name (bold)
            b_tag = td.find('b')
            if b_tag:
                location['branch'] = b_tag.get_text(strip=True)

            # Parse lines for Contact, Tel, Physical/Postal address
            in_physical = False
            in_postal = False
            phys_lines = []
            postal_lines = []

            for line in lines:
                if line.startswith('Contact:'):
                    location['contact_person'] = line.replace('Contact:', '').strip()
                elif line.startswith('Tel:'):
                    phone = line.replace('Tel:', '').strip()
                    data['phone'] = phone
                    location['phone'] = phone
                elif 'Physical address' in line:
                    in_physical = True
                    in_postal = False
                elif 'Postal address' in line:
                    in_postal = True
                    in_physical = False
                elif line == 'South Africa' or line == location.get('branch'):
                    continue
                elif in_physical:
                    phys_lines.append(line)
                elif in_postal:
                    postal_lines.append(line)

            if phys_lines:
                addr = ', '.join(phys_lines)
                location['physical_address'] = addr
                if not data.get('physical_address'):
                    data['physical_address'] = addr
            if postal_lines:
                addr = ', '.join(postal_lines)
                location['postal_address'] = addr

            if location.get('branch') or location.get('phone') or location.get('physical_address'):
                locations.append(location)

        if locations:
            data['locations'] = locations
    else:
        data['locations'] = []

    return data


def extract_pressoffice_contact(soup, data):
    """
    Extract data from a Press Office contact page (/PressOffice/Contact.aspx?ci=...).
    Structure: div.elevated-container > <p> with <b>T.</b>, <b>E.</b>, <b>W.</b>, <b>A.</b>.
    """
    container = soup.find('div', class_='elevated-container')
    if not container:
        return data

    # Name from og:title meta or page <h1>
    meta_title = soup.find('meta', property='og:title')
    if meta_title and meta_title.get('content'):
        data['name'] = meta_title['content']

    # Parse the <p> block with T./E./W./A. labels
    p_tag = container.find('p')
    if p_tag:
        # Phone: <b>T.</b> text
        for b in p_tag.find_all('b'):
            label = b.get_text(strip=True)
            next_sib = b.next_sibling
            next_text = next_sib.strip() if isinstance(next_sib, str) else ''

            if label == 'T.':
                data['phone'] = next_text.strip()
            elif label == 'A.':
                data['physical_address'] = next_text.strip()

        # Email: RTL-reversed <a class="jxRevEL">
        email_link = p_tag.find('a', class_='jxRevEL')
        if email_link:
            data['email'] = extract_rtl_email(email_link)
        else:
            # Fallback: any link with rtl style
            for a in p_tag.find_all('a'):
                style = a.get('style', '')
                if 'rtl' in style:
                    data['email'] = extract_rtl_email(a)
                    break

        # Website: <b>W.</b> followed by <a>
        for b in p_tag.find_all('b'):
            if b.get_text(strip=True) == 'W.':
                web_link = b.find_next('a')
                if web_link and 'jxRevEL' not in web_link.get('class', []):
                    data['website'] = web_link.get('href', '')
                    break

    data['details'] = {}
    data['locations'] = []
    return data


def extract_company_profile(html_content, source_info):
    """
    Parse a company profile page and extract all available data.
    Detects page type (Standard vs Press Office) and delegates accordingly.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = {
        "name": source_info.get("name"),
        "category": source_info.get("category"),
        "type": source_info.get("type"),
        "source_url": source_info.get("contact_url"),
        "short_description": None,
        "website": None,
        "phone": None,
        "email": None,
        "physical_address": None,
    }

    url = source_info.get("contact_url", "")

    # Detect page type and use appropriate extractor
    if 'PressOffice/Contact' in url:
        data = extract_pressoffice_contact(soup, data)
    else:
        data = extract_standard_company(soup, data)

    return data


def scrape_one(company, session):
    """
    Fetch and parse a single company profile. Returns the enriched dict or None on failure.
    """
    url = company.get("contact_url")
    if not url:
        return None

    try:
        response = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            return None
        return extract_company_profile(response.content, company)
    except Exception as e:
        return None


def main():
    # 1. Load input list
    if not os.path.exists(INPUT_JSON):
        print(f"Error: {INPUT_JSON} not found. Run scrape_bizcommunity.py first.")
        return

    with open(INPUT_JSON, 'r') as f:
        companies = json.load(f)

    print(f"Loaded {len(companies)} companies from {INPUT_JSON}")

    # 2. Load existing batch files for resume
    existing_results, done_urls, last_batch_num = _load_existing_batches()
    if done_urls:
        print(f"Resuming: {len(done_urls)} companies already scraped across {last_batch_num} batches.")

    # 3. Filter to only unprocessed companies
    to_scrape = [c for c in companies if c.get('contact_url') not in done_urls]
    print(f"Remaining to scrape: {len(to_scrape)}")

    if not to_scrape:
        print("Nothing to do — all companies already scraped.")
        return

    # 4. Scrape in parallel, saving in batches of BATCH_SIZE
    completed = 0
    errors = 0
    start_time = time.time()
    current_batch = []
    current_batch_num = last_batch_num + 1

    # Use a Session per thread for connection pooling
    def make_session():
        s = requests.Session()
        s.headers.update(HEADERS)
        return s

    # Thread-local sessions
    thread_local = threading.local()

    def get_session():
        if not hasattr(thread_local, 'session'):
            thread_local.session = make_session()
        return thread_local.session

    def worker(company):
        session = get_session()
        return scrape_one(company, session)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(worker, co): co for co in to_scrape}

        for future in as_completed(futures):
            result = future.result()

            with lock:
                if result:
                    current_batch.append(result)
                    completed += 1
                else:
                    errors += 1

                total_done = completed + errors

                # Progress log every 25
                if total_done % 25 == 0:
                    elapsed = time.time() - start_time
                    rate = total_done / elapsed if elapsed > 0 else 0
                    eta_sec = (len(to_scrape) - total_done) / rate if rate > 0 else 0
                    eta_min = eta_sec / 60
                    print(
                        f"  Progress: {total_done}/{len(to_scrape)} "
                        f"({completed} ok, {errors} failed) | "
                        f"{rate:.1f}/s | ETA: {eta_min:.1f} min"
                    )

                # Save batch when it reaches BATCH_SIZE
                if len(current_batch) >= BATCH_SIZE:
                    path = _save_batch(current_batch, current_batch_num)
                    print(f"  [Batch {current_batch_num}] Saved {len(current_batch)} profiles to {path}")
                    current_batch = []
                    current_batch_num += 1

    # Save any remaining companies in the last partial batch
    if current_batch:
        path = _save_batch(current_batch, current_batch_num)
        print(f"  [Batch {current_batch_num}] Saved {len(current_batch)} profiles to {path}")

    # 5. Merge all batches into one file
    print("\nMerging all batches...")
    total = _merge_all_batches()

    elapsed = time.time() - start_time
    print(f"Done! Scraped {completed} profiles ({errors} failures) in {elapsed/60:.1f} minutes.")
    print(f"Total profiles in {MERGED_OUTPUT}: {total}")


if __name__ == "__main__":
    main()

"""
Step 2: Scrape contact details (phone, email, website) from each company's
contactus.html page on South Africa Yellow Pages.

Uses multithreading (ThreadPoolExecutor) for speed.
Saves progress incrementally so you can resume if interrupted.
"""

import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import re
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

# --- CONFIGURATION ---
INPUT_CSV = "output/south_africa_yellow_pages.csv"
OUTPUT_CSV = "output/south_africa_yellow_pages_contacts.csv"
PROGRESS_FILE = "output/yp_contacts_progress.json"
MAX_WORKERS = 10       # concurrent threads
TIMEOUT = 10           # seconds per request
SAVE_EVERY = 200       # save progress every N records

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
}

lock = Lock()
results = []
completed_urls = set()
errors_count = 0


def get_contact_url(profile_url: str) -> str:
    """Convert .../index.html to .../contactus.html"""
    return profile_url.replace("/index.html", "/contactus.html")


def clean_text(text: str) -> str:
    """Clean whitespace and special chars from extracted text."""
    text = re.sub(r'[\u25a1\u00a0]+', ' ', text)  # replace box chars and nbsp
    text = re.sub(r'\s+', ' ', text)               # collapse whitespace
    return text.strip().strip(",").strip()


def extract_phone(section) -> str:
    """Extract phone number from the Phone section."""
    body = section.find('div', class_='contact-body')
    if not body:
        return ""
    text = body.get_text(" ", strip=True)
    # Remove the "Phone" label
    text = re.sub(r'^Phone\s*', '', text, flags=re.IGNORECASE)
    text = clean_text(text)
    return text if text else ""


def extract_email(section) -> str:
    """Extract email from the Email section (if visible as text, not behind modal)."""
    body = section.find('div', class_='contact-body')
    if not body:
        return ""
    text = body.get_text(" ", strip=True)
    # Remove "Email" and "Get Quote" labels
    text = re.sub(r'^Email\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'Get Quote', '', text, flags=re.IGNORECASE)
    text = clean_text(text)

    # Try to find an actual email pattern
    email_match = re.search(r'[\w.+-]+@[\w.-]+\.\w+', text)
    if email_match:
        return email_match.group(0)

    # Also check for mailto links
    mailto = body.find('a', href=lambda x: x and 'mailto:' in x)
    if mailto:
        return mailto['href'].replace('mailto:', '').strip()

    return text if text and text != "Get Quote" else ""


def extract_website(section) -> str:
    """Extract website URL from the Website section."""
    # Check for <a> links
    for a in section.find_all('a'):
        href = a.get('href', '')
        if href and href.startswith('http') and 'gulfyp.com' not in href and 'southafricayellowpages' not in href:
            return href.strip()

    # Check for raw text that looks like a URL
    body = section.find('div', class_='contact-body')
    if body:
        text = body.get_text(" ", strip=True)
        url_match = re.search(r'(https?://\S+)', text)
        if url_match:
            return url_match.group(1)
        # Check for www. patterns
        www_match = re.search(r'(www\.\S+)', text)
        if www_match:
            return www_match.group(1)

    return ""


def extract_address(section) -> str:
    """Extract address from the Address/Location section."""
    body = section.find('div', class_='contact-body')
    if not body:
        return ""
    # Get all <p> text, skip the company name (in <strong>)
    strong = body.find('strong')
    name = strong.get_text(strip=True) if strong else ""

    text = body.get_text(" ", strip=True)
    # Remove the company name
    if name:
        text = text.replace(name, "", 1)
    text = clean_text(text)
    # Remove trailing "South Africa" if duplicated
    text = re.sub(r'\s*South Africa\s*$', '', text, flags=re.IGNORECASE)
    return text.strip().strip(",").strip()


def scrape_contact(row: dict) -> dict:
    """Scrape a single company's contact page."""
    global errors_count

    name = row["Name"]
    profile_url = row["Profile_URL"]
    original_address = row["Address"]
    contact_url = get_contact_url(profile_url)

    result = {
        "Name": name,
        "Address": original_address,
        "Profile_URL": profile_url,
        "Contact_Address": "",
        "Phone": "",
        "Email": "",
        "Website": "",
        "Status": "OK"
    }

    try:
        resp = requests.get(contact_url, headers=headers, timeout=TIMEOUT)
        if resp.status_code != 200:
            result["Status"] = f"HTTP {resp.status_code}"
            return result

        soup = BeautifulSoup(resp.content, 'html.parser')
        info = soup.find('div', class_='contact-info')

        if not info:
            result["Status"] = "no contact-info div"
            return result

        sections = info.find_all('div', class_='col-md-4')

        # Section 0: Address/Location
        if len(sections) > 0:
            result["Contact_Address"] = extract_address(sections[0])

        # Section 1: Phone
        if len(sections) > 1:
            result["Phone"] = extract_phone(sections[1])

        # Section 2: Email
        if len(sections) > 2:
            result["Email"] = extract_email(sections[2])

        # Section 3: Website
        if len(sections) > 3:
            result["Website"] = extract_website(sections[3])

    except requests.exceptions.Timeout:
        result["Status"] = "timeout"
    except requests.exceptions.ConnectionError:
        result["Status"] = "connection_error"
    except Exception as e:
        result["Status"] = f"error: {str(e)[:80]}"
        errors_count += 1

    return result


def save_progress(results_list, completed_set):
    """Save results CSV and progress tracker."""
    df = pd.DataFrame(results_list)
    df.to_csv(OUTPUT_CSV, index=False)
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(list(completed_set), f)


def load_progress():
    """Load previously completed URLs to allow resume."""
    done_urls = set()
    existing_results = []

    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            done_urls = set(json.load(f))

    if os.path.exists(OUTPUT_CSV) and done_urls:
        df = pd.read_csv(OUTPUT_CSV)
        existing_results = df.to_dict('records')

    return done_urls, existing_results


def main():
    global results, completed_urls, errors_count

    # Load input
    df = pd.read_csv(INPUT_CSV)
    total = len(df)
    print(f"Loaded {total} companies from {INPUT_CSV}")

    # Load progress for resume
    completed_urls, results = load_progress()
    if completed_urls:
        print(f"Resuming: {len(completed_urls)} already done, {total - len(completed_urls)} remaining")

    # Filter to only unfinished
    remaining = [row for _, row in df.iterrows() if row["Profile_URL"] not in completed_urls]
    print(f"Scraping {len(remaining)} contact pages with {MAX_WORKERS} threads...\n")

    done = len(completed_urls)
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(scrape_contact, row.to_dict()): row for row in remaining}

        for future in as_completed(futures):
            result = future.result()
            with lock:
                results.append(result)
                completed_urls.add(result["Profile_URL"])
                done += 1

                # Progress update
                if done % 50 == 0 or done == total:
                    elapsed = time.time() - start_time
                    rate = (done - len(completed_urls) + len(remaining)) / max(elapsed, 1)
                    phones = sum(1 for r in results if r["Phone"])
                    eta_min = (total - done) / max(rate, 0.1) / 60
                    print(f"  [{done}/{total}] phones: {phones} | "
                          f"rate: {rate:.1f}/s | "
                          f"ETA: {eta_min:.0f}min | "
                          f"errors: {errors_count}")

                # Incremental save
                if done % SAVE_EVERY == 0:
                    save_progress(results, completed_urls)
                    print(f"  -> Progress saved ({done} records)")

    # Final save
    save_progress(results, completed_urls)

    # Summary
    df_out = pd.DataFrame(results)
    phones = df_out["Phone"].astype(bool).sum()
    emails = df_out["Email"].astype(bool).sum()
    websites = df_out["Website"].astype(bool).sum()
    ok = (df_out["Status"] == "OK").sum()
    elapsed = time.time() - start_time

    print(f"\n{'='*50}")
    print(f"DONE in {elapsed/60:.1f} minutes")
    print(f"Total records: {len(df_out)}")
    print(f"  With phone:   {phones}")
    print(f"  With email:   {emails}")
    print(f"  With website: {websites}")
    print(f"  Status OK:    {ok}")
    print(f"  Errors:       {errors_count}")
    print(f"Saved to: {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

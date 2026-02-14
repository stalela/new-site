import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from urllib.parse import urljoin
import os

# --- CONFIGURATION ---
INPUT_CSV = "output/bizcommunity_categories.csv"
OUTPUT_JSON = "output/bizcommunity_companies.json"
BASE_URL = "https://www.bizcommunity.com"

# Headers to mimic a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def extract_companies_from_page(soup, category_name):
    """
    Extracts companies from both the Press Office table and the Standard Company table.
    """
    page_data = []

    # 1. FIND ALL LISTING TABLES
    # The page uses the class 'kBrowseTable' for both types of lists
    tables = soup.find_all('table', class_='kBrowseTable')

    for table in tables:
        rows = table.find_all('tr')
        
        for row in rows:
            company_info = {
                "category": category_name,
                "name": None,
                "contact_url": None,
                "type": "Standard" # Default
            }

            # --- CASE A: PRESS OFFICE LISTING (Has Logo & Specific 'Contact' link) ---
            if "kBrowseCompany-PressOffice" in table.get('class', []):
                company_info["type"] = "Press Office"
                
                # Get Name
                name_tag = row.find('a', class_='kBrowseCompany-NameLink')
                if name_tag:
                    company_info["name"] = name_tag.get_text(strip=True)
                
                # Get Contact URL (Specifically look for the 'Contact' text link)
                contact_link = row.find('a', string="Contact")
                if contact_link:
                    company_info["contact_url"] = urljoin(BASE_URL, contact_link['href'])
                elif name_tag:
                    # Fallback to profile URL if specific contact link missing
                    company_info["contact_url"] = urljoin(BASE_URL, name_tag['href'])

            # --- CASE B: STANDARD COMPANY LISTING (Text only) ---
            else:
                # The first cell usually contains the Name Link
                cells = row.find_all('td')
                if cells:
                    name_tag = cells[0].find('a')
                    if name_tag:
                        company_info["name"] = name_tag.get_text(strip=True)
                        # For standard listings, the profile URL is the contact entry point
                        company_info["contact_url"] = urljoin(BASE_URL, name_tag['href'])

            # Only add if we found a name
            if company_info["name"]:
                page_data.append(company_info)

    return page_data

def get_next_page_url(soup):
    """
    Finds the 'Next' button url for pagination.
    """
    # Based on HTML: <a class="... biz-btn--filled" ...><span>Next</span></a>
    next_btn = soup.find('a', class_='biz-btn--filled')
    if next_btn and "Next" in next_btn.get_text():
        return urljoin(BASE_URL, next_btn['href'])
    return None

def main():
    # 1. Load Categories
    if not os.path.exists(INPUT_CSV):
        print(f"Error: {INPUT_CSV} not found. Please run the category scraper first.")
        return

    df = pd.read_csv(INPUT_CSV)
    
    # Store all results here — resume from existing file if present
    all_companies = []
    seen_urls = set()
    processed_categories = set()

    if os.path.exists(OUTPUT_JSON):
        with open(OUTPUT_JSON, 'r') as f:
            all_companies = json.load(f)
        for co in all_companies:
            seen_urls.add(co['contact_url'])
            processed_categories.add(co['category'])
        print(f"Resuming: loaded {len(all_companies)} existing companies from {len(processed_categories)} categories.")

    print(f"Loaded {len(df)} categories. Starting scrape...")

    # 2. Iterate through categories
    for index, row in df.iterrows():
        category = row['Category_Name']
        current_url = row['URL']
        
        # Skip already-processed categories on resume
        if category in processed_categories:
            print(f"--- [{index+1}/{len(df)}] Skipping (already done): {category} ---")
            continue

        print(f"--- [{index+1}/{len(df)}] Processing Category: {category} ---")
        
        consecutive_empty_pages = 0
        while current_url:
            try:
                print(f"  Scraping: {current_url}")
                response = requests.get(current_url, headers=HEADERS, timeout=10)
                
                if response.status_code != 200:
                    print(f"  Failed to load page: {response.status_code}")
                    break

                soup = BeautifulSoup(response.content, 'html.parser')

                # Extract data
                companies = extract_companies_from_page(soup, category)
                
                new_count = 0
                # Add to master list (checking for duplicates)
                for co in companies:
                    if co['contact_url'] not in seen_urls:
                        all_companies.append(co)
                        seen_urls.add(co['contact_url'])
                        new_count += 1

                print(f"  Found {len(companies)} companies ({new_count} new, {len(companies) - new_count} duplicates)")

                # Stop pagination if we keep getting 0 new companies
                if new_count == 0:
                    consecutive_empty_pages += 1
                    if consecutive_empty_pages >= 2:
                        print(f"  Stopping pagination: {consecutive_empty_pages} consecutive pages with 0 new companies.")
                        break
                else:
                    consecutive_empty_pages = 0

                # Handle Pagination
                current_url = get_next_page_url(soup)
                
                # Polite delay
                time.sleep(1)

            except Exception as e:
                print(f"  Error extracting {current_url}: {e}")
                break
        
        # Save intermediate results every 5 categories just in case
        if index % 5 == 0:
            with open(OUTPUT_JSON, 'w') as f:
                json.dump(all_companies, f, indent=4)
            print(f"  [Checkpoint] Saved {len(all_companies)} companies so far.")

    # 3. Final Save
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(all_companies, f, indent=4)
    
    print(f"\nScraping Complete!")
    print(f"Total Unique Companies Found: {len(all_companies)}")
    print(f"Data saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    main()

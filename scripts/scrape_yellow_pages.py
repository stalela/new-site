import requests
from bs4 import BeautifulSoup
import pandas as pd
import string
import time
import random

# --- CONFIGURATION ---
BASE_URL = "https://southafricayellowpages.com/companies.htm"
LETTERS = list(string.ascii_uppercase)  # A-Z

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def scrape_letter(letter):
    """Scrape all companies for a given letter."""
    url = f"{BASE_URL}?prodletter={letter}&s=1"
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, 'html.parser')
    company_links = soup.find_all('a', href=lambda x: x and '/co-' in x)

    companies = []
    for link in company_links:
        try:
            name = link.get_text(strip=True)
            profile_url = link['href']

            parent_li = link.find_parent('li')
            address = "N/A"
            if parent_li:
                full_text = parent_li.get_text(" ", strip=True)
                address = full_text.replace(name, "").strip().strip(",").strip()

            companies.append({
                "Name": name,
                "Address": address,
                "Profile_URL": profile_url
            })
        except Exception as e:
            print(f"  Error parsing item: {e}")
            continue

    return companies

def scrape_yellow_pages():
    """
    Step 1: Scrape company name, address, and profile URL from the
    South Africa Yellow Pages, iterating through prodletter=A to Z
    to ensure full coverage.
    """
    all_companies = []
    seen_urls = set()

    for letter in LETTERS:
        print(f"[{letter}] Fetching...", end=" ", flush=True)
        try:
            companies = scrape_letter(letter)
            # Deduplicate by profile URL
            new = 0
            for c in companies:
                if c["Profile_URL"] not in seen_urls:
                    seen_urls.add(c["Profile_URL"])
                    all_companies.append(c)
                    new += 1
            print(f"found {len(companies)}, new: {new}, total: {len(all_companies)}")
        except Exception as e:
            print(f"ERROR: {e}")

        time.sleep(random.uniform(1, 2))

    # --- SAVE TO CSV ---
    df = pd.DataFrame(all_companies)
    df.to_csv("output/south_africa_yellow_pages.csv", index=False)
    print(f"\nDone! Saved to output/south_africa_yellow_pages.csv")
    print(f"Total unique companies: {len(df)}")

if __name__ == "__main__":
    scrape_yellow_pages()

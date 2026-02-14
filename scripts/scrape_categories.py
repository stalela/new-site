import requests
from bs4 import BeautifulSoup
import pandas as pd
from urllib.parse import urljoin

# 1. Setup
base_url = "https://www.bizcommunity.com"
start_url = "https://www.bizcommunity.com/Companies/196/1.html"

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

def scrape_categories():
    print("Fetching category list...")
    try:
        response = requests.get(start_url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        category_data = []

        # The categories are located in divs with class 'kBrowseList'
        browse_lists = soup.find_all('div', class_='kBrowseList')

        for container in browse_lists:
            # The actual lists are hidden in <ul> tags with IDs starting with "xMore_"
            # We filter for these specific lists to avoid grabbing navigation menus
            uls = container.find_all('ul', id=lambda x: x and x.startswith('xMore_'))

            for ul in uls:
                # 1. Find the Parent Industry Name
                # The <h2> header is the element immediately preceding the <ul>
                parent_header = ul.find_previous_sibling('h2')
                
                industry_name = "Unknown"
                if parent_header:
                    # Text looks like: "Marketing & Media (7783)"
                    raw_text = parent_header.get_text(strip=True)
                    # Split by '(' to remove the count number
                    industry_name = raw_text.split('(')[0].strip()

                # 2. Extract Sub-categories inside the <ul>
                links = ul.find_all('a')
                
                for link in links:
                    cat_name = link.get_text(strip=True)
                    cat_href = link['href']
                    full_url = urljoin(base_url, cat_href)

                    # Store the data
                    category_data.append({
                        "Industry": industry_name,
                        "Category_Name": cat_name,
                        "URL": full_url
                    })
                    
        return category_data

    except Exception as e:
        print(f"Error: {e}")
        return []

# --- RUN THE SCRIPT ---
if __name__ == "__main__":
    categories = scrape_categories()

    if categories:
        # Convert to DataFrame for easy viewing/saving
        df = pd.DataFrame(categories)
        
        print(f"\nSuccessfully extracted {len(df)} categories.")
        print(df.head(10).to_string())
        
        # Save to CSV
        output_path = "output/bizcommunity_categories.csv"
        df.to_csv(output_path, index=False)
        print(f"\nSaved to {output_path}")
    else:
        print("No categories found.")

#!/usr/bin/env python3
"""
Simple scraper to extract xHamster video URLs from a local HTML file or a page URL.
Hardcoded configuration - edit the values below to change settings.
"""

import json
import os
import re
import sys
import time

try:
    import requests
    from bs4 import BeautifulSoup
except Exception as e:
    print("Missing dependency: please run 'pip install requests beautifulsoup4'")
    raise

try:
    import pymongo
    HAS_MONGODB = True
except ImportError:
    HAS_MONGODB = False


# ============================================================================
# CONFIGURATION - Edit these values to change behavior
# ============================================================================
INPUT = "https://xhamster.com/1"  # URL or file path
OUTPUT = "urls.txt"  # Output file (None to skip)
DOMAIN = "https://xhamster.com"  # Base domain
START_PAGE = 1  # Start page number
END_PAGE = 22921  # End page number
DELAY = 10  # Delay between requests in seconds

MONGODB_URI = "mongodb+srv://indiandigitalservice01:V2aQpeBve8H54rRW@sarkariresult.womtf2i.mongodb.net/xxxdata"
MONGODB_COLLECTION = "video_urls"  # Collection name
# ============================================================================


def extract_video_urls(html: str, domain: str = "https://xhamster.com") -> list:
    soup = BeautifulSoup(html, "lxml")
    seen = set()
    results = []

    # List of domains to exclude (thumbnails, previews, etc.)
    exclude_domains = [
        "xhpingcdn.com",
        "ic-vt-nss.xhpingcdn.com",
        "ic-tt-nss.xhpingcdn.com",
        "ic-vrm-nss.xhpingcdn.com",
        "thumb-v",
        "video.flirtify.com",
    ]

    def is_valid_video_url(url: str) -> bool:
        """Check if URL is a main xhamster video page (not a preview/thumbnail)"""
        # Must be xhamster.com video page
        if "xhamster.com" not in url:
            return False
        
        # Exclude thumbnails and preview CDNs
        for exclude in exclude_domains:
            if exclude in url:
                return False
        
        # Must contain /videos/ and be from main domain
        if "/videos/" not in url:
            return False
        
        # Exclude creator/channel video listings
        if "/creators/videos/" in url or "/channels/videos/" in url:
            return False
        
        return True

    # Look for anchor tags with /videos/ in href
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/videos/" not in href:
            continue

        # Normalize to absolute URL
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            href = domain.rstrip("/") + href
        elif not href.startswith("http"):
            href = domain.rstrip("/") + "/" + href.lstrip("/")

        # avoid javascript: or mailto
        if href.startswith("javascript:") or href.startswith("mailto:"):
            continue

        # Validate it's a real video page, not preview/creator page
        if is_valid_video_url(href) and href not in seen:
            seen.add(href)
            results.append(href)

    return results


def main():
    """Main function using hardcoded configuration"""
    input_src = INPUT
    output_file_path = OUTPUT
    domain = DOMAIN
    start = START_PAGE
    end = END_PAGE
    delay = DELAY
    mongodb_uri = MONGODB_URI
    mongodb_collection = MONGODB_COLLECTION
    
    global_seen = set()  # Track seen URLs across all pages
    total_urls = 0

    # Determine if input is a URL or file
    is_url = input_src.startswith("http://") or input_src.startswith("https://")

    # Initialize output file if requested
    output_file = None
    if output_file_path and is_url:
        try:
            output_file = open(output_file_path, "w", encoding="utf-8")
        except Exception as e:
            print(f"✗ Error opening output file: {e}")
            sys.exit(1)

    # Initialize MongoDB connection if requested
    db_collection = None
    client = None
    if mongodb_uri:
        if not HAS_MONGODB:
            print("✗ MongoDB support requires 'pymongo'. Install with: pip install pymongo")
            sys.exit(1)
        try:
            client = pymongo.MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db_name = mongodb_uri.split('/')[-1].split('?')[0] if '/' in mongodb_uri else 'default'
            db = client[db_name]
            db_collection = db[mongodb_collection]
            print(f"✓ Connected to MongoDB: {db_name}/{mongodb_collection}")
        except Exception as e:
            print(f"✗ Error connecting to MongoDB: {e}")
            if output_file:
                output_file.close()
            sys.exit(1)

    if is_url:
        # URL mode: loop through pages and save each immediately
        print(f"Scraping pages {start} to {end}...")
        for page in range(start, end + 1):
            try:
                # Build page URL
                if page == 1 and input_src.rstrip("/").endswith(("https://xhamster.com", "xhamster.com")):
                    page_url = input_src.rstrip("/") + "/1"
                else:
                    if re.search(r'/\d+$', input_src):
                        page_url = re.sub(r'/\d+$', f'/{page}', input_src)
                    else:
                        page_url = input_src.rstrip("/") + f"/{page}"

                print(f"\n  Page {page}: {page_url}")
                r = requests.get(page_url, timeout=15)
                r.raise_for_status()
                html = r.text
                
                # Extract URLs from this page only
                urls = extract_video_urls(html, domain=domain)
                page_new_urls = 0
                
                # Process each URL immediately (don't load all in memory)
                for url in urls:
                    if url not in global_seen:
                        global_seen.add(url)
                        page_new_urls += 1
                        total_urls += 1
                        
                        # Save to file immediately
                        if output_file is not None:
                            output_file.write(url + "\n")
                            output_file.flush()  # Write immediately, don't buffer
                        
                        # Save to MongoDB immediately
                        if db_collection is not None:
                            try:
                                db_collection.insert_one({
                                    "url": url,
                                    "page": page,
                                    "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                                    "source": "videoscrepper"
                                })
                            except pymongo.errors.DuplicateKeyError:
                                pass  # URL already exists
                            except Exception as e:
                                print(f"    ✗ Error saving to MongoDB: {e}")
                
                print(f"    Found {len(urls)} URLs, {page_new_urls} new (total: {total_urls})")
                
                # Delay between requests
                if page < end:
                    time.sleep(delay)
                    
            except Exception as e:
                print(f"  ✗ Error fetching page {page}: {e}")
                continue
        
        # Close file
        if output_file is not None:
            output_file.close()
            print(f"\n✓ Saved {total_urls} URLs to: {output_file_path}")
        
        # Close MongoDB
        if client is not None:
            client.close()
            print(f"✓ Saved {total_urls} URLs to MongoDB")
    
    else:
        # File mode: read single HTML file
        if not os.path.exists(input_src):
            print(f"Input file not found: {input_src}")
            sys.exit(1)
        
        with open(input_src, "r", encoding="utf-8", errors="replace") as fh:
            html = fh.read()
        
        all_urls = extract_video_urls(html, domain=domain)
        
        print(f"\n=== Summary ===")
        print(f"Total URLs found: {len(all_urls)}")
        
        # Save file mode
        if output_file_path:
            try:
                if output_file_path.lower().endswith(".json"):
                    with open(output_file_path, "w", encoding="utf-8") as f:
                        json.dump(all_urls, f, ensure_ascii=False, indent=2)
                else:
                    with open(output_file_path, "w", encoding="utf-8") as f:
                        for u in all_urls:
                            f.write(u + "\n")
                print(f"✓ Saved {len(all_urls)} URLs to: {output_file_path}")
            except Exception as e:
                print(f"✗ Error saving output: {e}")
        
        # Save MongoDB for file mode
        if db_collection is not None:
            inserted = 0
            for url in all_urls:
                try:
                    db_collection.insert_one({
                        "url": url,
                        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "videoscrepper"
                    })
                    inserted += 1
                except pymongo.errors.DuplicateKeyError:
                    pass
                except Exception as e:
                    print(f"Error: {e}")
            if client is not None:
                client.close()
            print(f"✓ Saved {inserted} URLs to MongoDB")


if __name__ == "__main__":
    main()

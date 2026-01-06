#!/usr/bin/env python3
"""
Simple scraper to extract xHamster video URLs from a local HTML file or a page URL.
Usage examples:
  python videoscrepper.py --input data.html --output urls.json
  python videoscrepper.py --input https://xhamster.com/1 --output urls.txt

Requires: requests, beautifulsoup4
Install: pip install requests beautifulsoup4
"""

import argparse
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

    # Regex fallback: extract only xhamster.com/videos/ URLs (not previews)
    pattern = re.compile(r"https?://xhamster\.com/videos/[-A-Za-z0-9_%]+(?:[-A-Za-z0-9_%/]+)?")
    for match in pattern.finditer(html):
        url = match.group(0)
        if is_valid_video_url(url) and url not in seen:
            seen.add(url)
            results.append(url)

    return results


def save_to_mongodb(urls: list, mongodb_uri: str, collection_name: str):
    """Save URLs to MongoDB one by one, avoiding duplicates"""
    if not HAS_MONGODB:
        print("\n✗ MongoDB support requires 'pymongo'. Install with: pip install pymongo")
        return
    
    try:
        print(f"\n=== Saving to MongoDB ===")
        print(f"Connecting to: {mongodb_uri.split('@')[1] if '@' in mongodb_uri else 'local'}")
        
        client = pymongo.MongoClient(mongodb_uri, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        print("✓ Connected to MongoDB")
        
        # Get database and collection
        db_name = mongodb_uri.split('/')[-1].split('?')[0] if '/' in mongodb_uri else 'default'
        db = client[db_name]
        collection = db[collection_name]
        
        print(f"✓ Using database: {db_name}, collection: {collection_name}")
        
        # Insert URLs one by one, avoiding duplicates
        inserted = 0
        skipped = 0
        
        for idx, url in enumerate(urls, 1):
            try:
                # Insert only if URL doesn't exist
                result = collection.insert_one({
                    "url": url,
                    "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                    "source": "videoscrepper"
                })
                inserted += 1
                if idx % 10 == 0:
                    print(f"  Inserted {inserted}/{len(urls)} URLs...")
            except pymongo.errors.DuplicateKeyError:
                skipped += 1
            except Exception as e:
                print(f"  Error inserting URL {url}: {e}")
        
        print(f"\n✓ MongoDB Save Complete")
        print(f"  - Inserted: {inserted} new URLs")
        print(f"  - Skipped: {skipped} duplicate URLs")
        print(f"  - Total: {len(urls)} URLs processed")
        
        client.close()
    except Exception as e:
        print(f"\n✗ Error connecting to MongoDB: {e}")
        print("  Ensure the connection string is correct and the database is accessible.")


def main():
    parser = argparse.ArgumentParser(description="Extract xHamster video URLs from HTML file or page URL(s) with pagination support")
    parser.add_argument("--input", "-i", required=True, help="Path to local HTML file or a page URL (e.g., https://xhamster.com/1)")
    parser.add_argument("--output", "-o", help="Output file (json or txt). If omitted, prints to stdout")
    parser.add_argument("--domain", default="https://xhamster.com", help="Base domain to resolve relative links")
    parser.add_argument("--start-page", type=int, default=1, help="Start page number (for URL pagination, default: 1)")
    parser.add_argument("--end-page", type=int, help="End page number (inclusive). If omitted, scrapes only the input page")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between page requests in seconds (default: 1.0)")
    parser.add_argument("--mongodb-uri", help="MongoDB connection URI (e.g., mongodb+srv://user:pass@host/dbname)")
    parser.add_argument("--mongodb-collection", default="video_urls", help="MongoDB collection name (default: video_urls)")

    args = parser.parse_args()

    input_src = args.input
    global_seen = set()  # Track seen URLs across all pages
    total_urls = 0

    # Determine if input is a URL or file
    is_url = input_src.startswith("http://") or input_src.startswith("https://")

    # Initialize output file if requested
    output_file = None
    if args.output and is_url:
        try:
            output_file = open(args.output, "w", encoding="utf-8")
        except Exception as e:
            print(f"✗ Error opening output file: {e}")
            sys.exit(1)

    # Initialize MongoDB connection if requested
    db_collection = None
    if args.mongodb_uri:
        if not HAS_MONGODB:
            print("✗ MongoDB support requires 'pymongo'. Install with: pip install pymongo")
            sys.exit(1)
        try:
            client = pymongo.MongoClient(args.mongodb_uri, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db_name = args.mongodb_uri.split('/')[-1].split('?')[0] if '/' in args.mongodb_uri else 'default'
            db = client[db_name]
            db_collection = db[args.mongodb_collection]
            print(f"✓ Connected to MongoDB: {db_name}/{args.mongodb_collection}")
        except Exception as e:
            print(f"✗ Error connecting to MongoDB: {e}")
            if output_file:
                output_file.close()
            sys.exit(1)

    if is_url:
        # URL mode: loop through pages and save each immediately
        base_url = input_src
        start = args.start_page
        end = args.end_page if args.end_page else args.start_page

        print(f"Scraping pages {start} to {end}...")
        for page in range(start, end + 1):
            try:
                # Build page URL
                if page == 1 and base_url.rstrip("/").endswith(("https://xhamster.com", "xhamster.com")):
                    page_url = base_url.rstrip("/") + "/1"
                else:
                    import re as re_module
                    if re_module.search(r'/\d+$', base_url):
                        page_url = re_module.sub(r'/\d+$', f'/{page}', base_url)
                    else:
                        page_url = base_url.rstrip("/") + f"/{page}"

                print(f"\n  Page {page}: {page_url}")
                r = requests.get(page_url, timeout=15)
                r.raise_for_status()
                html = r.text
                
                # Extract URLs from this page only
                urls = extract_video_urls(html, domain=args.domain)
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
                    time.sleep(args.delay)
                    
            except Exception as e:
                print(f"  ✗ Error fetching page {page}: {e}")
                continue
        
        # Close file
        if output_file is not None:
            output_file.close()
            print(f"\n✓ Saved {total_urls} URLs to: {args.output}")
        
        # Close MongoDB
        if db_collection is not None:
            print(f"✓ Saved {total_urls} URLs to MongoDB")
    
    else:
        # File mode: read single HTML file
        if not os.path.exists(input_src):
            print(f"Input file not found: {input_src}")
            sys.exit(1)
        
        with open(input_src, "r", encoding="utf-8", errors="replace") as fh:
            html = fh.read()
        
        all_urls = extract_video_urls(html, domain=args.domain)
        
        print(f"\n=== Summary ===")
        print(f"Total URLs found: {len(all_urls)}")
        print(json.dumps(all_urls, indent=2))
        
        # Save file mode
        if args.output:
            try:
                if args.output.lower().endswith(".json"):
                    with open(args.output, "w", encoding="utf-8") as f:
                        json.dump(all_urls, f, ensure_ascii=False, indent=2)
                else:
                    with open(args.output, "w", encoding="utf-8") as f:
                        for u in all_urls:
                            f.write(u + "\n")
                print(f"✓ Saved {len(all_urls)} URLs to: {args.output}")
            except Exception as e:
                print(f"✗ Error saving output: {e}")
        
        # Save MongoDB for file mode
        if db_collection is not None:
            for url in all_urls:
                try:
                    db_collection.insert_one({
                        "url": url,
                        "scraped_at": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "source": "videoscrepper"
                    })
                except pymongo.errors.DuplicateKeyError:
                    pass
                except Exception as e:
                    print(f"Error: {e}")
            print(f"✓ Saved {len(all_urls)} URLs to MongoDB")


if __name__ == "__main__":
    main()

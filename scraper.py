# ==========================================
# IMPORTS
# ==========================================

import requests
from bs4 import BeautifulSoup
import re
import json
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================================
# CONFIG
# ==========================================

SCRAPERAPI_URL = "https://api.scraperapi.com"

session = requests.Session()

# ==========================================
# UTILS
# ==========================================

def extract_shop_name(url):
    url = url.strip().rstrip("/")
    match = re.search(r"etsy\.com/(?:[a-z]{2}/)?shop/([A-Za-z0-9_]+)", url)
    if match:
        return match.group(1)

    if not url.startswith("http") and re.match(r"^[A-Za-z0-9_]+$", url):
        return url

    return None


def parse_number(text):
    if not text:
        return 0

    text = text.replace(",", "").replace("\u202f", "").replace("\xa0", "")

    match = re.search(r"([\d.]+)\s*([KkMm]?)", text)
    if match:
        value = float(match.group(1))
        suffix = match.group(2).upper()

        if suffix == "K":
            value *= 1000
        elif suffix == "M":
            value *= 1000000

        return int(value)

    return 0


# ==========================================
# FETCH VIA SCRAPERAPI (OPTIMISÉ)
# ==========================================

def fetch_via_scraperapi(target_url, scraper_key):
    params = {
        "api_key": scraper_key,
        "url": target_url,
        "render": "true",
        "country_code": "us",
    }

    try:
        response = session.get(
            SCRAPERAPI_URL,
            params=params,
            timeout=60,
            headers={"Connection": "keep-alive"},
        )

        if response.status_code == 200:
            return response.text, None

        return None, f"ScraperAPI error {response.status_code}"

    except Exception as e:
        return None, str(e)
        # ==========================================
# SCRAPE SHOP PAGE
# ==========================================

def scrape_shop_page(shop_name, scraper_key):
    url = f"https://www.etsy.com/shop/{shop_name}"

    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return None, error

    soup = BeautifulSoup(html, "lxml")
    text = soup.get_text(" ", strip=True)

    total_sales = 0
    total_reviews = 0

    sales_match = re.search(r"([\dKkMm,.]+)\s+sales", text, re.I)
    if sales_match:
        total_sales = parse_number(sales_match.group(1))

    reviews_match = re.search(r"([\dKkMm,.]+)\s+reviews", text, re.I)
    if reviews_match:
        total_reviews = parse_number(reviews_match.group(1))

    return {
        "shop_name": shop_name,
        "total_sales": total_sales,
        "total_reviews": total_reviews,
        "scraped_at": datetime.utcnow().isoformat()
    }, None


# ==========================================
# SCRAPE SINGLE LISTINGS PAGE
# ==========================================

def scrape_listings_page(shop_name, scraper_key, page=1):

    url = f"https://www.etsy.com/shop/{shop_name}?page={page}"

    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return [], error

    soup = BeautifulSoup(html, "lxml")

    listings = []
    seen_ids = set()

    links = soup.find_all("a", href=re.compile(r"/listing/\d+"))

    for link in links:

        href = link.get("href", "")
        match = re.search(r"/listing/(\d+)", href)

        if not match:
            continue

        listing_id = match.group(1)

        if listing_id in seen_ids:
            continue

        seen_ids.add(listing_id)

        title = link.get_text(strip=True)
        title = title[:250] if title else ""

        # Prix détecté dans le texte proche
        price = 0.0
        currency = "USD"

        price_match = re.search(r"([\d.,]+)", title)
        if price_match:
            try:
                price = float(price_match.group(1).replace(",", ""))
            except:
                price = 0.0

        listings.append({
            "listing_id": listing_id,
            "title": title,
            "price": price,
            "currency": currency
        })

    return listings, None


# ==========================================
# SCRAPE ALL LISTINGS (THREADING 🚀)
# ==========================================

def scrape_all_listings(shop_name, scraper_key, max_pages=5):

    all_listings = []

    def worker(page):
        results, err = scrape_listings_page(shop_name, scraper_key, page)
        if results:
            return results
        return []

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(worker, page)
            for page in range(1, max_pages + 1)
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
                all_listings.extend(result)
            except:
                pass

    return all_listings
    # ==========================================
# SCRAPE SINGLE REVIEWS PAGE
# ==========================================

def scrape_reviews_page(shop_name, scraper_key, page=1):

    url = f"https://www.etsy.com/shop/{shop_name}/reviews?page={page}"

    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return [], error

    soup = BeautifulSoup(html, "lxml")

    reviews = []

    review_blocks = soup.find_all("p")

    for block in review_blocks:
        text = block.get_text(strip=True)

        # On filtre les textes trop courts
        if len(text) < 20:
            continue

        reviews.append(text)

    return reviews, None


# ==========================================
# SCRAPE ALL REVIEWS (THREADING 🚀)
# ==========================================

def scrape_all_reviews(shop_name, scraper_key, max_pages=3):

    all_reviews = []

    def worker(page):
        results, err = scrape_reviews_page(shop_name, scraper_key, page)
        if results:
            return results
        return []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(worker, page)
            for page in range(1, max_pages + 1)
        ]

        for future in as_completed(futures):
            try:
                result = future.result()
                all_reviews.extend(result)
            except:
                pass

    return all_reviews


# ==========================================
# TAG EXTRACTION (TITRES + REVIEWS)
# ==========================================

def extract_keywords_from_text(text):
    words = re.findall(r"[A-Za-zÀ-ÿ]{4,}", text.lower())
    return words


def analyze_tags(listings, reviews):

    tag_counter = defaultdict(int)

    # Depuis les titres
    for listing in listings:
        words = extract_keywords_from_text(listing.get("title", ""))
        for word in words:
            tag_counter[word] += 1

    # Depuis les reviews
    for review in reviews:
        words = extract_keywords_from_text(review)
        for word in words:
            tag_counter[word] += 1

    # Top 50 mots
    sorted_tags = sorted(tag_counter.items(), key=lambda x: x[1], reverse=True)

    return dict(sorted_tags[:50])


# ==========================================
# PRICE ANALYSIS
# ==========================================

def analyze_prices(listings):

    prices = [l["price"] for l in listings if l["price"] > 0]

    if not prices:
        return {}

    return {
        "average_price": round(sum(prices) / len(prices), 2),
        "min_price": round(min(prices), 2),
        "max_price": round(max(prices), 2),
        "median_price": round(sorted(prices)[len(prices)//2], 2)
    }


# ==========================================
# FULL SHOP ANALYSIS (VERSION COMPLETE)
# ==========================================

def analyze_full_shop(shop_url, scraper_key,
                      max_listing_pages=5,
                      max_review_pages=3):

    shop_name = extract_shop_name(shop_url)
    if not shop_name:
        return None, "Invalid shop URL"

    # 1️⃣ Shop info
    shop_info, error = scrape_shop_page(shop_name, scraper_key)
    if error:
        return None, error

    # 2️⃣ Listings
    listings = scrape_all_listings(shop_name, scraper_key, max_listing_pages)

    # 3️⃣ Reviews
    reviews = scrape_all_reviews(shop_name, scraper_key, max_review_pages)

    # 4️⃣ Tags analysis
    tags = analyze_tags(listings, reviews)

    # 5️⃣ Price stats
    price_stats = analyze_prices(listings)

    return {
        "shop": shop_info,
        "listings_count": len(listings),
        "reviews_count": len(reviews),
        "price_analysis": price_stats,
        "top_keywords": tags,
        "generated_at": datetime.utcnow().isoformat()
    }, None

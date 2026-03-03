import requests
from bs4 import BeautifulSoup
import re
import json
import time
from datetime import datetime
from collections import defaultdict
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
try:
    from PIL import Image
    PIL_AVAILABLE = True
except ImportError:
    PIL_AVAILABLE = False


ETSY_API_BASE = "https://openapi.etsy.com/v3/application"
SCRAPERAPI_URL = "https://api.scraperapi.com"

_thread_local = threading.local()


def _get_session():
    if not hasattr(_thread_local, "session"):
        _thread_local.session = requests.Session()
        _thread_local.session.headers.update({"Accept-Encoding": "gzip, deflate"})
    return _thread_local.session


def extract_shop_name(url):
    url = url.strip().rstrip("/")
    patterns = [
        r"etsy\.com/(?:[a-z]{2}/)?shop/([A-Za-z0-9_]+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    if not url.startswith("http") and re.match(r"^[A-Za-z0-9_]+$", url):
        return url
    return None


def build_shop_url(shop_name):
    return f"https://www.etsy.com/shop/{shop_name}"


def fetch_via_scraperapi(target_url, scraper_key, max_retries=2, render=False):
    params = {
        "api_key": scraper_key,
        "url": target_url,
        "country_code": "us",
    }
    if render:
        params["render"] = "true"
    last_error = None
    for attempt in range(max_retries):
        try:
            response = _get_session().get(SCRAPERAPI_URL, params=params, timeout=45)
            if response.status_code == 200:
                return response.text, None
            elif response.status_code == 401:
                return None, "Cle ScraperAPI invalide. Verifiez votre cle."
            elif response.status_code == 403:
                return None, "Acces refuse par ScraperAPI. Verifiez votre forfait."
            elif response.status_code == 429:
                return None, "Limite de requetes ScraperAPI atteinte. Reessayez plus tard."
            elif response.status_code in (500, 502, 503, 504):
                last_error = f"Erreur ScraperAPI (code {response.status_code})"
                if attempt < max_retries - 1:
                    time.sleep(2 * (attempt + 1))
                    continue
            else:
                return None, f"Erreur ScraperAPI (code {response.status_code})."
        except requests.Timeout:
            last_error = "Timeout - la page met trop de temps a charger"
            if attempt < max_retries - 1:
                time.sleep(2 * (attempt + 1))
                continue
        except requests.RequestException as e:
            return None, f"Erreur de connexion : {str(e)}"
    return None, f"{last_error}. Reessayez plus tard (apres {max_retries} tentatives)."


def etsy_api_get(endpoint, api_key, params=None):
    headers = {
        "x-api-key": api_key,
        "Accept": "application/json",
    }
    url = f"{ETSY_API_BASE}{endpoint}"
    try:
        response = requests.get(url, headers=headers, params=params, timeout=15)
        if response.status_code == 200:
            return response.json(), None

        error_detail = ""
        try:
            err_body = response.json()
            error_detail = err_body.get("error", err_body.get("message", ""))
        except Exception:
            error_detail = response.text[:200]

        if response.status_code == 401:
            return None, f"Clé API invalide (401). {error_detail}"
        elif response.status_code == 403:
            return None, f"Accès refusé (403). {error_detail}"
        elif response.status_code == 404:
            return None, f"Non trouvé (404). {error_detail}"
        elif response.status_code == 429:
            return None, "Trop de requêtes (429). Réessayez plus tard."
        else:
            return None, f"Erreur API (code {response.status_code}). {error_detail}"
    except requests.RequestException as e:
        return None, f"Erreur de connexion : {str(e)}"


def parse_number(text):
    if not text:
        return 0
    text = text.strip().replace("\u202f", "").replace("\xa0", "")

    suffix_match = re.search(r"([\d,.]+)\s*([KkMm])", text)
    if suffix_match:
        num_str = suffix_match.group(1).replace(",", "")
        suffix = suffix_match.group(2).upper()
        try:
            val = float(num_str)
            if suffix == "K":
                return int(val * 1_000)
            elif suffix == "M":
                return int(val * 1_000_000)
        except ValueError:
            pass

    nums = re.findall(r"[\d,.\s]+", text)
    if nums:
        num_str = nums[0].strip().replace(",", "").replace(" ", "")
        if num_str.count(".") > 1:
            num_str = num_str.replace(".", "")
        try:
            return int(float(num_str))
        except ValueError:
            return 0
    return 0


def scrape_shop_page(shop_name, scraper_key):
    url = build_shop_url(shop_name)
    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return None, error
    if not html or len(html) < 1000:
        return None, "La page n'a pas pu être chargée correctement."

    soup = BeautifulSoup(html, "lxml")

    data = {
        "shop_name": shop_name,
        "shop_url": url,
        "total_sales": 0,
        "total_reviews": 0,
        "star_rating": None,
        "shop_location": None,
        "member_since": None,
        "admirers": 0,
        "icon_url": "",
        "title": "",
    }

    page_text = soup.get_text(" ", strip=True)

    sales_patterns = [
        r"([\d,.]+[KkMm]?)\s*(?:sales|ventes)",
        r"([\d,]+)\s+Sales",
    ]
    for pattern in sales_patterns:
        sales_match = re.search(pattern, page_text, re.IGNORECASE)
        if sales_match:
            val = parse_number(sales_match.group(1))
            if val > 0:
                data["total_sales"] = val
                break

    reviews_patterns = [
        r"\(([\d,.]+[KkMm]?)\)",
        r"([\d,.]+[KkMm]?)\s*(?:reviews|avis)",
        r"([\d,]+)\s+Reviews",
    ]
    for pattern in reviews_patterns:
        reviews_match = re.search(pattern, page_text, re.IGNORECASE)
        if reviews_match:
            val = parse_number(reviews_match.group(1))
            if val > 0:
                data["total_reviews"] = val
                break

    admirers_patterns = [
        r"([\d,.]+[KkMm]?)\s*(?:admirers|admirateurs)",
        r"([\d,]+)\s+Admirers",
    ]
    for pattern in admirers_patterns:
        admirers_match = re.search(pattern, page_text, re.IGNORECASE)
        if admirers_match:
            val = parse_number(admirers_match.group(1))
            if val > 0:
                data["admirers"] = val
                break

    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for script in scripts:
        try:
            json_data = json.loads(script.string)
            if isinstance(json_data, dict):
                if json_data.get("@type") in ["Store", "Organization", "LocalBusiness"]:
                    if "aggregateRating" in json_data:
                        agg = json_data["aggregateRating"]
                        if agg.get("ratingValue"):
                            data["star_rating"] = str(round(float(agg["ratingValue"]), 1))
                        if agg.get("reviewCount") and not data["total_reviews"]:
                            data["total_reviews"] = int(agg["reviewCount"])
                    if "address" in json_data:
                        addr = json_data["address"]
                        if isinstance(addr, dict):
                            parts = []
                            if addr.get("addressLocality"):
                                parts.append(addr["addressLocality"])
                            if addr.get("addressCountry"):
                                parts.append(addr["addressCountry"])
                            if parts:
                                data["shop_location"] = ", ".join(parts)
                    if json_data.get("name"):
                        data["title"] = json_data["name"]
                    if json_data.get("image"):
                        img = json_data["image"]
                        if isinstance(img, list) and img:
                            data["icon_url"] = img[0]
                        elif isinstance(img, str):
                            data["icon_url"] = img
        except (json.JSONDecodeError, TypeError, ValueError):
            continue

    if not data["star_rating"]:
        for span in soup.find_all("span"):
            text = span.get_text(strip=True)
            if re.match(r"^\d\.\d$", text):
                val = float(text)
                if 1.0 <= val <= 5.0:
                    data["star_rating"] = text
                    break

    member_match = re.search(r"(?:on etsy since|sur etsy depuis|member since)\s*(\d{4})", page_text, re.IGNORECASE)
    if member_match:
        data["member_since"] = member_match.group(1)

    if not data["member_since"]:
        years_match = re.search(r"(\d+)\s*(?:years?\s+on\s+etsy|ans?\s+sur\s+etsy)", page_text, re.IGNORECASE)
        if years_match:
            years = int(years_match.group(1))
            data["member_since"] = str(datetime.now().year - years)

    return data, None


def scrape_listings_page(shop_name, scraper_key, page_num=1):
    url = f"https://www.etsy.com/shop/{shop_name}?page={page_num}#items"
    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return [], error

    soup = BeautifulSoup(html, "lxml")
    listings = []
    seen_ids = set()

    listing_links = soup.find_all("a", href=re.compile(r"/listing/\d+"))
    for link in listing_links:
        href = link.get("href", "")
        id_match = re.search(r"/listing/(\d+)", href)
        if not id_match:
            continue
        listing_id = id_match.group(1)
        if listing_id in seen_ids:
            continue
        seen_ids.add(listing_id)

        card = link
        parent = link.parent
        if parent:
            grandparent = parent.parent
            if grandparent:
                card = grandparent

        title = ""
        for el in [link] + link.find_all(["h2", "h3", "span", "div"]):
            t = el.get_text(strip=True)
            if len(t) > 10 and not re.match(r"^[\d€$£,.]+$", t):
                title = t
                break
        if not title:
            img = link.find("img")
            if img:
                title = img.get("alt", "")

        price = 0.0
        currency = "USD"
        for span in card.find_all("span"):
            text = span.get_text(strip=True)
            price_match = re.match(r"^[€$£]?\s*([\d,.]+)\s*[€$£]?$", text)
            if price_match:
                price_str = price_match.group(1).replace(",", ".")
                if price_str.count(".") > 1:
                    price_str = price_str.replace(".", "", price_str.count(".") - 1)
                try:
                    val = float(price_str)
                    if 0.1 < val < 100000:
                        price = val
                        if "€" in text:
                            currency = "EUR"
                        elif "£" in text:
                            currency = "GBP"
                        break
                except ValueError:
                    continue

        listings.append({
            "listing_id": listing_id,
            "title": title[:200],
            "price": price,
            "currency": currency,
            "tags": [],
        })

    return listings, None


def scrape_listing_details(listing_id, scraper_key):
    url = f"https://www.etsy.com/listing/{listing_id}"
    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return {}

    soup = BeautifulSoup(html, "lxml")
    details = {"listing_id": listing_id, "tags": [], "materials": [], "views": 0, "favorites": 0}

    scripts = soup.find_all("script", {"type": "application/ld+json"})
    for script in scripts:
        try:
            json_data = json.loads(script.string)
            if isinstance(json_data, dict) and json_data.get("@type") == "Product":
                if json_data.get("material"):
                    mat = json_data["material"]
                    if isinstance(mat, list):
                        details["materials"] = mat
                    elif isinstance(mat, str):
                        details["materials"] = [m.strip() for m in mat.split(",")]
        except (json.JSONDecodeError, TypeError):
            continue

    tag_section = soup.find("ul", id=re.compile(r"tag", re.IGNORECASE))
    if tag_section:
        for li in tag_section.find_all("li"):
            tag_text = li.get_text(strip=True)
            if tag_text:
                details["tags"].append(tag_text.lower())

    if not details["tags"]:
        for a_tag in soup.find_all("a", href=re.compile(r"/search\?q=|/c/|search_query")):
            text = a_tag.get_text(strip=True)
            if text and len(text) < 50:
                details["tags"].append(text.lower())

    meta_keywords = soup.find("meta", {"name": "keywords"})
    if meta_keywords and not details["tags"]:
        kw = meta_keywords.get("content", "")
        if kw:
            details["tags"] = [t.strip().lower() for t in kw.split(",") if t.strip()]

    page_text = soup.get_text(" ", strip=True)
    fav_match = re.search(r"([\d,]+)\s*(?:favorites|favoris)", page_text, re.IGNORECASE)
    if fav_match:
        details["favorites"] = parse_number(fav_match.group(1))

    return details


def scrape_reviews_page(shop_name, scraper_key, page_num=1):
    url = f"https://www.etsy.com/shop/{shop_name}/reviews?page={page_num}"
    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return [], error

    soup = BeautifulSoup(html, "lxml")
    reviews = []

    review_containers = soup.find_all("div", {"data-review-id": True})
    if not review_containers:
        review_containers = soup.find_all("div", class_=re.compile(r"review", re.IGNORECASE))

    for card in review_containers:
        date_text = None
        rating = None
        review_text = ""

        date_el = card.find(string=re.compile(
            r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}", re.IGNORECASE
        ))
        if date_el:
            date_text = date_el.strip()

        stars_el = card.find(attrs={"aria-label": re.compile(r"\d.*star", re.IGNORECASE)})
        if stars_el:
            star_match = re.search(r"(\d)", stars_el.get("aria-label", ""))
            if star_match:
                rating = int(star_match.group(1))
        if not rating:
            rating_input = card.find("input", {"name": "rating"})
            if rating_input:
                try:
                    rating = int(rating_input.get("value", 0))
                except (ValueError, TypeError):
                    pass

        text_el = card.find("p")
        if text_el:
            review_text = text_el.get_text(strip=True)
        if not review_text:
            for div in card.find_all("div"):
                t = div.get_text(strip=True)
                if len(t) > 20 and not re.match(r"^\d", t):
                    review_text = t
                    break

        if date_text or review_text:
            review_date = parse_review_date(date_text)
            reviews.append({
                "date": review_date,
                "date_text": date_text or "N/A",
                "rating": rating,
                "text": review_text[:300],
            })

    return reviews, None


def parse_review_date(date_text):
    if not date_text:
        return None
    months = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
    }
    match = re.search(r"(\w+)\s+(\d{1,2}),?\s+(\d{4})", date_text, re.IGNORECASE)
    if match:
        month_str = match.group(1)[:3].lower()
        day = int(match.group(2))
        year = int(match.group(3))
        month = months.get(month_str)
        if month:
            try:
                return datetime(year, month, day)
            except ValueError:
                pass
    return None


def extract_tags(listings):
    tag_counts = defaultdict(int)
    for listing in listings:
        for tag in listing.get("tags", []):
            tag_lower = tag.lower().strip()
            if tag_lower and len(tag_lower) >= 2:
                tag_counts[tag_lower] += 1
        title = listing.get("title", "")
        words = re.findall(r"[A-Za-zÀ-ÿ]{4,}", title.lower())
        stop_words = {
            "the", "and", "for", "with", "this", "that", "from", "your",
            "are", "was", "were", "been", "have", "has", "had", "will",
            "can", "not", "but", "all", "her", "his", "our", "they",
            "you", "she", "him", "its", "who", "how", "each", "which",
            "their", "them", "then", "than", "into", "over", "such",
            "gift", "set", "new", "one", "two", "des", "les", "une",
            "pour", "par", "avec", "dans", "sur", "est", "pas", "plus",
        }
        for word in words:
            if word not in stop_words:
                tag_counts[word] += 1
    return dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:50])


def estimate_monthly_sales(total_sales, member_since=None):
    if not total_sales:
        return {}

    months_active = 24
    if member_since:
        try:
            start_year = int(member_since)
            now = datetime.now()
            months_active = max(1, (now.year - start_year) * 12 + now.month)
        except (ValueError, TypeError):
            pass

    avg_monthly = total_sales / months_active
    monthly_data = {}
    now = datetime.now()
    for i in range(min(12, months_active)):
        month = now.month - i
        year = now.year
        while month <= 0:
            month += 12
            year -= 1
        month_key = f"{year}-{month:02d}"
        monthly_data[month_key] = round(avg_monthly)

    return dict(reversed(list(monthly_data.items())))


def estimate_revenue(total_sales, listings):
    if not listings or not total_sales:
        return 0.0
    prices = [l["price"] for l in listings if l["price"] > 0]
    if not prices:
        return 0.0
    avg_price = sum(prices) / len(prices)
    return round(avg_price * total_sales, 2)


def reviews_by_month(reviews):
    monthly = defaultdict(int)
    for review in reviews:
        if review.get("date"):
            key = review["date"].strftime("%Y-%m")
            monthly[key] += 1
    return dict(sorted(monthly.items()))


def reviews_by_rating(reviews):
    ratings = defaultdict(int)
    for review in reviews:
        r = review.get("rating")
        if r:
            ratings[str(r)] += 1
    return dict(sorted(ratings.items()))


def scrape_search_page(query, scraper_key, sort="most_relevant", page=1):
    params_str = f"q={requests.utils.quote(query)}&order={sort}&explicit=1&page={page}"
    url = f"https://www.etsy.com/search?{params_str}"
    html, error = fetch_via_scraperapi(url, scraper_key)
    if error:
        return [], 0, error

    soup = BeautifulSoup(html, "lxml")
    page_text = soup.get_text(" ", strip=True)

    total_results = 0
    results_match = re.search(r"([\d,.]+)\s*results", page_text, re.IGNORECASE)
    if results_match:
        total_results = parse_number(results_match.group(1))

    listings = []
    seen_ids = set()

    listing_links = soup.find_all("a", href=re.compile(r"/listing/\d+"))
    for link in listing_links:
        href = link.get("href", "")
        id_match = re.search(r"/listing/(\d+)", href)
        if not id_match:
            continue
        listing_id = id_match.group(1)
        if listing_id in seen_ids:
            continue
        seen_ids.add(listing_id)

        card = link
        parent = link.parent
        if parent:
            grandparent = parent.parent
            if grandparent:
                card = grandparent

        title = ""
        img = link.find("img")
        if img:
            title = img.get("alt", "")
        if not title:
            for el in link.find_all(["h2", "h3", "span", "div"]):
                t = el.get_text(strip=True)
                if len(t) > 10 and not re.match(r"^[\d€$£,.]+$", t):
                    title = t
                    break

        price = 0.0
        currency = "USD"
        for span in card.find_all("span"):
            text = span.get_text(strip=True)
            price_match = re.match(r"^[€$£]?\s*([\d,.]+)\s*[€$£]?$", text)
            if price_match:
                price_str = price_match.group(1).replace(",", ".")
                if price_str.count(".") > 1:
                    price_str = price_str.replace(".", "", price_str.count(".") - 1)
                try:
                    val = float(price_str)
                    if 0.1 < val < 100000:
                        price = val
                        if "€" in text:
                            currency = "EUR"
                        elif "£" in text:
                            currency = "GBP"
                        break
                except ValueError:
                    continue

        rating = None
        for span in card.find_all("span"):
            text = span.get_text(strip=True)
            if re.match(r"^\d\.\d$", text):
                val = float(text)
                if 1.0 <= val <= 5.0:
                    rating = val
                    break

        shop_name = ""
        badge_words = {"bestseller", "freeshipping", "etsypick", "startseller", "ad", "ads", "sponsored"}
        for span in card.find_all("span"):
            text = span.get_text(strip=True)
            if (re.match(r"^[A-Za-z][A-Za-z0-9_]{2,30}$", text)
                    and text.lower() not in badge_words
                    and text != title[:30]):
                shop_name = text
                break

        image_url = ""
        if img:
            image_url = img.get("src", "") or img.get("data-src", "")

        listings.append({
            "listing_id": listing_id,
            "title": title[:200],
            "price": price,
            "currency": currency,
            "rating": rating,
            "shop_name": shop_name,
            "image_url": image_url,
            "position": len(listings) + 1,
        })

    return listings, total_results, None


DIGITAL_KEYWORDS = {
    "digital download", "digital", "printable", "instant download",
    "svg", "png", "pdf", "eps", "dxf", "clipart", "clip art",
    "template", "editable", "canva", "cricut", "silhouette",
    "sublimation", "dtf", "dtg", "print on demand",
    "digital planner", "digital paper", "digital art",
    "downloadable", "e-book", "ebook", "digital file",
    "digital print", "wall art printable", "digital pattern",
    "digital sticker", "digital invitation",
}


def is_digital_product(title):
    title_lower = title.lower()
    for kw in DIGITAL_KEYWORDS:
        if kw in title_lower:
            return True
    return False


def analyze_category(query, scraper_key, max_pages=3, sort="most_relevant", progress_callback=None, exclude_digital=False, max_results=0):
    all_listings = []
    total_results = 0

    if max_pages <= 2:
        for page_num in range(1, max_pages + 1):
            if progress_callback:
                progress_callback(page_num / (max_pages + 1),
                                f"Recherche page {page_num}/{max_pages}...",
                                all_listings)
            page_listings, page_total, error = scrape_search_page(query, scraper_key, sort=sort, page=page_num)
            if error:
                if page_num == 1:
                    return None, error
                break
            if not page_listings:
                break
            if page_total > 0:
                total_results = page_total
            if exclude_digital:
                page_listings = [l for l in page_listings if not is_digital_product(l.get("title", ""))]
            all_listings.extend(page_listings)
            if max_results > 0 and len(all_listings) >= max_results:
                all_listings = all_listings[:max_results]
                break
    else:
        if progress_callback:
            progress_callback(0.05, f"Recherche page 1/{max_pages}...")
        first_listings, first_total, first_error = scrape_search_page(query, scraper_key, sort=sort, page=1)
        if first_error:
            return None, first_error
        if first_total > 0:
            total_results = first_total
        if first_listings:
            if exclude_digital:
                first_listings = [l for l in first_listings if not is_digital_product(l.get("title", ""))]
            all_listings.extend(first_listings)

        remaining_pages = list(range(2, max_pages + 1))
        if remaining_pages and (max_results <= 0 or len(all_listings) < max_results):
            def fetch_cat_page(pn):
                return pn, scrape_search_page(query, scraper_key, sort=sort, page=pn)

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {executor.submit(fetch_cat_page, pn): pn for pn in remaining_pages}
                page_results = {}
                for future in as_completed(futures):
                    pn, (plist, ptotal, perr) = future.result()
                    page_results[pn] = (plist, ptotal, perr)

                for pn in sorted(page_results.keys()):
                    plist, ptotal, perr = page_results[pn]
                    if progress_callback:
                        progress_callback(pn / (max_pages + 1),
                                        f"Page {pn}/{max_pages} analysee...",
                                        all_listings)
                    if perr or not plist:
                        break
                    if ptotal > 0:
                        total_results = ptotal
                    if exclude_digital:
                        plist = [l for l in plist if not is_digital_product(l.get("title", ""))]
                    all_listings.extend(plist)
                    if max_results > 0 and len(all_listings) >= max_results:
                        all_listings = all_listings[:max_results]
                        break

    price_ranges = defaultdict(int)
    for l in all_listings:
        p = l["price"]
        if p <= 0:
            continue
        elif p < 5:
            price_ranges["0-5"] += 1
        elif p < 10:
            price_ranges["5-10"] += 1
        elif p < 20:
            price_ranges["10-20"] += 1
        elif p < 50:
            price_ranges["20-50"] += 1
        elif p < 100:
            price_ranges["50-100"] += 1
        else:
            price_ranges["100+"] += 1

    tag_counts = defaultdict(int)
    for l in all_listings:
        words = re.findall(r"[A-Za-zÀ-ÿ]{4,}", l["title"].lower())
        stop_words = {
            "the", "and", "for", "with", "this", "that", "from", "your",
            "are", "was", "were", "been", "have", "has", "had", "will",
            "can", "not", "but", "all", "her", "his", "our", "they",
            "you", "she", "him", "its", "who", "how", "each", "which",
            "their", "them", "then", "than", "into", "over", "such",
            "gift", "free", "shipping", "custom", "personalized",
        }
        for word in words:
            if word not in stop_words:
                tag_counts[word] += 1
    top_keywords = dict(sorted(tag_counts.items(), key=lambda x: x[1], reverse=True)[:30])

    shop_counts = defaultdict(int)
    for l in all_listings:
        if l.get("shop_name"):
            shop_counts[l["shop_name"]] += 1
    top_shops = dict(sorted(shop_counts.items(), key=lambda x: x[1], reverse=True)[:20])

    prices = [l["price"] for l in all_listings if l["price"] > 0]
    price_stats = {}
    if prices:
        price_stats = {
            "avg": round(sum(prices) / len(prices), 2),
            "median": round(sorted(prices)[len(prices) // 2], 2),
            "min": round(min(prices), 2),
            "max": round(max(prices), 2),
        }

    if progress_callback:
        progress_callback(1.0, "Analyse terminee !", all_listings)

    return {
        "query": query,
        "sort": sort,
        "total_results": total_results,
        "listings": all_listings,
        "listings_count": len(all_listings),
        "price_ranges": dict(price_ranges),
        "price_stats": price_stats,
        "top_keywords": top_keywords,
        "top_shops": top_shops,
    }, None


COMMON_STOPWORDS = {
    "the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for",
    "of", "with", "by", "from", "is", "it", "this", "that", "was", "are",
    "be", "has", "had", "have", "will", "can", "do", "does", "not", "no",
    "all", "any", "if", "so", "up", "out", "as", "its", "my", "we", "he",
    "she", "you", "your", "our", "his", "her", "us", "them", "their",
    "me", "i", "am", "been", "being", "were", "what", "when", "where",
    "who", "how", "which", "each", "every", "more", "most", "very",
    "just", "about", "into", "over", "after", "than", "also", "only",
    "new", "one", "two", "free", "off", "per", "see", "now", "get",
    "shipping", "sold", "shop", "sale", "buy", "star", "seller",
    "bestseller", "pick", "ad", "ads", "sponsored",
}


def _extract_keywords_from_html(html):
    if not html or len(html) < 500:
        return set(), defaultdict(int)

    soup = BeautifulSoup(html, "lxml")
    keywords = set()
    bigram_freq = defaultdict(int)

    for a_tag in soup.find_all("a", href=True):
        href = a_tag.get("href", "")
        if "/search?" in href and "q=" in href:
            q_match = re.search(r"[?&]q=([^&]+)", href)
            if q_match:
                kw = requests.utils.unquote(q_match.group(1)).replace("+", " ").strip().lower()
                if kw and len(kw) > 2 and len(kw) < 60 and len(kw.split()) <= 5:
                    keywords.add(kw)
        if "/c/" in href or "/market/" in href:
            text = a_tag.get_text(strip=True).lower()
            clean = re.sub(r"[^a-z0-9\s]", "", text).strip()
            if clean and 2 < len(clean) < 50 and len(clean.split()) <= 4:
                if clean not in COMMON_STOPWORDS:
                    keywords.add(clean)

    listing_links = soup.find_all("a", href=re.compile(r"/listing/\d+"))
    for link in listing_links:
        img = link.find("img")
        title = ""
        if img:
            title = img.get("alt", "")
        if not title:
            for el in link.find_all(["h2", "h3", "span", "div"]):
                t = el.get_text(strip=True)
                if len(t) > 10:
                    title = t
                    break
        if title:
            words = re.findall(r"[a-zA-Z]+", title.lower())
            words = [w for w in words if w not in COMMON_STOPWORDS and len(w) > 2]
            for i in range(len(words) - 1):
                bigram = f"{words[i]} {words[i+1]}"
                bigram_freq[bigram] += 1

    return keywords, bigram_freq


def discover_trending_keywords(scraper_key, progress_callback=None):
    if progress_callback:
        progress_callback(0.02, "Scan des pages Etsy en parallele...")

    pages_to_scan = [
        "https://www.etsy.com/",
        "https://www.etsy.com/trending",
        "https://www.etsy.com/featured",
        "https://www.etsy.com/search?q=&explicit=1&order=most_relevant",
    ]

    trending_keywords = set()
    all_bigram_freq = defaultdict(int)
    pages_succeeded = 0
    last_error = None

    def fetch_page(url):
        return url, fetch_via_scraperapi(url, scraper_key)

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(fetch_page, url) for url in pages_to_scan]
        for i, future in enumerate(as_completed(futures)):
            url, (html, error) = future.result()
            if progress_callback:
                progress_callback(0.02 + 0.07 * ((i + 1) / len(pages_to_scan)),
                                f"Page {i+1}/{len(pages_to_scan)} analysee...")
            if error:
                last_error = error
                continue
            pages_succeeded += 1
            kws, bigrams = _extract_keywords_from_html(html)
            trending_keywords.update(kws)
            for b, c in bigrams.items():
                all_bigram_freq[b] += c

    top_bigrams = sorted(all_bigram_freq.items(), key=lambda x: x[1], reverse=True)
    for bigram, count in top_bigrams[:25]:
        if count >= 2:
            trending_keywords.add(bigram)

    if progress_callback:
        progress_callback(0.10, f"{len(trending_keywords)} mots-cles tendance decouverts !")

    if pages_succeeded == 0:
        return [], last_error or "Impossible d'acceder a Etsy."

    if not trending_keywords:
        return [], "Aucun mot-cle tendance trouve sur Etsy."

    final = list(trending_keywords)
    final.sort(key=lambda x: len(x))
    return final[:60], None


def analyze_keyword_quick(keyword, scraper_key):
    listings, total_results, error = scrape_search_page(keyword, scraper_key, sort="most_relevant", page=1)
    if error:
        return {"keyword": keyword, "volume": 0, "competition": 0, "sellers": 0, "score": 0, "error": error}, []

    unique_shops = set()
    for l in listings:
        if l.get("shop_name"):
            unique_shops.add(l["shop_name"].lower())

    volume = total_results if total_results > 0 else len(listings) * 48
    competition = len(unique_shops)

    return {
        "keyword": keyword,
        "volume": volume,
        "competition": competition,
        "sellers": len(unique_shops),
        "avg_price": round(sum(l["price"] for l in listings if l["price"] > 0) / max(1, len([l for l in listings if l["price"] > 0])), 2) if listings else 0,
        "listings_found": len(listings),
    }, listings


def analyze_keywords_auto(scraper_key, progress_callback=None):
    if progress_callback:
        progress_callback(0.01, "Decouverte automatique des mots-cles populaires Etsy...")

    trending, disc_error = discover_trending_keywords(scraper_key, progress_callback)

    if disc_error or not trending:
        return None, disc_error or "Impossible de decouvrir les mots-cles tendance depuis Etsy."

    if progress_callback:
        progress_callback(0.12, f"{len(trending)} mots-cles a analyser en parallele...")

    results = []
    batch_size = 5
    total = len(trending)
    done_count = 0

    for batch_start in range(0, total, batch_size):
        batch = trending[batch_start:batch_start + batch_size]

        def analyze_one(kw):
            return analyze_keyword_quick(kw, scraper_key)

        with ThreadPoolExecutor(max_workers=batch_size) as executor:
            futures = {executor.submit(analyze_one, kw): kw for kw in batch}
            for future in as_completed(futures):
                data, _ = future.result()
                done_count += 1
                if progress_callback:
                    pct = 0.12 + (0.86 * done_count / total)
                    progress_callback(pct, f"Analyse: {futures[future]} ({done_count}/{total})")
                if data.get("volume", 0) > 0 or data.get("listings_found", 0) > 0:
                    results.append(data)

    if not results:
        return None, "Aucun resultat exploitable trouve."

    max_volume = max(r["volume"] for r in results) if results else 1
    max_comp = max(r["competition"] for r in results) if results else 1

    for r in results:
        vol_norm = (r["volume"] / max_volume * 100) if max_volume > 0 else 0
        comp_norm = (r["competition"] / max_comp * 100) if max_comp > 0 else 100
        if comp_norm < 1:
            comp_norm = 1
        r["score"] = round(vol_norm / comp_norm * 10, 1)
        r["volume_normalized"] = round(vol_norm, 1)
        r["competition_normalized"] = round(comp_norm, 1)

    max_score = max(r["score"] for r in results) if results else 1
    if max_score > 0:
        for r in results:
            r["score"] = round(r["score"] / max_score * 100, 1)

    results.sort(key=lambda x: x["score"], reverse=True)

    if progress_callback:
        progress_callback(1.0, "Analyse terminee !")

    return {
        "base_keyword": "Decouverte automatique",
        "total_keywords": len(results),
        "discovery_method": "fully_autonomous",
        "keywords": results,
    }, None


def download_image(url, timeout=10):
    if not url or not PIL_AVAILABLE:
        return None
    try:
        if url.startswith("//"):
            url = "https:" + url
        resp = requests.get(url, timeout=timeout, headers={
            "User-Agent": "Mozilla/5.0"
        })
        if resp.status_code == 200 and len(resp.content) > 500:
            img = Image.open(BytesIO(resp.content)).convert("RGB")
            return img
    except Exception:
        pass
    return None


def compute_phash(img, hash_size=16):
    if img is None:
        return None
    try:
        img_resized = img.resize((hash_size, hash_size), Image.LANCZOS)
        pixels = list(img_resized.getdata())
        avg = sum(sum(p) for p in pixels) / (len(pixels) * 3)
        bits = []
        for p in pixels:
            bits.append(1 if (p[0] + p[1] + p[2]) / 3 > avg else 0)
        return bits
    except Exception:
        return None


def compute_color_histogram(img, bins=8):
    if img is None:
        return None
    try:
        img_small = img.resize((64, 64), Image.LANCZOS)
        pixels = list(img_small.getdata())
        hist = [0] * (bins * 3)
        for r, g, b in pixels:
            hist[min(r * bins // 256, bins - 1)] += 1
            hist[bins + min(g * bins // 256, bins - 1)] += 1
            hist[2 * bins + min(b * bins // 256, bins - 1)] += 1
        total = len(pixels)
        hist = [h / total for h in hist]
        return hist
    except Exception:
        return None


def image_similarity(img1, img2):
    if img1 is None or img2 is None or not PIL_AVAILABLE:
        return 0.0

    hash1 = compute_phash(img1)
    hash2 = compute_phash(img2)
    hash_sim = 0.0
    if hash1 and hash2 and len(hash1) == len(hash2):
        matching = sum(1 for a, b in zip(hash1, hash2) if a == b)
        hash_sim = matching / len(hash1)

    hist1 = compute_color_histogram(img1)
    hist2 = compute_color_histogram(img2)
    hist_sim = 0.0
    if hist1 and hist2:
        import math
        dot = sum(a * b for a, b in zip(hist1, hist2))
        mag1 = math.sqrt(sum(a * a for a in hist1))
        mag2 = math.sqrt(sum(b * b for b in hist2))
        if mag1 > 0 and mag2 > 0:
            hist_sim = dot / (mag1 * mag2)

    similarity = (hash_sim * 0.6) + (hist_sim * 0.4)
    return round(similarity * 100, 1)


def search_aliexpress(title, scraper_key, etsy_image_url=None):
    keywords = re.findall(r"[A-Za-z0-9]+", title)
    stop = {"the", "and", "for", "with", "this", "that", "from", "your", "are",
            "was", "gift", "custom", "personalized", "handmade", "unique", "cute",
            "best", "new", "hot", "sale", "free", "shipping", "etsy"}
    filtered = [w for w in keywords if w.lower() not in stop and len(w) > 2][:8]
    query = " ".join(filtered)
    if not query:
        return None, "Pas de mots-cles exploitables"

    search_url = f"https://www.aliexpress.com/wholesale?SearchText={requests.utils.quote(query)}"
    aliexpress_link = search_url

    try:
        resp = requests.get(SCRAPERAPI_URL, params={
            "api_key": scraper_key,
            "url": search_url,
            "render": "true",
        }, timeout=60)
        if resp.status_code != 200:
            return {"search_url": aliexpress_link, "query": query, "results": []}, None
    except Exception:
        return {"search_url": aliexpress_link, "query": query, "results": []}, None

    soup = BeautifulSoup(resp.text, "lxml")
    results = []

    cards = soup.find_all("a", href=re.compile(r"aliexpress\.com/item/|/item/\d+"))
    if not cards:
        cards = soup.find_all("div", class_=re.compile(r"card|product|item", re.IGNORECASE))

    seen = set()
    for card in cards[:10]:
        link_el = card if card.name == "a" else card.find("a", href=True)
        href = ""
        if link_el:
            href = link_el.get("href", "")
            if href.startswith("//"):
                href = "https:" + href

        if href in seen:
            continue
        if href:
            seen.add(href)

        ali_title = ""
        img_url = ""

        img = card.find("img")
        if img:
            ali_title = img.get("alt", "")
            img_url = img.get("src", "") or img.get("data-src", "")
            if img_url.startswith("//"):
                img_url = "https:" + img_url

        if not ali_title:
            for el in card.find_all(["h1", "h2", "h3", "span", "div"]):
                t = el.get_text(strip=True)
                if len(t) > 10 and not re.match(r"^[\d€$,.]+$", t):
                    ali_title = t[:200]
                    break

        ali_price = 0.0
        for el in card.find_all(["span", "div"]):
            txt = el.get_text(strip=True)
            pm = re.search(r"[\$€]?\s*([\d]+[.,]\d{2})", txt)
            if pm:
                try:
                    ali_price = float(pm.group(1).replace(",", "."))
                    if 0.01 < ali_price < 100000:
                        break
                except ValueError:
                    continue

        if ali_title or href:
            results.append({
                "title": ali_title,
                "price": ali_price,
                "image_url": img_url,
                "url": href,
                "similarity": 0.0,
            })

    etsy_img = None
    if etsy_image_url and PIL_AVAILABLE:
        etsy_img = download_image(etsy_image_url)

    if etsy_img and results:
        for r in results:
            if r.get("image_url"):
                ali_img = download_image(r["image_url"])
                if ali_img:
                    r["similarity"] = image_similarity(etsy_img, ali_img)

        results.sort(key=lambda x: x["similarity"], reverse=True)

    best_sim = max((r["similarity"] for r in results), default=0.0) if results else 0.0

    return {
        "search_url": aliexpress_link,
        "query": query,
        "results": results[:5],
        "best_similarity": best_sim,
    }, None


def scrape_full_shop(url, scraper_key, progress_callback=None, max_listing_pages=3, max_review_pages=5, etsy_api_key=None):
    shop_name = extract_shop_name(url)
    if not shop_name:
        return None, "Impossible d'extraire le nom de la boutique depuis l'URL."

    if progress_callback:
        progress_callback(0.05, "Recuperation des infos de la boutique...")

    shop_info, error = scrape_shop_page(shop_name, scraper_key)
    if error:
        return None, error
    if not shop_info:
        return None, "Impossible d'acceder a la boutique."

    if progress_callback:
        progress_callback(0.12, "Recuperation des produits et avis en parallele...")

    all_listings = []
    all_reviews = []

    def fetch_listing_page(pn):
        return pn, scrape_listings_page(shop_name, scraper_key, pn)

    def fetch_review_page(pn):
        return pn, scrape_reviews_page(shop_name, scraper_key, pn)

    with ThreadPoolExecutor(max_workers=4) as executor:
        listing_futures = [executor.submit(fetch_listing_page, pn) for pn in range(1, max_listing_pages + 1)]
        review_futures = [executor.submit(fetch_review_page, pn) for pn in range(1, max_review_pages + 1)]

        listing_results = {}
        for future in as_completed(listing_futures):
            pn, (plist, perr) = future.result()
            listing_results[pn] = (plist, perr)

        for pn in sorted(listing_results.keys()):
            plist, perr = listing_results[pn]
            if perr or not plist:
                break
            all_listings.extend(plist)

        if progress_callback:
            progress_callback(0.45, f"{len(all_listings)} produits trouves, attente avis...")

        review_results = {}
        for future in as_completed(review_futures):
            pn, (prev, perr) = future.result()
            review_results[pn] = (prev, perr)

        for pn in sorted(review_results.keys()):
            prev, perr = review_results[pn]
            if perr or not prev:
                break
            all_reviews.extend(prev)

    if progress_callback:
        progress_callback(0.60, "Recuperation des tags des produits populaires...")

    top_listings = all_listings[:5]
    if top_listings:
        def fetch_tags(idx_listing):
            idx, listing = idx_listing
            return idx, scrape_listing_details(listing["listing_id"], scraper_key)

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(fetch_tags, (i, l)) for i, l in enumerate(top_listings)]
            for future in as_completed(futures):
                idx, details = future.result()
                if details.get("tags"):
                    all_listings[idx]["tags"] = details["tags"]

    if progress_callback:
        progress_callback(0.85, "Analyse des donnees...")

    tags = extract_tags(all_listings)
    monthly_sales = estimate_monthly_sales(shop_info["total_sales"], shop_info.get("member_since"))
    revenue = estimate_revenue(shop_info["total_sales"], all_listings)
    review_months = reviews_by_month(all_reviews)
    review_ratings = reviews_by_rating(all_reviews)

    result = {
        "shop_info": shop_info,
        "listings": all_listings,
        "listings_count": len(all_listings),
        "reviews": all_reviews,
        "reviews_count": len(all_reviews),
        "tags": tags,
        "monthly_sales_estimate": monthly_sales,
        "estimated_revenue": revenue,
        "reviews_by_month": review_months,
        "reviews_by_rating": review_ratings,
    }

    if progress_callback:
        progress_callback(1.0, "Termine !")

    return result, None

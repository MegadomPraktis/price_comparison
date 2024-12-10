import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

PRAKTIS_SEARCH_URL = "https://praktis.bg/catalogsearch/result/?q={}"
PRAKTIKER_SEARCH_URL = "https://praktiker.bg/search/{}"

# Initialize session and headers
session = requests.Session()
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
]
session.headers.update({"Accept-Language": "en-US,en;q=0.9"})


def get_soup(url):
    for attempt in range(3):  # Retry up to 3 times
        try:
            session.headers.update({"User-Agent": random.choice(USER_AGENTS)})
            response = session.get(url, timeout=10)
            response.raise_for_status()
            return BeautifulSoup(response.content, "html.parser")
        except requests.RequestException as e:
            print(f"Attempt {attempt + 1}: Failed to fetch {url}: {e}")
            time.sleep(2 ** attempt + random.uniform(0.5, 1.5))  # Exponential backoff
    return None


def fetch_product_data_praktis(code):
    url = PRAKTIS_SEARCH_URL.format(code)
    soup = get_soup(url)
    if not soup:
        return {"code": code, "name": "N/A", "regular_price": "N/A", "promo_price": "N/A"}

    name = soup.select_one("p.product-name.h4")
    regular_price = soup.select_one("span.price.striked, div.old-price span.price") or soup.select_one("span.price")
    promo_price = soup.select_one("div.special-price span.price")

    return {
        "code": code,
        "name": name.text.strip() if name else "N/A",
        "regular_price": regular_price.text.strip().replace("лв.", "").strip() if regular_price else "N/A",
        "promo_price": promo_price.text.strip().replace("лв.", "").strip() if promo_price else None,
    }


def fetch_product_data_praktiker(code):
    url = PRAKTIKER_SEARCH_URL.format(code)
    soup = get_soup(url)
    if not soup:
        return {"code": code, "name": "N/A", "regular_price": "N/A", "promo_price": None}

    name_element = soup.select_one("h2.product-item__title a")
    name = name_element.text.strip() if name_element else "N/A"

    regular_price = "N/A"
    promo_price = None

    old_price_element = soup.select_one("span.product-price--old .product-price__value")
    old_price_sup = old_price_element.find_next("sup") if old_price_element else None
    if old_price_element:
        regular_price = old_price_element.text.strip()
        if old_price_sup:
            regular_price += "." + old_price_sup.text.strip()
    else:
        regular_price_element = soup.select_one("span.product-price__value, span.price__value")
        regular_price_sup = regular_price_element.find_next("sup") if regular_price_element else None
        if regular_price_element:
            regular_price = regular_price_element.text.strip()
            if regular_price_sup:
                regular_price += "." + regular_price_sup.text.strip()

    promo_price_element = soup.select_one(
        "div.product-store-prices__item > span.product-price:not(.product-price--old) span.product-price__value"
    )
    promo_price_sup = promo_price_element.find_next("sup") if promo_price_element else None
    if promo_price_element:
        promo_price = promo_price_element.text.strip()
        if promo_price_sup:
            promo_price += "." + promo_price_sup.text.strip()

    return {
        "code": code,
        "name": name,
        "regular_price": regular_price,
        "promo_price": promo_price,
    }


def process_excel_and_fetch_data(input_file, output_file):
    try:
        df = pd.read_excel(input_file, engine="odf")
        praktis_codes = df["Praktis"]
        praktiker_codes = df["Praktiker"]

        # Prepare output DataFrame
        output_df = pd.DataFrame(columns=["ID", "Name", "Praktis", "Praktiker", "Praktis Promo", "Praktiker Promo"])

        delay_range = (0.5, 2.0)  # Delay between requests
        max_threads = 5  # Limit concurrent threads to avoid server overload

        praktis_data = []
        praktiker_data = []

        with ThreadPoolExecutor(max_threads) as executor:
            futures_praktis = {executor.submit(fetch_product_data_praktis, code): code for code in praktis_codes}
            for future in as_completed(futures_praktis):
                praktis_data.append(future.result())
                time.sleep(random.uniform(*delay_range))  # Random delay

            futures_praktiker = {executor.submit(fetch_product_data_praktiker, code): code for code in praktiker_codes}
            for future in as_completed(futures_praktiker):
                praktiker_data.append(future.result())
                time.sleep(random.uniform(*delay_range))  # Random delay

        # Combine data and populate the output DataFrame
        for p_data, k_data in zip(praktis_data, praktiker_data):
            output_df = pd.concat(
                [
                    output_df,
                    pd.DataFrame([{
                        "ID": p_data["code"],
                        "Name": p_data["name"],
                        "Praktis": p_data["regular_price"],
                        "Praktiker": k_data["regular_price"],
                        "Praktis Promo": p_data["promo_price"],
                        "Praktiker Promo": k_data["promo_price"],
                    }])
                ],
                ignore_index=True
            )

        # Save the results to an Excel file
        output_df.to_excel(output_file, index=False, engine="openpyxl")
        print(f"Data exported successfully to {output_file}")
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    input_excel = r"C:\Users\МЕГАДОМ\Desktop\products_list.ods"
    output_excel = r"C:\Users\МЕГАДОМ\Desktop\product_details.xlsx"
    process_excel_and_fetch_data(input_excel, output_excel)

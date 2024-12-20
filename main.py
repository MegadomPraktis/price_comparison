import time
import random
import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl import load_workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment
from datetime import datetime

# Constants for search URLs
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
            response = session.get(url, timeout=15)
            response.raise_for_status()
            return BeautifulSoup(response.content, 'html.parser')
        except requests.RequestException:
            time.sleep(2 ** attempt + random.uniform(0.5, 1.5))  # Exponential backoff
    return None

def fetch_product_data_praktis(code):
    code = str(code).strip()  # Ensure the code is a string and strip leading/trailing whitespace
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
        "regular_price": regular_price.text.strip().replace("\u043b\u0432.", "").strip() if regular_price else "N/A",
        "promo_price": promo_price.text.strip().replace("\u043b\u0432.", "").strip() if promo_price else None,
    }

def fetch_product_data_praktiker(code):
    code = str(code).strip()  # Ensure the code is a string and strip leading/trailing whitespace
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

def adjust_excel_formatting(file_path):
    workbook = load_workbook(file_path)
    sheet = workbook.active

    for col in sheet.columns:
        max_length = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                if cell.value:
                    max_length = max(max_length, len(str(cell.value)))
                cell.alignment = Alignment(wrap_text=True)
            except:
                pass
        adjusted_width = max_length + 2
        sheet.column_dimensions[col_letter].width = adjusted_width

    workbook.save(file_path)
    workbook.close()

def process_excel_and_fetch_data(input_file, output_file):
    try:
        # Start the timer
        start_time = time.time()

        df = pd.read_excel(input_file, engine="odf")
        praktis_codes = df.iloc[:, 0].tolist()
        praktiker_codes = df.iloc[:, 1].tolist()

        delay_range = (1.0, 2.0)  # Delay between requests
        max_threads = 5  # Limit concurrent threads to avoid server overload

        praktis_data = {code: None for code in praktis_codes}

        with ThreadPoolExecutor(max_threads) as executor:
            futures_praktis = {executor.submit(fetch_product_data_praktis, code): code for code in praktis_codes}
            for future in as_completed(futures_praktis):
                code = futures_praktis[future]
                praktis_data[code] = future.result()
                time.sleep(random.uniform(*delay_range))

        praktis_data = [praktis_data[code] for code in praktis_codes]

        praktiker_data = []
        with ThreadPoolExecutor(max_threads) as executor:
            futures_praktiker = {executor.submit(fetch_product_data_praktiker, code): code for code in praktiker_codes}
            for future in as_completed(futures_praktiker):
                praktiker_data.append(future.result())
                time.sleep(random.uniform(*delay_range))

        output_df = pd.DataFrame({
            "Praktis Code": praktis_codes,
            "Praktiker Code": praktiker_codes,
            "Praktis Name": [item["name"] for item in praktis_data],
            "Praktiker Name": [item["name"] for item in praktiker_data],
            "Praktis Regular Price": [item["regular_price"] for item in praktis_data],
            "Praktiker Regular Price": [item["regular_price"] for item in praktiker_data],
            "Praktis Promo Price": [item["promo_price"] for item in praktis_data],
            "Praktiker Promo Price": [item["promo_price"] for item in praktiker_data],
        })

        output_df.to_excel(output_file, index=False)
        adjust_excel_formatting(output_file)

        # Stop the timer and calculate elapsed time
        end_time = time.time()
        elapsed_time = end_time - start_time

        print(f"Data exported successfully to {output_file}")
        print(f"Execution time: {elapsed_time:.2f} seconds")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    input_excel = r"C:\Users\МЕГАДОМ\Desktop\products_list - Copy.ods"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    output_excel = rf"C:\Users\МЕГАДОМ\Desktop\product_details_{timestamp}.xlsx"
    process_excel_and_fetch_data(input_excel, output_excel)

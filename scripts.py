import os
import requests
import time
import csv
from dotenv import load_dotenv

load_dotenv()
polygon_api_key = os.getenv("API_key")

limit = 1000
url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&order=asc&limit={limit}&sort=ticker&apiKey={polygon_api_key}"

tickers = []

example_ticker = {'ticker': 'BASE', 'name': 'Couchbase, Inc. Common Stock', 'market': 'stocks', 'locale': 'us', 'primary_exchange': 'XNAS', 'type': 'CS', 'active': True, 'currency_name': 'usd', 'cik': '0001845022', 'composite_figi': 'BBG001Z5ZB04', 'share_class_figi': 'BBG001Z5ZB22', 'last_updated_utc': '2025-09-25T06:05:34.542162508Z'}

FIELDS = [
    "ticker",
    "name",
    "market",
    "locale",
    "primary_exchange",
    "type",
    "active",
    "currency_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "last_updated_utc",
]
csv_path = "tickers.csv"

csv_file = open(csv_path, "w", newline="", encoding="utf-8")
writer = csv.DictWriter(csv_file, fieldnames=FIELDS)
writer.writeheader()

while url:
    response = requests.get(url)
    data = response.json()

    # Stop if API returned an error
    if data.get("status") != "OK":
        print("Error from API:", data)
        break

    # Collect tickers
    for t in data.get("results", []):
        tickers.append(t["ticker"])

        # Write a CSV row with the exact schema (fill missing fields with empty string)
        row = {k: t.get(k, "") for k in FIELDS}
        writer.writerow(row)

    # Move to next page (if any)
    url = data.get("next_url")
    if url and "apiKey=" not in url:
        url = f"{url}&apiKey={polygon_api_key}"
    time.sleep(1)

csv_file.close()

print(f"Total tickers collected: {len(tickers)}")
print(f"Wrote CSV: {csv_path}")

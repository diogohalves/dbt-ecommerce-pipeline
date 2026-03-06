import requests
import csv
from datetime import date

url = "https://api.exchangerate-api.com/v4/latest/BRL"
response = requests.get(url)
data = response.json()

currencies = ["USD", "EUR", "GBP", "ARS", "CLP"]
rows = []

for currency in currencies:
    if currency in data["rates"]:
        rows.append({
            "base_currency": "BRL",
            "target_currency": currency,
            "rate": data["rates"][currency],
            "rate_date": date.today().isoformat()
        })

output_path = "seeds/exchange_rates.csv"

with open(output_path, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["base_currency", "target_currency", "rate", "rate_date"])
    writer.writeheader()
    writer.writerows(rows)

print(f"✅ exchange_rates.csv criado com {len(rows)} moedas")
for r in rows:
    print(f"  BRL → {r['target_currency']}: {r['rate']}")
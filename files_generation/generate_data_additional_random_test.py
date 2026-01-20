import csv
import random
from datetime import datetime
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_clients_enriched(n_clients: int, output_path: str):
    countries = ["France", "Germany", "Spain", "Italy", "Belgium", "Canada"]
    segments = ["STANDARD", "PREMIUM", "VIP"]
    channels = ["SEO", "ADS", "SOCIAL", "REFERRAL"]

    clients = []

    for i in range(1, n_clients + 1):
        age = random.randint(18, 75)
        date_inscription = fake.date_between(start_date="-4y", end_date="today")

        clients.append({
            "id_client": i,
            "nom": fake.name(),
            "email": fake.email(),
            "date_inscription": date_inscription.strftime("%Y-%m-%d"),
            "pays": random.choice(countries),
            "age": age,
            "segment_client": random.choices(segments, weights=[0.6, 0.3, 0.1])[0],
            "revenu_annuel": round(random.uniform(15000, 120000), 2),
            "canal_acquisition": random.choice(channels)
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=clients[0].keys())
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated enriched clients file → {output_path}")

if __name__ == "__main__":
    generate_clients_enriched(
        1500,
        "./data/sources/clients_enriched.csv"
    )

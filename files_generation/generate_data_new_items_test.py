import csv
import random
from datetime import datetime, timedelta
from pathlib import Path
from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

# =========================
# Clients enrichis
# =========================

def generate_clients_enriched(n_clients: int, output_path: str) -> list[int]:
    countries = ["France", "Germany", "Spain", "Italy", "Belgium", "Canada"]
    segments = ["STANDARD", "PREMIUM", "VIP"]
    channels = ["SEO", "ADS", "SOCIAL", "REFERRAL"]

    clients = []
    client_ids = []

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

        client_ids.append(i)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=clients[0].keys())
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated enriched clients → {output_path}")
    return client_ids


# =========================
# Achats
# =========================

def generate_achats(client_ids: list[int], avg_purchases_per_client: int, output_path: str) -> None:
    products = [
        "Laptop", "Phone", "Tablet", "Headphones", "Monitor",
        "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"
    ]

    achats = []
    achat_id = 1
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    for client_id in client_ids:
        n_purchases = random.randint(1, avg_purchases_per_client * 2)

        for _ in range(n_purchases):
            date_achat = fake.date_time_between(start_date=start_date, end_date=end_date)
            achats.append({
                "id_achat": achat_id,
                "id_client": client_id,
                "date_achat": date_achat.strftime("%Y-%m-%d %H:%M:%S"),
                "montant": round(random.uniform(10, 500), 2),
                "produit": random.choice(products)
            })
            achat_id += 1

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"]
        )
        writer.writeheader()
        writer.writerows(achats)

    print(f"Generated {len(achats)} achats → {output_path}")


# =========================
# Main
# =========================

if __name__ == "__main__":
    output_dir = Path("./data/sources")
    output_dir.mkdir(parents=True, exist_ok=True)

    client_ids = generate_clients_enriched(
        n_clients=1500,
        output_path=str(output_dir / "clients_enriched.csv")
    )

    generate_achats(
        client_ids=client_ids,
        avg_purchases_per_client=15,
        output_path=str(output_dir / "achats.csv")
    )

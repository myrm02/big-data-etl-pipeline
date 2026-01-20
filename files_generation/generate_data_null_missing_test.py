import csv
import random
from pathlib import Path
from faker import Faker

fake = Faker()
random.seed(42)

def generate_clients_incomplete(n_clients: int, output_path: str):
    clients = []

    for i in range(1, n_clients + 1):
        row = {
            "id_client": i,
            "nom": fake.name() if random.random() > 0.1 else "",
            "email": fake.email() if random.random() > 0.3 else None,
            "date_inscription": "INVALID_DATE" if random.random() > 0.8 else fake.date(),
            "pays": random.choice(["France", None, "", "Germany"])
        }

        # Supprimer complètement une colonne parfois
        if random.random() > 0.85:
            row.pop("email")

        clients.append(row)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    fieldnames = set().union(*(c.keys() for c in clients))

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated incomplete clients file → {output_path}")

if __name__ == "__main__":
    generate_clients_incomplete(
        1000,
        "./data/sources/clients_incomplete.csv"
    )
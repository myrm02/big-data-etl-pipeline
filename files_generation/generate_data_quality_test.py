import csv
import random
from pathlib import Path

def generate_clients_noisy(n_clients: int, output_path: str):
    clients = []

    for i in range(1, n_clients + 1):
        clients.append({
            "id_client": i,
            "nom": random.choice(["###", "???", "CLIENT_X", ""]),
            "email": random.choice(["not_an_email", "", None]),
            "age": random.choice([-10, 0, 250, None]),
            "revenu_annuel": random.choice([-50000, 0, 9999999]),
            "pays": random.choice(["FR", "123", "", None]),
        })

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=clients[0].keys())
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated noisy clients file → {output_path}")

if __name__ == "__main__":
    generate_clients_noisy(
        800,
        "./data/sources/clients_noisy.csv"
    )
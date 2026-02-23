import csv
import random
import uuid
from datetime import datetime, timedelta
import os

RAW_PATH = "../data/raw/sales_raw.csv"

def random_status():
    return random.choice(["Completed", "completed", "CANCELLED", "Pending"])

def generate_orders(num_orders=50):
    rows = []
    base_time = datetime.now()

    for _ in range(num_orders):
        order_id = random.randint(1000, 1020)  # intentional duplicates
        customer_id = random.randint(1, 10)
        product_id = random.randint(100, 110)
        quantity = random.randint(1, 5)

        # introduce missing price occasionally
        price = random.choice([round(random.uniform(10, 200), 2), None])

        order_status = random_status()

        order_timestamp = base_time - timedelta(days=random.randint(0, 5))
        updated_at = base_time

        rows.append([
            order_id,
            customer_id,
            product_id,
            quantity,
            price,
            order_status,
            order_timestamp,
            updated_at
        ])

    return rows


def write_to_csv(rows):
    os.makedirs(os.path.dirname(RAW_PATH), exist_ok=True)

    file_exists = os.path.isfile(RAW_PATH)

    with open(RAW_PATH, mode="a", newline="") as file:
        writer = csv.writer(file)

        if not file_exists:
            writer.writerow([
                "order_id",
                "customer_id",
                "product_id",
                "quantity",
                "price",
                "order_status",
                "order_timestamp",
                "updated_at"
            ])

        writer.writerows(rows)


if __name__ == "__main__":
    data = generate_orders()
    write_to_csv(data)
    print("Raw data generated successfully.")
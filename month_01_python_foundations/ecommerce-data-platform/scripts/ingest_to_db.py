from io import SEEK_SET
import sqlite3
import csv
import os
from datetime import datetime

RAW_PATH = "../data/raw/sales_raw.csv"
DB_PATH = "../database.db"

def create_table(conn):
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders_raw(
            order_id INTEGER,
            customer_id INTEGER,
            product_id INTEGER,
            quantity INTEGER,
            price REAL,
            order_status TEXT,
            order_timestamp TEXT,
            update TEXT,
            PRIMARY KEY (order_id)
    )
    """)
    conn.commit()

def ingest_data(conn):
    cursor = conn.cursor()

    with open(RAW_PATH, newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            cursor.execute("""
            INSERT INTO orders_raw (
            order_id,
            customer_id,
            product_id,
            quantity,
            price,
            order_status,
            order_timestamp,
            update
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(order_id) DO UPDATE SET
                customer_id=excluded.customer_id,
                product_id=excluded.product_id,
                quantity=excluded.quantity,
                price=excluded.price,
                order_status=excluded.order_status,
                order_timestamp=excluded.order_timestamp,
                updated_at=excluded.updated_at
            """, (
                int(row["order_id"]),
                int(row["customer_id"]),
                int(row["product_id"]),
                int(row["quantity"]),
                float(row["price"]) if row["price"] else None,
                row["order_status"],
                row["order_timestamp"],
                row["update_at"]
            ))
            
        conn.commit()


if __name__ == "__main__":
    conn = sqlite3.connect(DB_PATH)
    create_table(conn)
    ingest_data(conn)
    conn.close()
    print("Ingestion complete.")

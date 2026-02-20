import csv
import hashlib
from pathlib import Path
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

SOURCE_FILE = Path("data/source/employees.csv")
RAW_DIR = Path("data/raw")
META_FILE = Path("data/metadata/ingestion_log.csv")

EXPECTED_COLUMNS = ["id", "name", "department", "salary", "age", "city"]


def file_hash(path):
    content = path.read_bytes()
    return hashlib.md5(content).hexdigest()

def already_ingested(hash_value):
    if not META_FILE.exists():
        return False

    with open(META_FILE, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["file_hash"] == hash_value:
                return True
    return False

def log_metadata(hash_value, rows):
    exists = META_FILE.exists()
    with open(META_FILE, "a", newline="") as f:
        fieldnames = ["timestamp", "file_hash", "rows"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if not exists:
            writer.writeheader()
        writer.writerow({
            "timestamp": datetime.utcnow(),
            "file_hash": hash_value,
            "rows": rows
        })

def ingest():
    if not SOURCE_FILE.exists():
        logger.warning("No source file found")
        return

    
    hash_value = file_hash(SOURCE_FILE)

    if already_ingested(hash_value):
        logger.info("File already ingested - skipping")
        return

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    valid_rows = []
    with open(SOURCE_FILE, newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames != EXPECTED_COLUMNS:
            logger.error("Schema mismatch")
            return

        for row in reader:
            if len(row) != len(EXPECTED_COLUMNS):
                logger.warning(f"Skipping malformed row: {row}")
                continue
            valid_rows.append(row)
    
    output_file = RAW_DIR / f"employees_{hash_value}.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=EXPECTED_COLUMNS)
        writer.writeheader()
        writer.writerows(valid_rows)

    log_metadata(hash_value, len(valid_rows))
    logger.info(f"Ingested {len(valid_rows)} rows into raw layer")
import logging
import os
from dotenv import load_dotenv
from pathlib import Path

# load environoment variable
load_dotenv("configs/.env")

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / "pipeline.log"

logging.basicConfig(
    filename = log_file,
    level = logging.INFO,
    format = "%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(__name__)

def main():
    logger.info("Pipeline Started")
    logger.info(f"Environment: {os.getenv('Env')}")
    logger.info("Pipeline Finished Successfully")

if __name__ == "__main__":
    main()
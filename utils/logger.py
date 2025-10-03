import logging
from utils import config

# Setup logging dengan level dari .env
logging.basicConfig(
    level=config.LOG_LEVEL,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Buat logger global untuk ETL
logger = logging.getLogger("etl")

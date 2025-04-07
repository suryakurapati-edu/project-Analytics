import logging
import os
from datetime import datetime

def get_logger(job_name="default"):
    LOG_DIR = "logs"
    os.makedirs(LOG_DIR, exist_ok=True)
    log_file = os.path.join(LOG_DIR, f"etl_{job_name}_{datetime.now().strftime('%Y%m%d')}.log")

    logger = logging.getLogger(f"ETLLogger_{job_name}")
    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
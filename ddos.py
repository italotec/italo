#!/usr/bin/env python3
"""
cpf_requester.py

Sends one GET request per second to:
    https://emprestimofacilitado.com/js/consulta_a.php?cpf=<CPF>

CPFs are read from a CSV file (column name: 'cpf').
"""

import csv
import time
import logging
import requests
from pathlib import Path
from typing import List

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #
CSV_PATH = Path("cpfs.csv")          # <-- change if your file is elsewhere
CPF_COLUMN = "cpf"                   # column header in the CSV
ENDPOINT_TEMPLATE = "https://emprestimofacilitado.com/js/consulta_a.php?cpf={cpf}"

# Request settings
TIMEOUT = 1                        # seconds
REQUESTS_PER_SECOND = 5000
DELAY = 1.0 / REQUESTS_PER_SECOND

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
def load_cpfs(csv_path: Path, column: str) -> List[str]:
    """Read CPFs from CSV and return a list of stripped strings."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV file not found: {csv_path}")

    cpfs = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if column not in reader.fieldnames:
            raise ValueError(f"Column '{column}' not found in CSV. "
                             f"Available: {reader.fieldnames}")
        for row in reader:
            cpf = row[column].strip()
            if cpf:
                cpfs.append(cpf)
    log.info(f"Loaded {len(cpfs)} CPF(s) from {csv_path}")
    return cpfs


def send_request(cpf: str) -> None:
    """Send a single GET request and log the result."""
    url = ENDPOINT_TEMPLATE.format(cpf=cpf)
    start = time.time()

    try:
        resp = requests.get(url, timeout=TIMEOUT)
        elapsed = time.time() - start
        log.info(
            f"CPF {cpf} -> status {resp.status_code} "
            f"({elapsed:.2f}s) | {len(resp.content)} bytes"
        )
        # Uncomment next line if you want to see the raw response:
        # log.debug(resp.text[:500])
    except requests.RequestException as e:
        log.error(f"CPF {cpf} -> error: {e}")


def main() -> None:
    cpfs = load_cpfs(CSV_PATH, CPF_COLUMN)

    if not cpfs:
        log.warning("No CPFs to process. Exiting.")
        return

    log.info(f"Starting requests (1 per second) for {len(cpfs)} CPF(s)...")
    for i, cpf in enumerate(cpfs, start=1):
        send_request(cpf)

        # Sleep to enforce exactly 1 request/second (accounting for request time)
        if i < len(cpfs):
            sleep_time = DELAY - (time.time() - time.monotonic())
            if sleep_time > 0:
                time.sleep(sleep_time)

    log.info("All requests completed.")


if __name__ == "__main__":
    main()
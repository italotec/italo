#!/usr/bin/env python3
"""
cpf_tor_requester.py

- Reads CPFs from cpfs.csv (column: cpf)
- Sends 1 request/second via Tor (Socks5h proxy on 127.0.0.1:9050)
- Renews Tor circuit every N requests (default: 10)
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
CSV_PATH = Path("cpfs.csv")               # <-- change if needed
CPF_COLUMN = "cpf"                        # column name in CSV
ENDPOINT_TEMPLATE = "https://emprestimofacilitado.com/js/consulta_a.php?cpf={cpf}"

# Tor proxy (Tails default)
TOR_PROXY = {
    "http":  "socks5h://127.0.0.1:9050",
    "https": "socks5h://127.0.0.1:9050"
}

# Request settings
TIMEOUT = 1                              # seconds (Tor can be slow)
REQUESTS_PER_SECOND = 5000
DELAY = 1.0 / REQUESTS_PER_SECOND

# Renew Tor circuit every N requests (set 0 to disable)
RENEW_CIRCUIT_EVERY = 0

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

# --------------------------------------------------------------------------- #
def load_cpfs(csv_path: Path, column: str) -> List[str]:
    """Load and clean CPFs from CSV."""
    if not csv_path.is_file():
        raise FileNotFoundError(f"CSV not found: {csv_path}")

    cpfs = []
    with csv_path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if column not in reader.fieldnames:
            raise ValueError(f"Column '{column}' missing. Found: {reader.fieldnames}")
        for row in reader:
            cpf = row[column].strip()
            if cpf:
                cpfs.append(cpf)
    log.info(f"Loaded {len(cpfs)} CPF(s) from {csv_path}")
    return cpfs


def renew_tor_circuit():
    """Send NEWNYM signal to Tor control port (9051 in Tails)."""
    try:
        import socket
        import struct

        control_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        control_socket.connect(("127.0.0.1", 9051))
        control_socket.send(b'AUTHENTICATE\r\n')
        resp = control_socket.recv(1024)
        if not resp.startswith(b'250'):
            log.warning("Tor auth failed")
            return

        control_socket.send(b'SIGNAL NEWNYM\r\n')
        resp = control_socket.recv(1024)
        control_socket.close()

        if resp.startswith(b'250'):
            log.info("Tor circuit renewed (NEWNYM)")
        else:
            log.warning(f"NEWNYM failed: {resp!r}")
    except Exception as e:
        log.error(f"Failed to renew Tor circuit: {e}")


def send_request(session: requests.Session, cpf: str) -> None:
    """Send one request via Tor and log result."""
    url = ENDPOINT_TEMPLATE.format(cpf=cpf)
    start = time.time()

    try:
        resp = session.get(url, timeout=TIMEOUT)
        elapsed = time.time() - start
        log.info(
            f"CPF {cpf} -> {resp.status_code} "
            f"({elapsed:.2f}s) | {len(resp.content)} bytes"
        )
        # Uncomment to log snippet of response:
        # log.debug(resp.text[:500])
    except requests.RequestException as e:
        log.error(f"CPF {cpf} -> REQUEST ERROR: {e}")


def main() -> None:
    cpfs = load_cpfs(CSV_PATH, CPF_COLUMN)
    if not cpfs:
        log.warning("No CPFs to process.")
        return

    # Persistent session with Tor proxy
    session = requests.Session()
    session.proxies.update(TOR_PROXY)

    log.info(f"Starting Tor-routed requests (1/sec) for {len(cpfs)} CPF(s)...")

    for i, cpf in enumerate(cpfs, start=1):
        send_request(session, cpf)

        # Renew circuit periodically
        if RENEW_CIRCUIT_EVERY > 0 and i % RENEW_CIRCUIT_EVERY == 0:
            renew_tor_circuit()
            time.sleep(2)  # give Tor a moment to establish new stream

        # Enforce 1 request per second
        if i < len(cpfs):
            elapsed = time.time() - time.monotonic()
            sleep_time = DELAY - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)

    log.info("All done.")


if __name__ == "__main__":
    main()

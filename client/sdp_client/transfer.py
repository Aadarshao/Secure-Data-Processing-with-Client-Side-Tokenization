import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


def load_tokenized_csv(path: str) -> List[Dict[str, Any]]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Tokenized CSV not found: {p}")

    records: List[Dict[str, Any]] = []
    with p.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)
    return records


def upload_batch(
    file_path: str,
    client_id: str,
    processing_type: str,
    server_url: Optional[str] = None,
    batch_id: Optional[str] = None,
    verify_tls: bool | str = True,
) -> Dict[str, Any]:
    """
    Upload a tokenized CSV as JSON to the ingestion API.

    server_url: base URL such as
      - http://localhost:8081        (dev, no TLS)
      - https://ingestion.example.com (prod, TLS)

    verify_tls:
      - True  (default)  -> use system CA bundle
      - False            -> skip certificate verification (dev only)
      - "path/to/ca.pem" -> verify against custom CA file
    """
    base_url = server_url or os.getenv("SDP_SERVER_URL", "http://localhost:8081")
    endpoint = f"{base_url.rstrip('/')}/api/v1/process"

    records = load_tokenized_csv(file_path)

    payload: Dict[str, Any] = {
        "client_id": client_id,
        "processing_type": processing_type,
        "records": records,
    }
    if batch_id:
        payload["batch_id"] = batch_id

    response = requests.post(
        endpoint,
        headers={"Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=30,
        verify=verify_tls,
    )
    response.raise_for_status()
    return response.json()

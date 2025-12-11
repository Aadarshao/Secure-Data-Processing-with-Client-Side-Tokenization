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
    api_key: Optional[str] = None,
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

    api_key:
      - Optional. If not provided, will read from SDP_API_KEY env variable.
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

    headers: Dict[str, str] = {
        "Content-Type": "application/json",
    }

    resolved_api_key = api_key or os.getenv("SDP_API_KEY")
    if resolved_api_key:
        headers["X-API-Key"] = resolved_api_key

    try:
        response = requests.post(
            endpoint,
            headers=headers,
            data=json.dumps(payload),
            timeout=30,
            verify=verify_tls,
        )
    except requests.RequestException as e:
        # Network / connection level error
        raise RuntimeError(
            f"Failed to connect to ingestion API at {endpoint}: {e}"
        ) from e

    if response.status_code == 401:
        raise RuntimeError(
            "Unauthorized (401) calling ingestion API. "
            "Check SDP_API_KEY or --api-key matches the server's SDP_API_KEY."
        )

    response.raise_for_status()
    return response.json()


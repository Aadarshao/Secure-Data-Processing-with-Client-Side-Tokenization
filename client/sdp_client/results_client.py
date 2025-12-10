import os
from typing import Any, Dict

import requests


def fetch_results(
    batch_id: str,
    server_url: str | None = None,
    verify_tls: bool | str = True,
) -> Dict[str, Any]:
    """
    Call the Results API: GET /api/v1/results/{batch_id}

    verify_tls:
      - True  (default)  -> normal TLS verification
      - False            -> skip TLS verification (dev only)
      - "path/to/ca.pem" -> verify against custom CA bundle
    """
    base_url = server_url or os.getenv("SDP_SERVER_URL", "http://localhost:8081")
    endpoint = f"{base_url.rstrip('/')}/api/v1/results/{batch_id}"

    resp = requests.get(endpoint, timeout=30, verify=verify_tls)
    resp.raise_for_status()
    return resp.json()

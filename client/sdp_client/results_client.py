import os
from typing import Any, Dict, Optional

import requests


def fetch_results(
    batch_id: str,
    server_url: Optional[str] = None,
    verify_tls: bool | str = True,
    api_key: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call the Results API: GET /api/v1/results/{batch_id}

    verify_tls:
      - True  (default)  -> normal TLS verification
      - False            -> skip TLS verification (dev only)
      - "path/to/ca.pem" -> verify against custom CA bundle

    api_key:
      - Optional. If not provided, will read from SDP_API_KEY env variable.
    """
    base_url = server_url or os.getenv("SDP_SERVER_URL", "http://localhost:8081")
    endpoint = f"{base_url.rstrip('/')}/api/v1/results/{batch_id}"

    # Build headers
    headers: Dict[str, str] = {}

    resolved_api_key = api_key or os.getenv("SDP_API_KEY")
    if resolved_api_key:
        headers["X-API-Key"] = resolved_api_key

    resp = requests.get(
        endpoint,
        headers=headers,
        timeout=30,
        verify=verify_tls,
    )
    resp.raise_for_status()
    return resp.json()

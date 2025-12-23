import os
from pathlib import Path
from typing import Any, Dict, Optional

import requests


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
    Upload a tokenized CSV to the ingestion API using multipart/form-data.

    Server endpoint:
      POST /api/v1/process-file
        Form fields: client_id, processing_type, batch_id (optional)
        File field:  file
    """
    base_url = server_url or os.getenv("SDP_SERVER_URL", "http://localhost:8081")
    endpoint = f"{base_url.rstrip('/')}/api/v1/process-file"

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"Tokenized CSV not found: {p}")

    headers: Dict[str, str] = {}
    resolved_api_key = api_key or os.getenv("SDP_API_KEY")
    if resolved_api_key:
        headers["X-API-Key"] = resolved_api_key

    form: Dict[str, str] = {
        "client_id": client_id,
        "processing_type": processing_type,
    }
    if batch_id:
        form["batch_id"] = batch_id

    try:
        with p.open("rb") as f:
            files = {"file": (p.name, f, "text/csv")}
            resp = requests.post(
                endpoint,
                headers=headers,
                data=form,
                files=files,
                timeout=30,
                verify=verify_tls,
            )
    except requests.RequestException as e:
        raise RuntimeError(f"Failed to connect to ingestion API at {endpoint}: {e}") from e

    if resp.status_code == 401:
        raise RuntimeError(
            "Unauthorized (401). Check SDP_API_KEY / --api-key matches the server."
        )

    if resp.status_code == 422:
        # surface FastAPI validation detail
        raise RuntimeError(f"Unprocessable Entity (422): {resp.text}")

    resp.raise_for_status()
    return resp.json()

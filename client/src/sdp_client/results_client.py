import os
import time
from typing import Any, Dict, Optional

import requests


def _compute_wait_from_429(resp: requests.Response, default_wait_s: float = 1.0) -> float:
    """
    Decide how long to wait after a 429.
    Priority:
      1) Retry-After (seconds)
      2) X-RateLimit-Reset (epoch seconds)
      3) default_wait_s
    """
    retry_after = resp.headers.get("Retry-After")
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except Exception:
            pass

    reset_epoch = resp.headers.get("X-RateLimit-Reset")
    if reset_epoch:
        try:
            reset_epoch_i = int(reset_epoch)
            now = int(time.time())
            return max(0.0, float(reset_epoch_i - now))
        except Exception:
            pass

    return default_wait_s


def fetch_results(
    batch_id: str,
    client_id: str,
    server_url: Optional[str] = None,
    verify_tls: bool | str = True,
    api_key: Optional[str] = None,
    timeout_seconds: int = 30,
    max_retries_on_429: int = 2,
) -> Dict[str, Any]:
    """
    Call the Results API: GET /api/v1/results/{batch_id}?client_id=...

    verify_tls:
      - True  (default)  -> normal TLS verification
      - False            -> skip TLS verification (dev only)
      - "path/to/ca.pem" -> verify against custom CA bundle

    api_key:
      - Optional. If not provided, will read from SDP_API_KEY env variable.

    max_retries_on_429:
      - Retries when server returns 429 Rate Limit.
    """
    base_url = server_url or os.getenv("SDP_SERVER_URL", "http://localhost:8081")
    endpoint = f"{base_url.rstrip('/')}/api/v1/results/{batch_id}"

    headers: Dict[str, str] = {}
    resolved_api_key = api_key or os.getenv("SDP_API_KEY")
    if resolved_api_key:
        headers["X-API-Key"] = resolved_api_key

    params = {"client_id": client_id}

    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(
                endpoint,
                headers=headers,
                params=params,
                timeout=timeout_seconds,
                verify=verify_tls,
            )
        except requests.RequestException as e:
            raise RuntimeError(f"Failed to connect to results API at {endpoint}: {e}") from e

        if resp.status_code == 401:
            raise RuntimeError(
                "Unauthorized (401) calling results API. "
                "Check SDP_API_KEY or --api-key matches the server's SDP_API_KEY "
                "and that the key is authorized for this client_id."
            )

        if resp.status_code == 403:
            raise RuntimeError(
                "Forbidden (403) calling results API. "
                "This usually means cross-tenant access: client_id does not match the batch owner."
            )

        if resp.status_code == 404:
            raise RuntimeError(
                f"Batch {batch_id} not found on server. "
                "Make sure it was uploaded and processed first."
            )

        if resp.status_code == 429:
            if attempt > (max_retries_on_429 + 1):
                raise RuntimeError(
                    "Rate limit exceeded (429) and retry limit reached. "
                    "Try again after a short wait."
                )

            wait_s = _compute_wait_from_429(resp, default_wait_s=1.0)
            time.sleep(wait_s)
            continue

        resp.raise_for_status()
        return resp.json()


def fetch_results_until_processed(
    batch_id: str,
    client_id: str,
    server_url: Optional[str] = None,
    verify_tls: bool | str = True,
    api_key: Optional[str] = None,
    timeout_seconds: int = 30,
    poll_interval_seconds: float = 1.0,
    wait_timeout_seconds: int = 120,
    retry_404_seconds: int = 5,
) -> Dict[str, Any]:
    """
    Poll results until status == "PROCESSED" (or until timeout).

    - wait_timeout_seconds: total time to wait for PROCESSED
    - poll_interval_seconds: base poll interval when not rate-limited
    - retry_404_seconds: for a short window, treat 404 as "not ready yet"
      (helps during races right after upload in some setups)
    """
    deadline = time.time() + float(wait_timeout_seconds)
    retry_404_deadline = time.time() + float(retry_404_seconds)

    last: Optional[Dict[str, Any]] = None

    while True:
        now = time.time()
        if now > deadline:
            status = (last or {}).get("status", "UNKNOWN")
            raise RuntimeError(
                f"Timed out waiting for batch {batch_id} to be PROCESSED. Last status={status}"
            )

        try:
            result = fetch_results(
                batch_id=batch_id,
                client_id=client_id,
                server_url=server_url,
                verify_tls=verify_tls,
                api_key=api_key,
                timeout_seconds=timeout_seconds,
                max_retries_on_429=0,  # we handle 429 ourselves in the outer loop
            )
            last = result

            if str(result.get("status", "")).upper() == "PROCESSED":
                return result

            time.sleep(max(0.1, float(poll_interval_seconds)))
            continue

        except RuntimeError as e:
            msg = str(e)

            # Short grace period: 404 can happen during brief races (optional)
            if " not found on server" in msg and time.time() <= retry_404_deadline:
                time.sleep(max(0.1, float(poll_interval_seconds)))
                continue

            # If fetch_results raised due to 429 retries disabled, it won't happen;
            # but if server responds oddly, just re-raise.
            raise
        except requests.HTTPError as he:
            # (Shouldn't happen because fetch_results handles it, but safe)
            raise RuntimeError(str(he)) from he

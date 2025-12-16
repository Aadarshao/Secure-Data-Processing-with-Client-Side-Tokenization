
import argparse
import json
import time
from pathlib import Path

from .tokenization import tokenize_csv, tokenize_csv_with_config
from .config import load_tokenization_config
from .transfer import upload_batch
from .results_client import fetch_results
from .integration import integrate_results_with_raw_csv


def _resolve_tls_verify(args) -> bool | str:
    """
    Decide TLS verification behavior for requests:
      - args.ca_bundle -> path string
      - args.insecure_skip_verify -> False
      - default -> True
    """
    if getattr(args, "ca_bundle", None):
        return args.ca_bundle
    if getattr(args, "insecure_skip_verify", False):
        return False
    return True


def _wait_for_processed(
    *,
    batch_id: str,
    client_id: str,
    server_url: str | None,
    verify_tls: bool | str,
    api_key: str | None,
    wait_timeout_seconds: int,
    poll_interval_seconds: float,
) -> dict:
    """
    Poll results endpoint until status == PROCESSED (or timeout).
    Returns the final results payload (may still be RECEIVED if you choose not to error).
    """
    deadline = time.time() + max(1, int(wait_timeout_seconds))
    last: dict | None = None

    while True:
        last = fetch_results(
            batch_id=batch_id,
            client_id=client_id,
            server_url=server_url,
            verify_tls=verify_tls,
            api_key=api_key,
        )

        if str(last.get("status", "")).upper() == "PROCESSED":
            return last

        if time.time() >= deadline:
            raise RuntimeError(
                f"Timed out waiting for batch {batch_id} to become PROCESSED "
                f"(last status={last.get('status')})."
            )

        time.sleep(max(0.1, float(poll_interval_seconds)))


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sdp-client",
        description="SDP Client – Tokenization & Transfer Tools",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- tokenize-csv ---
    tokenize_parser = subparsers.add_parser(
        "tokenize-csv",
        help="Tokenize a CSV column and store originals in a local Token Vault",
    )
    tokenize_parser.add_argument("--input", "-i", required=True, help="Path to input CSV file")
    tokenize_parser.add_argument("--output", "-o", required=True, help="Path to output tokenized CSV file")
    tokenize_parser.add_argument("--source-table", required=True, help="Logical source table name (e.g., customers)")
    tokenize_parser.add_argument("--column", "-c", required=True, help="Column name to tokenize (e.g., email)")
    tokenize_parser.add_argument(
        "--token-type",
        default="HASH",
        choices=["HASH", "FPE", "MASKED", "RANDOM"],
        help="Token type (currently only HASH semantics implemented)",
    )

    # --- tokenize-config ---
    cfg_parser = subparsers.add_parser(
        "tokenize-config",
        help="Tokenize a CSV using a YAML config file",
    )
    cfg_parser.add_argument("--config", "-c", required=True, help="Path to YAML config file describing tokenization rules")
    cfg_parser.add_argument("--input", "-i", required=True, help="Path to input CSV file")
    cfg_parser.add_argument("--output", "-o", required=True, help="Path to output tokenized CSV file")

    # --- upload-batch ---
    upload_parser = subparsers.add_parser(
        "upload-batch",
        help="Upload a tokenized CSV file to the ingestion API",
    )
    upload_parser.add_argument("--file", "-f", required=True, help="Path to tokenized CSV file")
    upload_parser.add_argument("--client-id", required=True, help="Client identifier (e.g., bank_demo)")
    upload_parser.add_argument("--processing-type", required=True, help="Processing type (e.g., risk_scoring)")
    upload_parser.add_argument("--server-url", help="Base URL of ingestion API (default: http://localhost:8081)")
    upload_parser.add_argument(
        "--insecure-skip-verify",
        action="store_true",
        help="Skip TLS certificate verification (DEV ONLY; NOT for production!)",
    )
    upload_parser.add_argument("--ca-bundle", help="Path to custom CA bundle to verify TLS (e.g. ca.pem)")
    upload_parser.add_argument("--batch-id", help="Optional batch_id to reuse across uploads (must be a UUID)")
    upload_parser.add_argument(
        "--api-key",
        help="API key for authenticating to the ingestion API (or set SDP_API_KEY env var)",
    )

    # --- fetch-results (Phase 5B helper) ---
    fetch_parser = subparsers.add_parser(
        "fetch-results",
        help="Fetch results for a batch and write raw JSON to a file",
    )
    fetch_parser.add_argument("--batch-id", required=True, help="Batch ID to fetch results for")
    fetch_parser.add_argument("--client-id", required=True, help="Client identifier (must match upload-batch)")
    fetch_parser.add_argument("--output", "-o", required=True, help="Path to output JSON file")
    fetch_parser.add_argument("--server-url", help="Base URL of ingestion/results API (default: http://localhost:8081)")
    fetch_parser.add_argument(
        "--insecure-skip-verify",
        action="store_true",
        help="Skip TLS certificate verification (DEV ONLY; NOT for production!)",
    )
    fetch_parser.add_argument("--ca-bundle", help="Path to custom CA bundle to verify TLS")
    fetch_parser.add_argument(
        "--api-key",
        help="API key for authenticating to the Results API (or set SDP_API_KEY env var)",
    )

    # --- integrate-results ---
    integrate_parser = subparsers.add_parser(
        "integrate-results",
        help="Fetch results for a batch and join with local raw CSV on a key column",
    )
    integrate_parser.add_argument("--batch-id", required=True, help="Batch ID to fetch results for")
    integrate_parser.add_argument("--client-id", required=True, help="Client identifier (must match upload-batch)")
    integrate_parser.add_argument("--raw-input", "-i", required=True, help="Path to raw input CSV with PII (stays local)")
    integrate_parser.add_argument("--output", "-o", required=True, help="Path to output CSV with results merged")
    integrate_parser.add_argument("--key-column", default="customer_id", help="Column name to join on (default: customer_id)")
    integrate_parser.add_argument("--server-url", help="Base URL of ingestion/results API (default: http://localhost:8081)")
    integrate_parser.add_argument(
        "--insecure-skip-verify",
        action="store_true",
        help="Skip TLS certificate verification (DEV ONLY; NOT for production!)",
    )
    integrate_parser.add_argument("--ca-bundle", help="Path to custom CA bundle to verify TLS")
    integrate_parser.add_argument(
        "--api-key",
        help="API key for authenticating to the Results API (or set SDP_API_KEY env var)",
    )
    integrate_parser.add_argument(
        "--wait",
        action="store_true",
        help="Wait until the batch status becomes PROCESSED before integrating results",
    )
    integrate_parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        default=120,
        help="Max seconds to wait for PROCESSED when --wait is set (default: 120)",
    )
    integrate_parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=1.0,
        help="Polling interval seconds when --wait is set (default: 1.0)",
    )

    # --- Parse & dispatch ---
    args = parser.parse_args()

    if args.command == "tokenize-csv":
        tokenize_csv(
            input_path=args.input,
            output_path=args.output,
            source_table=args.source_table,
            column_name=args.column,
            token_type=args.token_type,
        )
        print(f"Tokenized CSV written to: {Path(args.output).resolve()}")
        print("Local Token Vault updated (SQLite: token_vault.db).")
        return

    if args.command == "tokenize-config":
        cfg = load_tokenization_config(args.config)
        tokenize_csv_with_config(
            input_path=args.input,
            output_path=args.output,
            config=cfg,
        )
        print(f"Tokenized CSV written to: {Path(args.output).resolve()}")
        print(
            f"Local Token Vault updated (SQLite: token_vault.db) "
            f"for source_table='{cfg.source_table}'."
        )
        return

    if args.command == "upload-batch":
        verify = _resolve_tls_verify(args)

        result = upload_batch(
            file_path=args.file,
            client_id=args.client_id,
            processing_type=args.processing_type,
            server_url=args.server_url,
            batch_id=args.batch_id,
            verify_tls=verify,
            api_key=args.api_key,
        )
        print("Server response:")
        print(result)
        return

    if args.command == "fetch-results":
        verify = _resolve_tls_verify(args)

        results = fetch_results(
            batch_id=args.batch_id,
            client_id=args.client_id,
            server_url=args.server_url,
            verify_tls=verify,
            api_key=args.api_key,
        )

        out_path = Path(args.output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

        print(f"Results JSON written to: {out_path}")
        return

    if args.command == "integrate-results":
        verify = _resolve_tls_verify(args)

        if args.wait:
            results = _wait_for_processed(
                batch_id=args.batch_id,
                client_id=args.client_id,
                server_url=args.server_url,
                verify_tls=verify,
                api_key=args.api_key,
                wait_timeout_seconds=args.wait_timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
        else:
            results = fetch_results(
                batch_id=args.batch_id,
                client_id=args.client_id,
                server_url=args.server_url,
                verify_tls=verify,
                api_key=args.api_key,
            )

        integrate_results_with_raw_csv(
            raw_input_path=args.raw_input,
            output_path=args.output,
            results=results,
            key_column=args.key_column,
        )

        print(f"Integrated results written to: {Path(args.output).resolve()}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()

import argparse
import json
import os
import shlex
import subprocess
import time
from pathlib import Path

import requests

from .tokenization import tokenize_csv, tokenize_csv_with_config
from .config import load_tokenization_config
from .transfer import upload_batch
from .results_client import fetch_results
from .integration import integrate_results_with_raw_csv


def _resolve_tls_verify(args) -> bool | str:
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


def _run_spark_docker_job(
    *,
    batch_id: str,
    client_id: str,
    processing_type: str,
    db_url: str,
    db_user: str,
    db_password: str,
    spark_image: str,
    jobs_mount: str,
    network: str,
    spark_script_path_in_container: str,
    postgres_jar_path_in_container: str,
    spark_args: list[str] | None = None,
) -> None:
    spark_args = spark_args or []

    cmd = [
        "docker",
        "run",
        "--rm",
        "--network",
        network,
        "-v",
        f"{jobs_mount}:/jobs",
        spark_image,
        "/opt/spark/bin/spark-submit",
        "--jars",
        postgres_jar_path_in_container,
        spark_script_path_in_container,
        "--batch-id",
        batch_id,
        "--client-id",
        client_id,
        "--processing-type",
        processing_type,
        "--db-url",
        db_url,
        "--db-user",
        db_user,
        "--db-password",
        db_password,
        *spark_args,
    ]

    print("\n=== Running Spark job (docker) ===")
    print("Command:")
    print("  " + " ".join(shlex.quote(c) for c in cmd))
    print("=================================\n")

    proc = subprocess.run(cmd, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Spark job failed (exit code {proc.returncode}).")


def _dev_process_batch(*, batch_id: str, server_url: str, verify_tls: bool | str, api_key: str | None) -> dict:
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key

    url = server_url.rstrip("/") + f"/dev/process-batch/{batch_id}"
    r = requests.post(url, headers=headers, timeout=30, verify=verify_tls)
    if r.status_code >= 400:
        raise RuntimeError(f"Dev processing failed: {r.status_code} {r.text}")
    return r.json()


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
    upload_parser.add_argument("--insecure-skip-verify", action="store_true")
    upload_parser.add_argument("--ca-bundle", help="Path to custom CA bundle to verify TLS (e.g. ca.pem)")
    upload_parser.add_argument("--batch-id", help="Optional batch_id to reuse across uploads (must be a UUID)")
    upload_parser.add_argument("--api-key", help="API key for authenticating (or set SDP_API_KEY env var)")

    # --- fetch-results ---
    fetch_parser = subparsers.add_parser(
        "fetch-results",
        help="Fetch results for a batch and write raw JSON to a file",
    )
    fetch_parser.add_argument("--batch-id", required=True, help="Batch ID to fetch results for")
    fetch_parser.add_argument("--client-id", required=True, help="Client identifier (must match upload-batch)")
    fetch_parser.add_argument("--output", "-o", required=True, help="Path to output JSON file")
    fetch_parser.add_argument("--server-url", help="Base URL of ingestion/results API (default: http://localhost:8081)")
    fetch_parser.add_argument("--insecure-skip-verify", action="store_true")
    fetch_parser.add_argument("--ca-bundle")
    fetch_parser.add_argument("--api-key", help="API key (or set SDP_API_KEY env var)")

    # --- integrate-results ---
    integrate_parser = subparsers.add_parser(
        "integrate-results",
        help="Fetch results for a batch and join with local raw CSV on a key column",
    )
    integrate_parser.add_argument("--batch-id", required=True)
    integrate_parser.add_argument("--client-id", required=True)
    integrate_parser.add_argument("--raw-input", "-i", required=True)
    integrate_parser.add_argument("--output", "-o", required=True)
    integrate_parser.add_argument("--key-column", default="customer_id")
    integrate_parser.add_argument("--server-url")
    integrate_parser.add_argument("--insecure-skip-verify", action="store_true")
    integrate_parser.add_argument("--ca-bundle")
    integrate_parser.add_argument("--api-key")
    integrate_parser.add_argument("--wait", action="store_true")
    integrate_parser.add_argument("--wait-timeout-seconds", type=int, default=120)
    integrate_parser.add_argument("--poll-interval-seconds", type=float, default=1.0)

    # --- process-batch ---
    proc_parser = subparsers.add_parser(
        "process-batch",
        help="Upload -> Process -> Wait -> Integrate results into raw CSV (raw PII stays local)",
    )

    proc_parser.add_argument("--raw-input", required=True)
    proc_parser.add_argument("--output", required=True)
    proc_parser.add_argument("--key-column", default="customer_id")

    proc_parser.add_argument("--tokenized-file")
    proc_parser.add_argument("--tokenize-column")
    proc_parser.add_argument("--tokenized-output")
    proc_parser.add_argument("--source-table", default="customers")
    proc_parser.add_argument("--token-type", default="HASH", choices=["HASH", "FPE", "MASKED", "RANDOM"])

    proc_parser.add_argument("--client-id", required=True)
    proc_parser.add_argument("--processing-type", required=True)
    proc_parser.add_argument("--server-url", default="http://localhost:8081")
    proc_parser.add_argument("--batch-id")
    proc_parser.add_argument("--api-key")

    # Optional dev processor (no Spark)
    proc_parser.add_argument("--use-dev-processor", action="store_true", help="Use /dev/process-batch instead of Spark (DEV ONLY)")

    # Spark options (only used if not --use-dev-processor)
    proc_parser.add_argument("--spark-image", default="apache/spark:3.5.1")
    proc_parser.add_argument("--spark-network", default="infra_default")
    proc_parser.add_argument("--jobs-mount", required=True)
    proc_parser.add_argument("--spark-script", default="/jobs/spark_processor.py")
    proc_parser.add_argument("--postgres-jar", default="/jobs/jars/postgresql.jar")

    proc_parser.add_argument("--db-url", required=True)
    proc_parser.add_argument("--db-user", required=True)
    proc_parser.add_argument("--db-password", required=True)

    proc_parser.add_argument("--spark-args", nargs=argparse.REMAINDER, default=[])

    proc_parser.add_argument("--wait-timeout-seconds", type=int, default=300)
    proc_parser.add_argument("--poll-interval-seconds", type=float, default=1.0)

    proc_parser.add_argument("--insecure-skip-verify", action="store_true")
    proc_parser.add_argument("--ca-bundle")

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
        print(f"Local Token Vault updated (SQLite: token_vault.db) for source_table='{cfg.source_table}'.")
        return

    if args.command == "upload-batch":
        verify = _resolve_tls_verify(args)
        api_key = args.api_key or os.getenv("SDP_API_KEY")

        result = upload_batch(
            file_path=args.file,
            client_id=args.client_id,
            processing_type=args.processing_type,
            server_url=args.server_url,
            batch_id=args.batch_id,
            verify_tls=verify,
            api_key=api_key,
        )
        print("Server response:")
        print(result)
        return

    if args.command == "fetch-results":
        verify = _resolve_tls_verify(args)
        api_key = args.api_key or os.getenv("SDP_API_KEY")

        results = fetch_results(
            batch_id=args.batch_id,
            client_id=args.client_id,
            server_url=args.server_url,
            verify_tls=verify,
            api_key=api_key,
        )

        out_path = Path(args.output).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(results, indent=2), encoding="utf-8")
        print(f"Results JSON written to: {out_path}")
        return

    if args.command == "integrate-results":
        verify = _resolve_tls_verify(args)
        api_key = args.api_key or os.getenv("SDP_API_KEY")

        if args.wait:
            results = _wait_for_processed(
                batch_id=args.batch_id,
                client_id=args.client_id,
                server_url=args.server_url,
                verify_tls=verify,
                api_key=api_key,
                wait_timeout_seconds=args.wait_timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )
        else:
            results = fetch_results(
                batch_id=args.batch_id,
                client_id=args.client_id,
                server_url=args.server_url,
                verify_tls=verify,
                api_key=api_key,
            )

        integrate_results_with_raw_csv(
            raw_input_path=args.raw_input,
            output_path=args.output,
            results=results,
            key_column=args.key_column,
        )
        print(f"Integrated results written to: {Path(args.output).resolve()}")
        return

    if args.command == "process-batch":
        verify = _resolve_tls_verify(args)
        api_key = args.api_key or os.getenv("SDP_API_KEY")

        raw_input = Path(args.raw_input).resolve()
        output_csv = Path(args.output).resolve()

        # 1) Determine tokenized file
        if args.tokenized_file:
            tokenized_path = Path(args.tokenized_file).resolve()
        else:
            if not args.tokenize_column:
                raise RuntimeError("You must provide either --tokenized-file OR --tokenize-column.")

            if args.tokenized_output:
                tokenized_path = Path(args.tokenized_output).resolve()
            else:
                tokenized_path = output_csv.with_suffix(".tokenized.csv")

            tokenize_csv(
                input_path=str(raw_input),
                output_path=str(tokenized_path),
                source_table=args.source_table,
                column_name=args.tokenize_column,
                token_type=args.token_type,
            )
            print(f"Tokenized CSV written to: {tokenized_path}")
            print("Local Token Vault updated (SQLite: token_vault.db).")

        # 2) Upload batch (multipart to /api/v1/process-file)
        upload_resp = upload_batch(
            file_path=str(tokenized_path),
            client_id=args.client_id,
            processing_type=args.processing_type,
            server_url=args.server_url,
            batch_id=args.batch_id,
            verify_tls=verify,
            api_key=api_key,
        )

        batch_id = upload_resp.get("batch_id") or upload_resp.get("batchId") or upload_resp.get("id")
        if not batch_id:
            raise RuntimeError(f"Upload response did not include batch_id. Response: {upload_resp}")

        print(f"\nBatch uploaded. batch_id={batch_id}\n")

        # 3) Process (dev or spark)
        if args.use_dev_processor:
            _dev_process_batch(batch_id=batch_id, server_url=args.server_url, verify_tls=verify, api_key=api_key)
        else:
            jobs_mount = str(Path(args.jobs_mount).resolve())
            _run_spark_docker_job(
                batch_id=batch_id,
                client_id=args.client_id,
                processing_type=args.processing_type,
                db_url=args.db_url,
                db_user=args.db_user,
                db_password=args.db_password,
                spark_image=args.spark_image,
                jobs_mount=jobs_mount,
                network=args.spark_network,
                spark_script_path_in_container=args.spark_script,
                postgres_jar_path_in_container=args.postgres_jar,
                spark_args=list(args.spark_args or []),
            )

        # 4) Wait for PROCESSED
        results = _wait_for_processed(
            batch_id=batch_id,
            client_id=args.client_id,
            server_url=args.server_url,
            verify_tls=verify,
            api_key=api_key,
            wait_timeout_seconds=args.wait_timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )

        # 5) Integrate results locally
        output_csv.parent.mkdir(parents=True, exist_ok=True)
        integrate_results_with_raw_csv(
            raw_input_path=str(raw_input),
            output_path=str(output_csv),
            results=results,
            key_column=args.key_column,
        )

        print(f"✅ Done. Integrated results written to: {output_csv}")
        return

    parser.print_help()


if __name__ == "__main__":
    main()

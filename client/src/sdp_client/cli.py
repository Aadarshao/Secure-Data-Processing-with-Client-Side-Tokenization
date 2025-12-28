import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import requests

from .config import load_tokenization_config
from .integration import integrate_results_with_raw_csv
from .results_client import fetch_results
from .tokenization import tokenize_csv, tokenize_csv_with_config
from .transfer import upload_batch


def _read_dotenv_file(path: Path) -> dict[str, str]:
    """
    Minimal .env parser:
      - supports KEY=VALUE
      - strips quotes around VALUE
      - ignores blank lines and comments
    """
    out: dict[str, str] = {}
    if not path.exists():
        return out

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            continue

        k, v = line.split("=", 1)
        k = k.strip()
        v = v.strip()

        # strip surrounding quotes
        if len(v) >= 2 and ((v[0] == v[-1] == '"') or (v[0] == v[-1] == "'")):
            v = v[1:-1]

        if k:
            out[k] = v

    return out

def _find_repo_root(start: Path) -> Path | None:
    """
    Walk upward looking for a repo marker.
    Works no matter where you run the command from.
    """
    for p in [start] + list(start.parents):
        if (p / "infra" / "docker-compose.yml").exists():
            return p
        if (p / ".git").exists():
            return p
        if (p / ".env").exists():
            return p
    return None


def _find_client_dir(repo_root: Path) -> Path | None:
    """
    Find the client directory under repo root.
    """
    candidate = repo_root / "client"
    if candidate.exists() and candidate.is_dir():
        return candidate
    return None


def _try_load_dotenv_files() -> None:
    """
    Loads env vars from:
      1) repo root .env
      2) client/.env

    Priority (lowest -> highest):
      - repo root .env
      - client/.env

    Already-set environment variables are NOT overridden.
    """
    here = Path(__file__).resolve()
    repo_root = _find_repo_root(here.parent)

    # If we can't find repo root, do nothing (but don't crash).
    if not repo_root:
        return

    client_dir = _find_client_dir(repo_root)

    repo_env = repo_root / ".env"
    client_env = (client_dir / ".env") if client_dir else None

    # Load root first (low priority), then client (high priority)
    for env_path in [repo_env, client_env]:
        if not env_path or not env_path.exists():
            continue
        values = _read_dotenv_file(env_path)
        for k, v in values.items():
            os.environ.setdefault(k, v)


def _try_load_dotenv_files() -> None:
    """
    Loads env vars from:
      - repo root .env
      - client/.env

    Priority (lowest -> highest):
      - repo root .env
      - client/.env

    Already-set environment variables are NOT overridden.
    """
    # This file is: client/src/sdp_client/cli.py
    client_dir = Path(__file__).resolve().parents[2]  # client/
    repo_root = client_dir.parent  # repo root

    repo_env = repo_root / ".env"
    client_env = client_dir / ".env"

    for env_path in (repo_env, client_env):
        values = _read_dotenv_file(env_path)
        for k, v in values.items():
            os.environ.setdefault(k, v)


def _die(msg: str, code: int = 1) -> None:
    print(msg, file=sys.stderr)
    raise SystemExit(code)


def _resolve_tls_verify(args) -> bool | str:
    """
    requests 'verify' resolution order:
      1) --ca-bundle
      2) SDP_CA_BUNDLE env var (recommended)
      3) --insecure-skip-verify -> False
      4) default True (requests will also honor REQUESTS_CA_BUNDLE / CURL_CA_BUNDLE)
    """
    if getattr(args, "ca_bundle", None):
        return args.ca_bundle

    env_ca = os.getenv("SDP_CA_BUNDLE")
    if env_ca:
        return env_ca

    if getattr(args, "insecure_skip_verify", False):
        return False

    return True


def _wait_for_processed(
    *,
    batch_id: str,
    client_id: str,
    server_url: str,
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
    _try_load_dotenv_files()

    default_server_url = os.getenv("SDP_SERVER_URL", "https://localhost:8443")

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
        help="Token type (FPE is currently a stub)",
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
    upload_parser.add_argument("--server-url", default=default_server_url)
    upload_parser.add_argument("--insecure-skip-verify", action="store_true")
    upload_parser.add_argument("--ca-bundle", help="Path to custom CA bundle (PEM)")
    upload_parser.add_argument("--batch-id", help="Optional batch_id to reuse across uploads (UUID)")
    upload_parser.add_argument("--api-key", help="API key (or set SDP_API_KEY env var)")

    # --- fetch-results ---
    fetch_parser = subparsers.add_parser(
        "fetch-results",
        help="Fetch results for a batch and write raw JSON to a file",
    )
    fetch_parser.add_argument("--batch-id", required=True)
    fetch_parser.add_argument("--client-id", required=True)
    fetch_parser.add_argument("--output", "-o", required=True)
    fetch_parser.add_argument("--server-url", default=default_server_url)
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
    integrate_parser.add_argument("--server-url", default=default_server_url)
    integrate_parser.add_argument("--insecure-skip-verify", action="store_true")
    integrate_parser.add_argument("--ca-bundle")
    integrate_parser.add_argument("--api-key")
    integrate_parser.add_argument("--wait", action="store_true")
    integrate_parser.add_argument("--wait-timeout-seconds", type=int, default=120)
    integrate_parser.add_argument("--poll-interval-seconds", type=float, default=1.0)

    # --- process-batch ---
    proc_parser = subparsers.add_parser(
        "process-batch",
        help="Upload -> Process -> Wait -> Integrate results into raw CSV",
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
    proc_parser.add_argument("--server-url", default=default_server_url)
    proc_parser.add_argument("--batch-id")
    proc_parser.add_argument("--api-key")

    proc_parser.add_argument("--use-dev-processor", action="store_true")

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

    try:
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
            server_url = args.server_url

            raw_input = Path(args.raw_input).resolve()
            output_csv = Path(args.output).resolve()

            if args.tokenized_file:
                tokenized_path = Path(args.tokenized_file).resolve()
            else:
                if not args.tokenize_column:
                    _die("You must provide either --tokenized-file OR --tokenize-column.", 2)

                tokenized_path = (
                    Path(args.tokenized_output).resolve()
                    if args.tokenized_output
                    else output_csv.with_suffix(".tokenized.csv")
                )

                tokenize_csv(
                    input_path=str(raw_input),
                    output_path=str(tokenized_path),
                    source_table=args.source_table,
                    column_name=args.tokenize_column,
                    token_type=args.token_type,
                )
                print(f"Tokenized CSV written to: {tokenized_path}")
                print("Local Token Vault updated (SQLite: token_vault.db).")

            upload_resp = upload_batch(
                file_path=str(tokenized_path),
                client_id=args.client_id,
                processing_type=args.processing_type,
                server_url=server_url,
                batch_id=args.batch_id,
                verify_tls=verify,
                api_key=api_key,
            )

            batch_id = upload_resp.get("batch_id") or upload_resp.get("batchId") or upload_resp.get("id")
            if not batch_id:
                _die(f"Upload response did not include batch_id. Response: {upload_resp}", 2)

            print(f"\nBatch uploaded. batch_id={batch_id}\n")

            if args.use_dev_processor:
                _dev_process_batch(batch_id=batch_id, server_url=server_url, verify_tls=verify, api_key=api_key)
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

            results = _wait_for_processed(
                batch_id=batch_id,
                client_id=args.client_id,
                server_url=server_url,
                verify_tls=verify,
                api_key=api_key,
                wait_timeout_seconds=args.wait_timeout_seconds,
                poll_interval_seconds=args.poll_interval_seconds,
            )

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

    except NotImplementedError as e:
        _die(f"ERROR: {e}", 2)
    except Exception as e:
        _die(f"ERROR: {e}", 1)


if __name__ == "__main__":
    main()

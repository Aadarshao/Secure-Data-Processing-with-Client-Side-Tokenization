import argparse
from pathlib import Path

from .tokenization import tokenize_csv, tokenize_csv_with_config
from .config import load_tokenization_config
from .transfer import upload_batch


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="sdp-client",
        description="SDP Client – Tokenization & Transfer Tools",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Existing: tokenize-csv ---
    tokenize_parser = subparsers.add_parser(
        "tokenize-csv",
        help="Tokenize a CSV column and store originals in a local Token Vault",
    )
    tokenize_parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input CSV file",
    )
    tokenize_parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path to output tokenized CSV file",
    )
    tokenize_parser.add_argument(
        "--source-table",
        required=True,
        help="Logical source table name (e.g., customers)",
    )
    tokenize_parser.add_argument(
        "--column",
        "-c",
        required=True,
        help="Column name to tokenize (e.g., email)",
    )
    tokenize_parser.add_argument(
        "--token-type",
        default="HASH",
        choices=["HASH", "FPE", "MASKED", "RANDOM"],
        help="Token type (currently only HASH semantics implemented)",
    )

    # --- Existing: tokenize-config ---
    cfg_parser = subparsers.add_parser(
        "tokenize-config",
        help="Tokenize a CSV using a YAML config file",
    )
    cfg_parser.add_argument(
        "--config",
        "-c",
        required=True,
        help="Path to YAML config file describing tokenization rules",
    )
    cfg_parser.add_argument(
        "--input",
        "-i",
        required=True,
        help="Path to input CSV file",
    )
    cfg_parser.add_argument(
        "--output",
        "-o",
        required=True,
        help="Path to output tokenized CSV file",
    )

    # --- NEW: upload-batch ---
    upload_parser = subparsers.add_parser(
        "upload-batch",
        help="Upload a tokenized CSV file to the ingestion API",
    )
    upload_parser.add_argument(
        "--file",
        "-f",
        required=True,
        help="Path to tokenized CSV file",
    )
    upload_parser.add_argument(
        "--client-id",
        required=True,
        help="Client identifier (e.g., bank_abc)",
    )
    upload_parser.add_argument(
        "--processing-type",
        required=True,
        help="Processing type (e.g., risk_scoring)",
    )
    upload_parser.add_argument(
        "--server-url",
        help="Base URL of ingestion API (default: http://localhost:8081)",
    )
    upload_parser.add_argument(
        "--insecure-skip-verify",
        action="store_true",
        help="Skip TLS certificate verification (DEV ONLY; NOT for production!)",
    )
    upload_parser.add_argument(
        "--ca-bundle",
        help="Path to custom CA bundle to verify TLS (e.g. ca.pem)",
    )
    upload_parser.add_argument(
        "--batch-id",
        help="Optional batch_id to reuse across uploads",
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

    elif args.command == "tokenize-config":
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

    elif args.command == "upload-batch":
        # Decide TLS verification behavior
        if args.ca_bundle:
            verify = args.ca_bundle          # use custom CA bundle
        elif args.insecure_skip_verify:
            verify = False                   # do NOT verify cert (dev only)
        else:
            verify = True                    # normal TLS verification

        result = upload_batch(
            file_path=args.file,
            client_id=args.client_id,
            processing_type=args.processing_type,
            server_url=args.server_url,
            batch_id=args.batch_id,
            verify_tls=verify,
        )
        print("Server response:")
        print(result)

    else:
        parser.print_help()


if __name__ == "__main__":
    main()

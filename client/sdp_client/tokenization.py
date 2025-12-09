import csv
import hashlib
from pathlib import Path
from typing import Dict

from .token_vault_db import insert_token_record
from .config import TokenizationConfig, ColumnRule


def generate_token(value: str) -> str:
    """
    Deterministic token generator based on SHA-256.
    This preserves referential integrity across rows/batches.
    """
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"tk_{digest[:24]}"  # short but unique enough for our purposes


def tokenize_csv(
    input_path: str,
    output_path: str,
    source_table: str,
    column_name: str,
    token_type: str = "HASH",
    db_path: str = None,
) -> None:
    """
    Read a CSV, tokenize a single column, and write a new CSV.
    Also store original values in the local Token Vault (SQLite).
    """
    in_path = Path(input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    # Ensure parent dir for output exists
    if out_path.parent and not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)

    # Process CSV line by line
    with in_path.open("r", newline="", encoding="utf-8") as in_file, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_file:
        reader = csv.DictReader(in_file)
        fieldnames = reader.fieldnames

        if fieldnames is None:
            raise ValueError("Input CSV has no header row")

        if column_name not in fieldnames:
            raise ValueError(f"Column '{column_name}' not found in CSV headers: {fieldnames}")

        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            original_value = row.get(column_name, "")
            if original_value:
                token_value = generate_token(original_value)
                row[column_name] = token_value

                insert_token_record(
                    original_value=original_value,
                    token_value=token_value,
                    token_type=token_type,
                    source_table=source_table,
                    source_column=column_name,
                    batch_id=None,
                    db_path=db_path or "token_vault.db",
                )

            writer.writerow(row)


def tokenize_csv_with_config(
    input_path: str,
    output_path: str,
    config: TokenizationConfig,
    db_path: str = None,
) -> None:
    """
    Tokenize one or more columns based on a YAML config.
    """
    in_path = Path(input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Input file not found: {in_path}")

    if out_path.parent and not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)

    with in_path.open("r", newline="", encoding="utf-8") as in_file, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_file:
        reader = csv.DictReader(in_file)
        fieldnames = reader.fieldnames

        if fieldnames is None:
            raise ValueError("Input CSV has no header row")

        # Check all configured columns exist
        for col in config.columns.keys():
            if col not in fieldnames:
                raise ValueError(
                    f"Configured column '{col}' not found in CSV headers: {fieldnames}"
                )

        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            for col_name, rule in config.columns.items():
                original_value = row.get(col_name, "")
                if not original_value:
                    continue

                if rule.token_type == "HASH":
                    token_value = generate_token(original_value)
                else:
                    # Future: implement FPE, MASKED, RANDOM
                    raise NotImplementedError(
                        f"Token type '{rule.token_type}' not implemented yet"
                    )

                row[col_name] = token_value

                insert_token_record(
                    original_value=original_value,
                    token_value=token_value,
                    token_type=rule.token_type,
                    source_table=config.source_table,
                    source_column=col_name,
                    batch_id=None,
                    db_path=db_path or "token_vault.db",
                )

            writer.writerow(row)

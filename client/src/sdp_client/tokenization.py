import csv
import hashlib
import re
import uuid
from pathlib import Path
from typing import Dict, Optional

from .token_vault_db import insert_token_record
from .config import TokenizationConfig


# -------------------------
# Token Generators
# -------------------------

def generate_token_hash(value: str) -> str:
    """
    Deterministic token generator based on SHA-256.
    Preserves referential integrity across rows/batches.
    """
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return f"tk_{digest[:24]}"  # short but unique enough


def generate_token_random() -> str:
    """
    Non-deterministic token generator. Same input will produce different tokens.
    Useful when referential integrity is NOT required.
    """
    return f"tk_{uuid.uuid4().hex[:24]}"


# -------------------------
# MASKED tokenization
# -------------------------

_EMAIL_RE = re.compile(r"^\s*([^@\s]+)@([^@\s]+\.[^@\s]+)\s*$")
_DIGITS_RE = re.compile(r"\d+")


def _mask_email(value: str) -> str:
    """
    Masks an email while keeping the domain.
    Example: "john.doe@gmail.com" -> "j***@gmail.com"
    """
    m = _EMAIL_RE.match(value)
    if not m:
        return _mask_generic(value)

    local, domain = m.group(1), m.group(2)
    local = local.strip()
    if not local:
        return f"***@{domain}"

    first = local[0]
    return f"{first}***@{domain}"


def _mask_phone_like(value: str) -> str:
    """
    Masks phone-like strings while preserving last 4 digits.
    Example: "+1 (555) 123-4567" -> "***-***-4567"
    """
    digits = "".join(_DIGITS_RE.findall(value))
    if len(digits) <= 4:
        return "*" * len(digits)

    last4 = digits[-4:]
    return f"***-***-{last4}"


def _mask_generic(value: str) -> str:
    """
    Generic masking:
    - if len <= 2: replace all with '*'
    - else: keep first char + stars + keep last char
    Example: "abcd" -> "a**d"
    """
    v = value.strip()
    if not v:
        return v
    if len(v) <= 2:
        return "*" * len(v)
    return v[0] + ("*" * (len(v) - 2)) + v[-1]


def generate_token_masked(value: str) -> str:
    """
    MASKED tokenization: not reversible, but keeps a recognizable shape.
    - emails: mask local part
    - phone-like: keep last 4 digits
    - fallback: generic masking
    """
    v = (value or "").strip()
    if not v:
        return v

    # Email
    if "@" in v:
        return _mask_email(v)

    # Phone-like if it has enough digits
    digits = "".join(_DIGITS_RE.findall(v))
    if len(digits) >= 7:
        return _mask_phone_like(v)

    return _mask_generic(v)


# -------------------------
# FPE stub (optional)
# -------------------------

def generate_token_fpe_stub(value: str) -> str:
    """
    FPE (Format Preserving Encryption) is intentionally NOT implemented here yet.

    Production note:
    - True FPE should use a vetted FF1/FF3 implementation, ideally backed by KMS/HSM.
    - We'll add a proper implementation in a later phase.

    For now we fail fast so it’s obvious in production.
    """
    raise NotImplementedError(
        "FPE tokenization is not implemented yet. Use HASH or MASKED for now."
    )


# -------------------------
# Dispatcher
# -------------------------

def tokenize_value(value: str, token_type: str) -> str:
    token_type = (token_type or "HASH").upper().strip()

    if token_type == "HASH":
        return generate_token_hash(value)

    if token_type == "MASKED":
        return generate_token_masked(value)

    if token_type == "RANDOM":
        return generate_token_random()

    if token_type == "FPE":
        return generate_token_fpe_stub(value)

    raise ValueError(f"Unsupported token_type: {token_type}")


# -------------------------
# CSV Tokenization
# -------------------------

def tokenize_csv(
    input_path: str,
    output_path: str,
    source_table: str,
    column_name: str,
    token_type: str = "HASH",
    db_path: Optional[str] = None,
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
                token_value = tokenize_value(original_value, token_type)
                row[column_name] = token_value

                # Store original in local vault
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
    db_path: Optional[str] = None,
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
                raise ValueError(f"Configured column '{col}' not found in CSV headers: {fieldnames}")

        writer = csv.DictWriter(out_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            for col_name, rule in config.columns.items():
                original_value = row.get(col_name, "")
                if not original_value:
                    continue

                token_value = tokenize_value(original_value, rule.token_type)
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

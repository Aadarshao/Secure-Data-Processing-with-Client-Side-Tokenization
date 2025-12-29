import csv
import json
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


def _norm_key(v: Optional[object]) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""
    try:
        if "." in s:
            f = float(s)
            if f.is_integer():
                return str(int(f))
            return s
        return str(int(s))
    except Exception:
        return s


def _strip_bom(s: str) -> str:
    # Remove UTF-8 BOM if present at start of string
    return s.lstrip("\ufeff")


def _resolve_key_column(fieldnames: List[str], requested: str) -> str:
    """
    Find the actual key column name in the CSV headers, tolerating BOM and case.
    """
    req = _strip_bom(requested).strip()

    # Exact match
    if requested in fieldnames:
        return requested

    # BOM-stripped exact match
    for f in fieldnames:
        if _strip_bom(f) == req:
            return f

    # Case-insensitive match (BOM-safe)
    req_lower = req.lower()
    for f in fieldnames:
        if _strip_bom(f).lower() == req_lower:
            return f

    raise ValueError(
        f"Key column '{requested}' not found in CSV headers. Headers={fieldnames}"
    )


def integrate_results_with_raw_csv(
    raw_input_path: str,
    output_path: str,
    results: Dict[str, Any],
    key_column: str = "customer_id",
) -> None:
    """
    Join server-side results with the local raw CSV on a non-PII key.

    Also writes: <output_path>.audit.json
    """
    in_path = Path(raw_input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Raw input file not found: {in_path}")

    if out_path.parent and not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)

    # Build map: normalized(record_key) -> result fields
    score_map: Dict[str, Dict[str, str]] = {}
    for rec in results.get("records", []) or []:
        nk = _norm_key(rec.get("record_key"))
        if not nk:
            continue
        score_map[nk] = {
            "risk_score": str(rec.get("risk_score", "")),
            "model_version": str(rec.get("model_version", "")),
        }

    rows_total = 0
    rows_matched = 0

    # IMPORTANT: use utf-8-sig so BOM is handled for content; headers may still include BOM,
    # so we also resolve the real key column name.
    with in_path.open("r", newline="", encoding="utf-8-sig") as in_f, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        base_fields: List[str] = reader.fieldnames or []

        if not base_fields:
            raise ValueError("Raw CSV has no header row")

        actual_key_col = _resolve_key_column(base_fields, key_column)

        extra_fields = ["risk_score", "model_version"]
        fieldnames = base_fields + [f for f in extra_fields if f not in base_fields]

        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rows_total += 1

            nk = _norm_key(row.get(actual_key_col))
            if nk:
                info = score_map.get(nk)
                if info:
                    row["risk_score"] = info["risk_score"]
                    row["model_version"] = info["model_version"]
                    rows_matched += 1

            writer.writerow(row)

    audit_path = Path(str(out_path) + ".audit.json")
    audit = {
        "ts_epoch": int(time.time()),
        "raw_input_path": str(in_path),
        "output_path": str(out_path),
        "requested_key_column": key_column,
        "resolved_key_column": actual_key_col,
        "batch_id": results.get("batch_id"),
        "status": results.get("status"),
        "rows_total": rows_total,
        "rows_matched": rows_matched,
        "results_records": len(results.get("records", []) or []),
        "example_result_keys": list(score_map.keys())[:5],
    }
    audit_path.write_text(json.dumps(audit, indent=2), encoding="utf-8")

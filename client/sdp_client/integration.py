import csv
from pathlib import Path
from typing import Any, Dict, List


def integrate_results_with_raw_csv(
    raw_input_path: str,
    output_path: str,
    results: Dict[str, Any],
    key_column: str = "customer_id",
) -> None:
    """
    Join server-side results with the local raw CSV on a non-PII key.

    - raw_input_path: original CSV with PII (never leaves client)
    - results: dict returned by fetch_results(...)
    - key_column: column used to join (e.g., customer_id)
    - output_path: enriched CSV with all raw columns + risk_score + model_version
    """
    in_path = Path(raw_input_path)
    out_path = Path(output_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Raw input file not found: {in_path}")

    if out_path.parent and not out_path.parent.exists():
        out_path.parent.mkdir(parents=True, exist_ok=True)

    # Build a map: record_key -> {risk_score, model_version}
    score_map: Dict[str, Dict[str, str]] = {}
    for rec in results.get("records", []):
        key = rec.get("record_key")
        if key is None:
            continue
        score_map[str(key)] = {
            "risk_score": str(rec.get("risk_score", "")),
            "model_version": str(rec.get("model_version", "")),
        }

    with in_path.open("r", newline="", encoding="utf-8") as in_f, out_path.open(
        "w", newline="", encoding="utf-8"
    ) as out_f:
        reader = csv.DictReader(in_f)
        base_fields: List[str] = reader.fieldnames or []

        extra_fields = ["risk_score", "model_version"]
        # Avoid duplicates if columns already exist
        fieldnames = base_fields + [f for f in extra_fields if f not in base_fields]

        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            key = row.get(key_column)
            if key is not None:
                info = score_map.get(str(key))
                if info:
                    row["risk_score"] = info["risk_score"]
                    row["model_version"] = info["model_version"]

            writer.writerow(row)

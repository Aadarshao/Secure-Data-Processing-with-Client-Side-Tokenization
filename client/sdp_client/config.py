from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Any

import yaml


@dataclass
class ColumnRule:
    token_type: str


@dataclass
class TokenizationConfig:
    source_table: str
    columns: Dict[str, ColumnRule]


def load_tokenization_config(path: str) -> TokenizationConfig:
    """
    Load a YAML config that looks like:

    source_table: customers
    columns:
      email: HASH
      phone:
        token_type: HASH
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")

    with p.open("r", encoding="utf-8") as f:
        data: Any = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError("Tokenization config must be a YAML mapping")

    source_table = data.get("source_table")
    if not source_table:
        raise ValueError("Config missing 'source_table'")

    raw_columns = data.get("columns")
    if not isinstance(raw_columns, dict) or not raw_columns:
        raise ValueError("Config 'columns' must be a non-empty mapping")

    columns: Dict[str, ColumnRule] = {}
    for col_name, col_cfg in raw_columns.items():
        if isinstance(col_cfg, str):
            token_type = col_cfg
        elif isinstance(col_cfg, dict):
            token_type = col_cfg.get("token_type")
            if not token_type:
                raise ValueError(f"Column '{col_name}' missing 'token_type'")
        else:
            raise ValueError(f"Invalid config for column '{col_name}'")

        columns[col_name] = ColumnRule(token_type=token_type)

    return TokenizationConfig(source_table=source_table, columns=columns)

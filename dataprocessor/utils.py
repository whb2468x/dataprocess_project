from __future__ import annotations

from typing import Iterable


def parse_csv_items(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in value.split(",") if item.strip()]


def parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or value == "":
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def parse_mapping(items: Iterable[str] | None, separator: str = ":") -> dict[str, str]:
    mapping: dict[str, str] = {}
    for item in items or []:
        if separator not in item:
            raise ValueError(f"参数 '{item}' 缺少分隔符 '{separator}'")
        key, val = item.split(separator, 1)
        key = key.strip()
        val = val.strip()
        if not key:
            raise ValueError(f"参数 '{item}' 的键不能为空")
        mapping[key] = val
    return mapping


def coerce_scalar(value: str | None):
    if value is None:
        return None
    text = value.strip()
    lowered = text.lower()
    if lowered in {"", "none", "null", "nan"}:
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    try:
        if any(marker in text for marker in [".", "e", "E"]):
            return float(text)
        return int(text)
    except ValueError:
        return text


def parse_fill_values(items: Iterable[str] | None) -> dict[str, str]:
    raw_mapping = parse_mapping(items, separator="=")
    return {key: coerce_scalar(value) for key, value in raw_mapping.items()}


def ensure_columns(df, columns: Iterable[str], dataset_name: str = "数据表") -> None:
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise KeyError(f"{dataset_name} 缺少列: {', '.join(missing)}")


def infer_output_name(base_name: str, suffix: str) -> str:
    return f"{base_name}_{suffix}" if suffix else base_name

from __future__ import annotations

from pathlib import Path

import pandas as pd


TEXT_EXTENSIONS = {".csv", ".tsv", ".txt"}


def _effective_suffix(path: Path) -> str:
    suffixes = path.suffixes
    if not suffixes:
        return ""
    if suffixes[-1] in {".gz", ".bz2", ".xz"} and len(suffixes) >= 2:
        return suffixes[-2].lower()
    return suffixes[-1].lower()


def infer_separator(path: str | Path) -> str:
    file_path = Path(path)
    with file_path.open("rt", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            if "\t" in line:
                return "\t"
            if ";" in line and "," not in line:
                return ";"
            if "|" in line and "," not in line:
                return "|"
            return ","
    return ","


def read_table(path: str | Path, sep: str | None = None, sheet_name: int | str = 0) -> pd.DataFrame:
    file_path = Path(path).expanduser().resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    effective_suffix = _effective_suffix(file_path)
    compression = "infer"

    if effective_suffix in TEXT_EXTENSIONS:
        delimiter = sep or ("\t" if effective_suffix == ".tsv" else infer_separator(file_path))
        return pd.read_csv(file_path, sep=delimiter, low_memory=False, compression=compression)
    if effective_suffix in {".xls", ".xlsx"}:
        return pd.read_excel(file_path, sheet_name=sheet_name)
    if effective_suffix == ".json":
        return pd.read_json(file_path)
    if effective_suffix == ".parquet":
        return pd.read_parquet(file_path)
    if effective_suffix == ".pkl":
        return pd.read_pickle(file_path)

    delimiter = sep or infer_separator(file_path)
    return pd.read_csv(file_path, sep=delimiter, low_memory=False, compression=compression)


def write_table(df: pd.DataFrame, path: str | Path, sep: str | None = None, index: bool = False) -> Path:
    file_path = Path(path).expanduser().resolve()
    file_path.parent.mkdir(parents=True, exist_ok=True)
    effective_suffix = _effective_suffix(file_path)

    if effective_suffix in {".csv", ""}:
        df.to_csv(file_path, index=index, sep=sep or ",")
    elif effective_suffix in {".tsv", ".txt"}:
        df.to_csv(file_path, index=index, sep=sep or "\t")
    elif effective_suffix in {".xls", ".xlsx"}:
        df.to_excel(file_path, index=index)
    elif effective_suffix == ".json":
        df.to_json(file_path, orient="records", force_ascii=False, indent=2)
    elif effective_suffix == ".parquet":
        df.to_parquet(file_path, index=index)
    elif effective_suffix == ".pkl":
        df.to_pickle(file_path)
    else:
        df.to_csv(file_path, index=index, sep=sep or ",")
    return file_path


def preview_text(df: pd.DataFrame, rows: int = 5) -> str:
    if df.empty:
        return "<空数据表>"
    return df.head(rows).to_string(index=False)


from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from ..utils import ensure_columns


def auto_convert_numeric(
    df: pd.DataFrame,
    columns: list[str] | None = None,
    threshold: float = 0.8,
) -> pd.DataFrame:
    result = df.copy()
    target_columns = columns or [col for col in result.columns if result[col].dtype == "object"]
    for col in target_columns:
        sample = result[col].dropna().astype(str).head(100)
        if sample.empty:
            continue
        converted = pd.to_numeric(sample, errors="coerce")
        ratio = converted.notna().mean()
        if ratio >= threshold:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    return result


def profile_table(df: pd.DataFrame) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    total_rows = len(df)
    for col in df.columns:
        series = df[col]
        non_null = int(series.notna().sum())
        null_count = int(series.isna().sum())
        unique_count = int(series.nunique(dropna=True))
        sample_values = " | ".join(series.dropna().astype(str).head(3).tolist())
        record: dict[str, object] = {
            "column": col,
            "dtype": str(series.dtype),
            "non_null": non_null,
            "null_count": null_count,
            "null_rate(%)": round((null_count / total_rows * 100), 2) if total_rows else 0.0,
            "unique_count": unique_count,
            "sample_values": sample_values,
        }
        if pd.api.types.is_numeric_dtype(series):
            record["min"] = series.min()
            record["mean"] = round(float(series.mean()), 6) if non_null else np.nan
            record["max"] = series.max()
            record["std"] = round(float(series.std()), 6) if non_null > 1 else np.nan
        else:
            record["min"] = ""
            record["mean"] = ""
            record["max"] = ""
            record["std"] = ""
        records.append(record)
    return pd.DataFrame(records)


def inspect_table(df: pd.DataFrame, sample_rows: int = 5) -> dict[str, object]:
    profile = profile_table(df)
    numeric_columns = list(df.select_dtypes(include="number").columns)
    return {
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "numeric_columns": numeric_columns,
        "preview": df.head(sample_rows),
        "profile": profile,
    }


def clean_table(
    df: pd.DataFrame,
    strip_column_names: bool = True,
    normalize_column_names: bool = False,
    strip_values: bool = False,
    blank_to_na: bool = True,
    rename_map: dict[str, str] | None = None,
    convert_numeric_columns: list[str] | None = None,
    auto_numeric: bool = False,
    fill_values: dict[str, object] | None = None,
    fill_numeric: float | int | None = None,
    fill_text: str | None = None,
    drop_empty_rows: bool = False,
    drop_empty_columns: bool = False,
    deduplicate_subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    result = df.copy()

    if strip_column_names:
        result.columns = [str(col).strip() for col in result.columns]
    if normalize_column_names:
        result.columns = [str(col).strip().lower().replace(" ", "_") for col in result.columns]
    if rename_map:
        result = result.rename(columns=rename_map)
    if strip_values:
        for col in result.select_dtypes(include=["object", "string"]).columns:
            result[col] = result[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    if blank_to_na:
        text_columns = result.select_dtypes(include=["object", "string"]).columns
        if len(text_columns) > 0:
            result[text_columns] = result[text_columns].replace(r"^\s*$", pd.NA, regex=True)
    if convert_numeric_columns:
        ensure_columns(result, convert_numeric_columns)
        for col in convert_numeric_columns:
            result[col] = pd.to_numeric(result[col], errors="coerce")
    if auto_numeric:
        result = auto_convert_numeric(result)
    if fill_values:
        for col, value in fill_values.items():
            if col in result.columns:
                result[col] = result[col].fillna(value)
    if fill_numeric is not None:
        numeric_cols = result.select_dtypes(include="number").columns
        result[numeric_cols] = result[numeric_cols].fillna(fill_numeric)
    if fill_text is not None:
        text_cols = result.select_dtypes(include=["object", "string"]).columns
        result[text_cols] = result[text_cols].fillna(fill_text)
    if drop_empty_columns:
        result = result.dropna(axis=1, how="all")
    if drop_empty_rows:
        result = result.dropna(axis=0, how="all")
    if deduplicate_subset is not None:
        subset = deduplicate_subset or None
        result = result.drop_duplicates(subset=subset, keep=keep)
    return result


def deduplicate_rows(
    df: pd.DataFrame,
    subset: list[str] | None = None,
    keep: str = "first",
) -> pd.DataFrame:
    if subset:
        ensure_columns(df, subset)
    return df.drop_duplicates(subset=subset or None, keep=keep).copy()


def filter_table(
    df: pd.DataFrame,
    query: str | None = None,
    include_columns: list[str] | None = None,
    exclude_columns: list[str] | None = None,
    sort_by: list[str] | None = None,
    ascending: bool = True,
    head: int | None = None,
) -> pd.DataFrame:
    result = df.copy()
    if query:
        result = result.query(query, engine="python")
    if include_columns:
        ensure_columns(result, include_columns)
        result = result[include_columns]
    if exclude_columns:
        ensure_columns(result, exclude_columns)
        result = result.drop(columns=exclude_columns)
    if sort_by:
        ensure_columns(result, sort_by)
        result = result.sort_values(by=sort_by, ascending=ascending)
    if head is not None:
        result = result.head(head)
    return result


def derive_columns(df: pd.DataFrame, expressions: dict[str, str]) -> pd.DataFrame:
    result = df.copy()
    local_dict = {"np": np}
    for new_col, expression in expressions.items():
        result[new_col] = result.eval(expression, engine="python", local_dict=local_dict)
    return result


def melt_table(
    df: pd.DataFrame,
    id_vars: list[str],
    value_vars: list[str],
    var_name: str = "variable",
    value_name: str = "value",
) -> pd.DataFrame:
    ensure_columns(df, [*id_vars, *value_vars])
    return df.melt(id_vars=id_vars, value_vars=value_vars, var_name=var_name, value_name=value_name)


def pivot_table_data(
    df: pd.DataFrame,
    index: list[str],
    columns: str,
    values: str,
    aggfunc: str = "sum",
    fill_value: object | None = None,
) -> pd.DataFrame:
    ensure_columns(df, [*index, columns, values])
    result = pd.pivot_table(
        df,
        index=index,
        columns=columns,
        values=values,
        aggfunc=aggfunc,
        fill_value=fill_value,
        dropna=False,
    )
    return result.reset_index()


def split_column(
    df: pd.DataFrame,
    column: str,
    separator: str = ";",
    new_columns: list[str] | None = None,
    max_splits: int = -1,
    keep_original: bool = True,
    prefix: str = "level",
) -> pd.DataFrame:
    ensure_columns(df, [column])
    result = df.copy()
    parts = result[column].fillna("").astype(str).str.split(separator, n=max_splits, expand=True)
    if new_columns:
        if len(new_columns) < parts.shape[1]:
            raise ValueError("提供的新列名数量不足以承载拆分结果")
        parts.columns = new_columns[: parts.shape[1]]
    else:
        parts.columns = [f"{prefix}_{idx + 1}" for idx in range(parts.shape[1])]
    if keep_original:
        return pd.concat([result, parts], axis=1)
    result = result.drop(columns=[column])
    return pd.concat([result, parts], axis=1)


def explode_column(
    df: pd.DataFrame,
    column: str,
    separator: str = ";",
    strip_items: bool = True,
    drop_empty: bool = True,
) -> pd.DataFrame:
    ensure_columns(df, [column])
    result = df.copy()
    result[column] = result[column].fillna("").astype(str).str.split(separator)
    result = result.explode(column)
    if strip_items:
        result[column] = result[column].map(lambda x: x.strip() if isinstance(x, str) else x)
    if drop_empty:
        result = result[result[column].notna() & (result[column] != "")]
    return result.reset_index(drop=True)


def concat_tables(
    tables: Iterable[pd.DataFrame],
    axis: int = 0,
    join: str = "outer",
    add_source: bool = False,
    source_names: list[str] | None = None,
) -> pd.DataFrame:
    frames = [table.copy() for table in tables]
    if add_source and axis == 0:
        source_names = source_names or [f"table_{idx + 1}" for idx in range(len(frames))]
        if len(source_names) != len(frames):
            raise ValueError("source_names 数量必须与输入表数量一致")
        tagged_frames = []
        for name, frame in zip(source_names, frames, strict=False):
            tagged = frame.copy()
            tagged["_source"] = name
            tagged_frames.append(tagged)
        frames = tagged_frames
    return pd.concat(frames, axis=axis, join=join, ignore_index=(axis == 0))


from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd

from ..utils import ensure_columns


SUPPORTED_AGGS = {
    "count",
    "nunique",
    "duplicate_count",
    "sum",
    "mean",
    "min",
    "max",
    "std",
    "median",
    "first",
    "last",
    "row_count",
    "size",
}
NUMERIC_ONLY_AGGS = {"sum", "mean", "min", "max", "std", "median"}


def _prepare_grouping(df: pd.DataFrame, group_cols: list[str] | None) -> tuple[pd.DataFrame, list[str], bool]:
    if group_cols:
        ensure_columns(df, group_cols)
        return df.copy(), group_cols, False
    result = df.copy()
    result["__all__"] = "ALL"
    return result, ["__all__"], True


def _merge_on_groups(base: pd.DataFrame, other: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    return pd.merge(base, other, on=group_cols, how="left")


def aggregate_table(
    df: pd.DataFrame,
    group_cols: list[str] | None = None,
    value_cols: list[str] | None = None,
    aggfuncs: Iterable[str] | None = None,
    include_group_size: bool = True,
) -> pd.DataFrame:
    working, group_cols, drop_dummy = _prepare_grouping(df, group_cols)
    value_cols = value_cols or [col for col in working.columns if col not in group_cols]
    ensure_columns(working, value_cols)
    aggfuncs = [agg.lower() for agg in (aggfuncs or ["count"])]

    invalid = [agg for agg in aggfuncs if agg not in SUPPORTED_AGGS]
    if invalid:
        raise ValueError(f"不支持的聚合函数: {', '.join(invalid)}")

    grouped = working.groupby(group_cols, dropna=False)
    result = grouped.size().reset_index(name="row_count") if include_group_size else working[group_cols].drop_duplicates()

    for col in value_cols:
        for agg in aggfuncs:
            if agg in {"row_count", "size"}:
                continue
            if agg in NUMERIC_ONLY_AGGS and not pd.api.types.is_numeric_dtype(working[col]):
                continue
            if agg == "duplicate_count":
                temp = grouped[col].apply(lambda x: x.dropna().shape[0] - x.dropna().nunique()).reset_index(
                    name=f"{col}_duplicate_count"
                )
            else:
                temp = getattr(grouped[col], agg)().reset_index(name=f"{col}_{agg}")
            result = _merge_on_groups(result, temp, group_cols)

    if drop_dummy:
        result = result.drop(columns=["__all__"])
    return result


def multivalue_statistics(
    df: pd.DataFrame,
    group_cols: list[str] | None,
    value_cols: list[str],
    separator: str = ";",
) -> pd.DataFrame:
    working, group_cols, drop_dummy = _prepare_grouping(df, group_cols)
    ensure_columns(working, value_cols)

    results: list[pd.DataFrame] = []
    for col in value_cols:
        exploded = working[group_cols + [col]].copy()
        exploded[col] = exploded[col].fillna("").astype(str).str.split(separator)
        exploded = exploded.explode(col)
        exploded[col] = exploded[col].map(lambda x: x.strip() if isinstance(x, str) else x)
        exploded = exploded[exploded[col].notna() & (exploded[col] != "")]

        unique_stats = exploded.groupby(group_cols, dropna=False)[col].nunique().reset_index(name=f"{col}_unique_count")
        total_stats = exploded.groupby(group_cols, dropna=False).size().reset_index(name=f"{col}_total_occurrences")
        merged = pd.merge(unique_stats, total_stats, on=group_cols, how="outer")
        merged[f"{col}_duplicate_count"] = merged[f"{col}_total_occurrences"] - merged[f"{col}_unique_count"]
        merged[f"{col}_duplicate_rate(%)"] = np.where(
            merged[f"{col}_total_occurrences"] > 0,
            (merged[f"{col}_duplicate_count"] / merged[f"{col}_total_occurrences"] * 100).round(2),
            0,
        )
        results.append(merged)

    final = results[0]
    for table in results[1:]:
        final = pd.merge(final, table, on=group_cols, how="outer")
    if drop_dummy:
        final = final.drop(columns=["__all__"])
    return final


def crosstab_table(
    df: pd.DataFrame,
    index_col: str,
    column_col: str,
    value_col: str | None = None,
    aggfunc: str = "count",
    margins: bool = True,
    normalize: str | None = None,
    fill_value: object | None = 0,
) -> pd.DataFrame:
    ensure_columns(df, [index_col, column_col])
    if value_col:
        ensure_columns(df, [value_col])
        result = pd.crosstab(
            index=df[index_col],
            columns=df[column_col],
            values=df[value_col],
            aggfunc=aggfunc,
            margins=margins,
            margins_name="总计",
            normalize=normalize,
        )
    else:
        result = pd.crosstab(
            index=df[index_col],
            columns=df[column_col],
            margins=margins,
            margins_name="总计",
            normalize=normalize,
        )
    if fill_value is not None:
        result = result.fillna(fill_value)
    return result.reset_index()


def normalize_columns(
    df: pd.DataFrame,
    columns: list[str],
    method: str = "zscore",
    axis: int = 0,
    pseudocount: float = 1e-9,
    scale: float = 1.0,
) -> pd.DataFrame:
    ensure_columns(df, columns)
    result = df.copy()
    numeric = result[columns].apply(pd.to_numeric, errors="coerce")
    axis = 0 if axis in {0, "column", "columns"} else 1
    method = method.lower()

    if method == "relative":
        totals = numeric.sum(axis=axis).replace(0, np.nan)
        result[columns] = numeric.div(totals, axis=1 - axis) * scale
    elif method == "cpm":
        totals = numeric.sum(axis=axis).replace(0, np.nan)
        result[columns] = numeric.div(totals, axis=1 - axis) * 1_000_000
    elif method == "log2":
        result[columns] = np.log2(numeric + pseudocount)
    elif method == "log10":
        result[columns] = np.log10(numeric + pseudocount)
    elif method == "zscore":
        if axis == 0:
            result[columns] = (numeric - numeric.mean()) / numeric.std(ddof=0).replace(0, np.nan)
        else:
            means = numeric.mean(axis=1)
            stds = numeric.std(axis=1, ddof=0).replace(0, np.nan)
            result[columns] = numeric.sub(means, axis=0).div(stds, axis=0)
    elif method == "minmax":
        if axis == 0:
            result[columns] = (numeric - numeric.min()) / (numeric.max() - numeric.min()).replace(0, np.nan)
        else:
            mins = numeric.min(axis=1)
            maxs = numeric.max(axis=1)
            result[columns] = numeric.sub(mins, axis=0).div((maxs - mins).replace(0, np.nan), axis=0)
    else:
        raise ValueError("method 仅支持 relative、cpm、log2、log10、zscore、minmax")
    return result


def top_n(
    df: pd.DataFrame,
    sort_col: str,
    n: int = 10,
    group_cols: list[str] | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    ensure_columns(df, [sort_col])
    result = df.sort_values(sort_col, ascending=ascending)
    if group_cols:
        ensure_columns(result, group_cols)
        result = result.groupby(group_cols, dropna=False, group_keys=False).head(n)
    else:
        result = result.head(n)
    return result.reset_index(drop=True)


def compare_groups(
    df: pd.DataFrame,
    group_col: str,
    value_col: str,
    case_group: str,
    control_group: str,
    feature_col: str | None = None,
    aggfunc: str = "mean",
    pseudocount: float = 1e-9,
) -> pd.DataFrame:
    ensure_columns(df, [group_col, value_col])
    subset = df[df[group_col].isin([case_group, control_group])].copy()

    if feature_col:
        ensure_columns(subset, [feature_col])
        pivot = subset.pivot_table(
            index=feature_col,
            columns=group_col,
            values=value_col,
            aggfunc=aggfunc,
            fill_value=0,
            dropna=False,
        )
        pivot = pivot.reindex(columns=[case_group, control_group], fill_value=0)
        result = pivot.reset_index().rename(columns={case_group: "case_value", control_group: "control_value"})
    else:
        case_series = subset.loc[subset[group_col] == case_group, value_col]
        control_series = subset.loc[subset[group_col] == control_group, value_col]
        case_value = getattr(case_series, aggfunc)()
        control_value = getattr(control_series, aggfunc)()
        result = pd.DataFrame([{"case_value": case_value, "control_value": control_value}])

    result["difference"] = result["case_value"] - result["control_value"]
    result["fold_change"] = (result["case_value"] + pseudocount) / (result["control_value"] + pseudocount)
    result["log2_fold_change"] = np.log2(result["fold_change"])
    return result


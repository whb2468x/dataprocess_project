from __future__ import annotations

import pandas as pd

from ..utils import ensure_columns


def join_tables(
    left: pd.DataFrame,
    right: pd.DataFrame,
    left_on: str,
    right_on: str,
    how: str = "inner",
    keep_columns: list[str] | None = None,
    suffixes: tuple[str, str] = ("_left", "_right"),
) -> pd.DataFrame:
    ensure_columns(left, [left_on], dataset_name="左表")
    ensure_columns(right, [right_on], dataset_name="右表")
    result = pd.merge(left, right, left_on=left_on, right_on=right_on, how=how, suffixes=suffixes)
    if keep_columns:
        ensure_columns(result, keep_columns, dataset_name="关联结果")
        result = result[keep_columns]
    return result


def build_matrix(
    df: pd.DataFrame,
    group_col: str,
    feature_col: str,
    value_col: str | None = None,
    aggfunc: str = "sum",
    fill_value: object | None = None,
    reference_df: pd.DataFrame | None = None,
    reference_col: str | None = None,
) -> pd.DataFrame:
    required = [group_col, feature_col]
    if value_col:
        required.append(value_col)
    ensure_columns(df, required)

    working = df.copy()
    ordered_features = None
    if reference_df is not None and reference_col:
        ensure_columns(reference_df, [reference_col], dataset_name="参考表")
        ordered_features = list(reference_df[reference_col].dropna().unique())
        working = working[working[feature_col].isin(ordered_features)]

    if value_col:
        matrix = working.pivot_table(
            index=group_col,
            columns=feature_col,
            values=value_col,
            aggfunc=aggfunc,
            fill_value=fill_value,
            dropna=False,
        )
    else:
        matrix = (
            working.assign(__value__=1)
            .pivot_table(
                index=group_col,
                columns=feature_col,
                values="__value__",
                aggfunc="sum",
                fill_value=fill_value,
                dropna=False,
            )
        )
    if ordered_features is not None:
        matrix = matrix.reindex(columns=ordered_features, fill_value=fill_value)
    return matrix.reset_index()

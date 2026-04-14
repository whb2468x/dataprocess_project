from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .io_utils import read_table, write_table
from .operations import (
    aggregate_table,
    build_matrix,
    clean_table,
    compare_groups,
    concat_tables,
    crosstab_table,
    deduplicate_rows,
    derive_columns,
    explode_column,
    filter_table,
    join_tables,
    melt_table,
    multivalue_statistics,
    normalize_columns,
    pivot_table_data,
    split_column,
    top_n,
)
from .session import DataSession


def _resolve_path(base_dir: Path, raw_path: str) -> Path:
    path = Path(raw_path).expanduser()
    if path.is_absolute():
        return path
    return (base_dir / path).resolve()


def _single_input_step(session: DataSession, step: dict[str, Any]):
    input_name = step["input"]
    return session.get(input_name)


def run_pipeline(config_path: str | Path) -> DataSession:
    config_file = Path(config_path).expanduser().resolve()
    base_dir = config_file.parent
    config = json.loads(config_file.read_text(encoding="utf-8"))
    session = DataSession()

    for name, path in config.get("datasets", {}).items():
        session.add(name, read_table(_resolve_path(base_dir, path)))

    for idx, step in enumerate(config.get("steps", []), start=1):
        operation = step.get("operation")
        if not operation:
            raise ValueError(f"第 {idx} 个步骤缺少 operation")
        params = step.get("params", {})
        output_name = step.get("output")

        if operation == "load":
            dataset_name = step["name"]
            table = read_table(_resolve_path(base_dir, step["path"]), sep=step.get("sep"))
            session.add(dataset_name, table)
            continue

        if operation in {"save", "export"}:
            table = session.get(step["input"])
            write_table(table, _resolve_path(base_dir, step["path"]))
            continue

        if operation == "concat":
            input_names = step["inputs"]
            tables = [session.get(name) for name in input_names]
            result = concat_tables(
                tables,
                axis=params.get("axis", 0),
                join=params.get("join", "outer"),
                add_source=params.get("add_source", False),
                source_names=input_names,
            )
        elif operation == "join":
            left = session.get(step["left"])
            right = session.get(step["right"])
            result = join_tables(left, right, **params)
        elif operation == "matrix":
            main_df = session.get(step["input"])
            reference_name = step.get("reference_input")
            reference_df = session.get(reference_name) if reference_name else None
            result = build_matrix(main_df, reference_df=reference_df, **params)
        else:
            table = _single_input_step(session, step)
            operation_map = {
                "clean": clean_table,
                "filter": filter_table,
                "derive": derive_columns,
                "aggregate": aggregate_table,
                "multivalue_stats": multivalue_statistics,
                "crosstab": crosstab_table,
                "normalize": normalize_columns,
                "melt": melt_table,
                "pivot": pivot_table_data,
                "split": split_column,
                "explode": explode_column,
                "deduplicate": deduplicate_rows,
                "topn": top_n,
                "compare": compare_groups,
            }
            if operation not in operation_map:
                raise ValueError(f"不支持的流水线操作: {operation}")
            result = operation_map[operation](table, **params)

        if output_name:
            session.add(output_name, result)

    for export in config.get("exports", []):
        table = session.get(export["dataset"])
        write_table(table, _resolve_path(base_dir, export["path"]))

    return session

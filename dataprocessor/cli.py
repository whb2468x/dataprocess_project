from __future__ import annotations

import argparse
import sys

from .io_utils import preview_text, read_table, write_table
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
    inspect_table,
    join_tables,
    melt_table,
    multivalue_statistics,
    normalize_columns,
    pivot_table_data,
    profile_table,
    split_column,
    top_n,
)
from .pipeline import run_pipeline
from .utils import coerce_scalar, parse_csv_items, parse_fill_values, parse_mapping


def _load_input(path: str, sep: str | None = None):
    return read_table(path, sep=sep)


def _save_or_preview(df, output: str | None, label: str = "结果") -> None:
    print(f"{label}: {len(df)} 行 x {len(df.columns)} 列")
    print(preview_text(df))
    if output:
        saved_path = write_table(df, output)
        print(f"\n已保存到: {saved_path}")


def _parse_expressions(items: list[str] | None) -> dict[str, str]:
    expressions: dict[str, str] = {}
    for item in items or []:
        if "=" not in item:
            raise ValueError(f"表达式 '{item}' 格式错误，应为 新列=表达式")
        new_col, expression = item.split("=", 1)
        expressions[new_col.strip()] = expression.strip()
    return expressions


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="dataprocess",
        description="模块化数据处理软件：支持交互模式、命令行子命令与批处理流水线。",
    )
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("interactive", help="进入交互模式")

    inspect_parser = subparsers.add_parser("inspect", help="查看数据概览")
    inspect_parser.add_argument("-i", "--input", required=True, help="输入文件路径")
    inspect_parser.add_argument("--sep", help="手动指定分隔符")
    inspect_parser.add_argument("--sample-rows", type=int, default=5, help="预览行数")

    profile_parser = subparsers.add_parser("profile", help="生成列级画像统计表")
    profile_parser.add_argument("-i", "--input", required=True)
    profile_parser.add_argument("-o", "--output")
    profile_parser.add_argument("--sep")

    clean_parser = subparsers.add_parser("clean", help="清洗数据表")
    clean_parser.add_argument("-i", "--input", required=True)
    clean_parser.add_argument("-o", "--output")
    clean_parser.add_argument("--sep")
    clean_parser.add_argument("--normalize-column-names", action="store_true", help="列名转为小写并把空格替换为下划线")
    clean_parser.add_argument("--strip-values", action="store_true", help="去除文本值两端空格")
    clean_parser.add_argument("--no-blank-to-na", action="store_true", help="不要把空白字符串转换为缺失值")
    clean_parser.add_argument("--rename", action="append", help="重命名列，格式 old:new")
    clean_parser.add_argument("--convert-numeric", help="要强制转为数值的列，逗号分隔")
    clean_parser.add_argument("--auto-numeric", action="store_true", help="自动检测并转换数值列")
    clean_parser.add_argument("--fill", action="append", help="按列填充缺失值，格式 col=value")
    clean_parser.add_argument("--fill-numeric", type=float)
    clean_parser.add_argument("--fill-text")
    clean_parser.add_argument("--drop-empty-rows", action="store_true")
    clean_parser.add_argument("--drop-empty-columns", action="store_true")
    clean_parser.add_argument("--deduplicate-subset", help="去重依据列，逗号分隔；留空表示整行去重")
    clean_parser.add_argument("--keep", default="first", choices=["first", "last", "False"])

    filter_parser = subparsers.add_parser("filter", help="筛选、排序、裁剪列")
    filter_parser.add_argument("-i", "--input", required=True)
    filter_parser.add_argument("-o", "--output")
    filter_parser.add_argument("--sep")
    filter_parser.add_argument("--query", help="pandas query 表达式")
    filter_parser.add_argument("--include", help="保留列，逗号分隔")
    filter_parser.add_argument("--exclude", help="删除列，逗号分隔")
    filter_parser.add_argument("--sort-by", help="排序列，逗号分隔")
    filter_parser.add_argument("--ascending", action="store_true")
    filter_parser.add_argument("--head", type=int, help="只保留前 N 行")

    derive_parser = subparsers.add_parser("derive", help="按表达式新增列")
    derive_parser.add_argument("-i", "--input", required=True)
    derive_parser.add_argument("-o", "--output")
    derive_parser.add_argument("--sep")
    derive_parser.add_argument("--expr", action="append", required=True, help="新列表达式，格式 new_col=表达式")

    aggregate_parser = subparsers.add_parser("aggregate", help="分组聚合统计")
    aggregate_parser.add_argument("-i", "--input", required=True)
    aggregate_parser.add_argument("-o", "--output")
    aggregate_parser.add_argument("--sep")
    aggregate_parser.add_argument("--group-cols", help="分组列，逗号分隔；留空表示全表汇总")
    aggregate_parser.add_argument("--value-cols", help="统计列，逗号分隔")
    aggregate_parser.add_argument("--agg", default="count", help="聚合函数，逗号分隔")
    aggregate_parser.add_argument("--no-group-size", action="store_true", help="不输出 row_count")

    multivalue_parser = subparsers.add_parser("multivalue", help="统计分隔值列")
    multivalue_parser.add_argument("-i", "--input", required=True)
    multivalue_parser.add_argument("-o", "--output")
    multivalue_parser.add_argument("--sep")
    multivalue_parser.add_argument("--group-cols", help="分组列，逗号分隔；留空表示全表汇总")
    multivalue_parser.add_argument("--value-cols", required=True, help="分隔值列，逗号分隔")
    multivalue_parser.add_argument("--separator", default=";")

    crosstab_parser = subparsers.add_parser("crosstab", help="交叉表统计")
    crosstab_parser.add_argument("-i", "--input", required=True)
    crosstab_parser.add_argument("-o", "--output")
    crosstab_parser.add_argument("--sep")
    crosstab_parser.add_argument("--index-col", required=True)
    crosstab_parser.add_argument("--column-col", required=True)
    crosstab_parser.add_argument("--value-col")
    crosstab_parser.add_argument("--agg", default="count")
    crosstab_parser.add_argument("--normalize", choices=["all", "index", "columns"])
    crosstab_parser.add_argument("--no-margins", action="store_true")

    join_parser = subparsers.add_parser("join", help="双表关联，可选关联后聚合")
    join_parser.add_argument("--left", required=True, help="左表路径")
    join_parser.add_argument("--right", required=True, help="右表路径")
    join_parser.add_argument("-o", "--output")
    join_parser.add_argument("--left-on", required=True)
    join_parser.add_argument("--right-on", required=True)
    join_parser.add_argument("--how", default="inner", choices=["inner", "left", "right", "outer"])
    join_parser.add_argument("--keep-columns", help="关联后保留的列，逗号分隔")
    join_parser.add_argument("--post-group-cols", help="关联后继续分组聚合的列")
    join_parser.add_argument("--post-value-cols", help="关联后聚合的值列")
    join_parser.add_argument("--post-agg", default="count")

    matrix_parser = subparsers.add_parser("matrix", help="构建矩阵/透视表")
    matrix_parser.add_argument("-i", "--input", required=True, help="主表路径")
    matrix_parser.add_argument("-r", "--reference", help="参考表路径")
    matrix_parser.add_argument("-o", "--output")
    matrix_parser.add_argument("--group-col", required=True)
    matrix_parser.add_argument("--feature-col", required=True)
    matrix_parser.add_argument("--value-col", help="值列；不填则按出现次数计数")
    matrix_parser.add_argument("--reference-col", help="参考表中的特征列")
    matrix_parser.add_argument("--agg", default="sum")
    matrix_parser.add_argument("--fill-value")

    melt_parser = subparsers.add_parser("melt", help="宽表转长表")
    melt_parser.add_argument("-i", "--input", required=True)
    melt_parser.add_argument("-o", "--output")
    melt_parser.add_argument("--sep")
    melt_parser.add_argument("--id-vars", required=True)
    melt_parser.add_argument("--value-vars", required=True)
    melt_parser.add_argument("--var-name", default="variable")
    melt_parser.add_argument("--value-name", default="value")

    pivot_parser = subparsers.add_parser("pivot", help="长表转宽表")
    pivot_parser.add_argument("-i", "--input", required=True)
    pivot_parser.add_argument("-o", "--output")
    pivot_parser.add_argument("--sep")
    pivot_parser.add_argument("--index-cols", required=True)
    pivot_parser.add_argument("--columns-col", required=True)
    pivot_parser.add_argument("--values-col", required=True)
    pivot_parser.add_argument("--agg", default="sum")
    pivot_parser.add_argument("--fill-value")

    normalize_parser = subparsers.add_parser("normalize", help="数值列归一化/变换")
    normalize_parser.add_argument("-i", "--input", required=True)
    normalize_parser.add_argument("-o", "--output")
    normalize_parser.add_argument("--sep")
    normalize_parser.add_argument("--columns", required=True)
    normalize_parser.add_argument("--method", default="zscore", choices=["relative", "cpm", "log2", "log10", "zscore", "minmax"])
    normalize_parser.add_argument("--axis", default="column", choices=["row", "column"])
    normalize_parser.add_argument("--pseudocount", type=float, default=1e-9)
    normalize_parser.add_argument("--scale", type=float, default=1.0)

    topn_parser = subparsers.add_parser("topn", help="取全表或分组 Top N")
    topn_parser.add_argument("-i", "--input", required=True)
    topn_parser.add_argument("-o", "--output")
    topn_parser.add_argument("--sep")
    topn_parser.add_argument("--sort-col", required=True)
    topn_parser.add_argument("-n", type=int, default=10)
    topn_parser.add_argument("--group-cols", help="分组列，逗号分隔")
    topn_parser.add_argument("--ascending", action="store_true")

    compare_parser = subparsers.add_parser("compare", help="比较两个组的差异")
    compare_parser.add_argument("-i", "--input", required=True)
    compare_parser.add_argument("-o", "--output")
    compare_parser.add_argument("--sep")
    compare_parser.add_argument("--group-col", required=True)
    compare_parser.add_argument("--value-col", required=True)
    compare_parser.add_argument("--case-group", required=True)
    compare_parser.add_argument("--control-group", required=True)
    compare_parser.add_argument("--feature-col")
    compare_parser.add_argument("--agg", default="mean")
    compare_parser.add_argument("--pseudocount", type=float, default=1e-9)

    split_parser = subparsers.add_parser("split", help="拆分层级/组合字段")
    split_parser.add_argument("-i", "--input", required=True)
    split_parser.add_argument("-o", "--output")
    split_parser.add_argument("--sep")
    split_parser.add_argument("--column", required=True)
    split_parser.add_argument("--separator", default=";")
    split_parser.add_argument("--new-columns", help="新列名，逗号分隔")
    split_parser.add_argument("--prefix", default="level")
    split_parser.add_argument("--drop-original", action="store_true")

    explode_parser = subparsers.add_parser("explode", help="按分隔符展开一列")
    explode_parser.add_argument("-i", "--input", required=True)
    explode_parser.add_argument("-o", "--output")
    explode_parser.add_argument("--sep")
    explode_parser.add_argument("--column", required=True)
    explode_parser.add_argument("--separator", default=";")
    explode_parser.add_argument("--keep-empty", action="store_true")

    dedup_parser = subparsers.add_parser("deduplicate", help="去重")
    dedup_parser.add_argument("-i", "--input", required=True)
    dedup_parser.add_argument("-o", "--output")
    dedup_parser.add_argument("--sep")
    dedup_parser.add_argument("--subset", help="去重依据列，逗号分隔")
    dedup_parser.add_argument("--keep", default="first", choices=["first", "last", "False"])

    concat_parser = subparsers.add_parser("concat", help="合并多个数据表")
    concat_parser.add_argument("inputs", nargs="+", help="多个输入文件")
    concat_parser.add_argument("-o", "--output")
    concat_parser.add_argument("--axis", default="rows", choices=["rows", "columns"])
    concat_parser.add_argument("--join", default="outer", choices=["outer", "inner"])
    concat_parser.add_argument("--add-source", action="store_true")

    pipeline_parser = subparsers.add_parser("pipeline", help="运行 JSON 批处理流水线")
    pipeline_parser.add_argument("-c", "--config", required=True, help="流水线 JSON 配置文件")

    return parser


def handle_inspect(args) -> int:
    df = _load_input(args.input, args.sep)
    summary = inspect_table(df, sample_rows=args.sample_rows)
    print(f"数据概览: {summary['rows']} 行 x {summary['columns']} 列")
    print("列名:", ", ".join(map(str, summary["column_names"])))
    print("\n预览:")
    print(summary["preview"].to_string(index=False))
    print("\n列画像:")
    print(summary["profile"].to_string(index=False))
    return 0


def handle_profile(args) -> int:
    df = _load_input(args.input, args.sep)
    result = profile_table(df)
    _save_or_preview(result, args.output, label="列画像")
    return 0


def handle_clean(args) -> int:
    df = _load_input(args.input, args.sep)
    keep = False if args.keep == "False" else args.keep
    result = clean_table(
        df,
        normalize_column_names=args.normalize_column_names,
        strip_values=args.strip_values,
        blank_to_na=not args.no_blank_to_na,
        rename_map=parse_mapping(args.rename),
        convert_numeric_columns=parse_csv_items(args.convert_numeric),
        auto_numeric=args.auto_numeric,
        fill_values=parse_fill_values(args.fill),
        fill_numeric=args.fill_numeric,
        fill_text=args.fill_text,
        drop_empty_rows=args.drop_empty_rows,
        drop_empty_columns=args.drop_empty_columns,
        deduplicate_subset=parse_csv_items(args.deduplicate_subset),
        keep=keep,
    )
    _save_or_preview(result, args.output, label="清洗结果")
    return 0


def handle_filter(args) -> int:
    df = _load_input(args.input, args.sep)
    result = filter_table(
        df,
        query=args.query,
        include_columns=parse_csv_items(args.include),
        exclude_columns=parse_csv_items(args.exclude),
        sort_by=parse_csv_items(args.sort_by),
        ascending=args.ascending,
        head=args.head,
    )
    _save_or_preview(result, args.output, label="筛选结果")
    return 0


def handle_derive(args) -> int:
    df = _load_input(args.input, args.sep)
    result = derive_columns(df, _parse_expressions(args.expr))
    _save_or_preview(result, args.output, label="派生列结果")
    return 0


def handle_aggregate(args) -> int:
    df = _load_input(args.input, args.sep)
    result = aggregate_table(
        df,
        group_cols=parse_csv_items(args.group_cols),
        value_cols=parse_csv_items(args.value_cols),
        aggfuncs=parse_csv_items(args.agg),
        include_group_size=not args.no_group_size,
    )
    _save_or_preview(result, args.output, label="聚合结果")
    return 0


def handle_multivalue(args) -> int:
    df = _load_input(args.input, args.sep)
    result = multivalue_statistics(
        df,
        group_cols=parse_csv_items(args.group_cols),
        value_cols=parse_csv_items(args.value_cols),
        separator=args.separator,
    )
    _save_or_preview(result, args.output, label="分隔值统计结果")
    return 0


def handle_crosstab(args) -> int:
    df = _load_input(args.input, args.sep)
    result = crosstab_table(
        df,
        index_col=args.index_col,
        column_col=args.column_col,
        value_col=args.value_col,
        aggfunc=args.agg,
        margins=not args.no_margins,
        normalize=args.normalize,
    )
    _save_or_preview(result, args.output, label="交叉表结果")
    return 0


def handle_join(args) -> int:
    left = _load_input(args.left)
    right = _load_input(args.right)
    result = join_tables(
        left,
        right,
        left_on=args.left_on,
        right_on=args.right_on,
        how=args.how,
        keep_columns=parse_csv_items(args.keep_columns),
    )
    if args.post_group_cols or args.post_value_cols:
        result = aggregate_table(
            result,
            group_cols=parse_csv_items(args.post_group_cols),
            value_cols=parse_csv_items(args.post_value_cols),
            aggfuncs=parse_csv_items(args.post_agg),
        )
    _save_or_preview(result, args.output, label="关联结果")
    return 0


def handle_matrix(args) -> int:
    df = _load_input(args.input)
    reference_df = _load_input(args.reference) if args.reference else None
    fill_value = args.fill_value
    result = build_matrix(
        df,
        group_col=args.group_col,
        feature_col=args.feature_col,
        value_col=args.value_col,
        aggfunc=args.agg,
        fill_value=coerce_scalar(fill_value),
        reference_df=reference_df,
        reference_col=args.reference_col,
    )
    _save_or_preview(result, args.output, label="矩阵结果")
    return 0


def handle_melt(args) -> int:
    df = _load_input(args.input, args.sep)
    result = melt_table(
        df,
        id_vars=parse_csv_items(args.id_vars),
        value_vars=parse_csv_items(args.value_vars),
        var_name=args.var_name,
        value_name=args.value_name,
    )
    _save_or_preview(result, args.output, label="长表结果")
    return 0


def handle_pivot(args) -> int:
    df = _load_input(args.input, args.sep)
    result = pivot_table_data(
        df,
        index=parse_csv_items(args.index_cols),
        columns=args.columns_col,
        values=args.values_col,
        aggfunc=args.agg,
        fill_value=coerce_scalar(args.fill_value),
    )
    _save_or_preview(result, args.output, label="宽表结果")
    return 0


def handle_normalize(args) -> int:
    df = _load_input(args.input, args.sep)
    axis = 1 if args.axis == "row" else 0
    result = normalize_columns(
        df,
        columns=parse_csv_items(args.columns),
        method=args.method,
        axis=axis,
        pseudocount=args.pseudocount,
        scale=args.scale,
    )
    _save_or_preview(result, args.output, label="标准化结果")
    return 0


def handle_topn(args) -> int:
    df = _load_input(args.input, args.sep)
    result = top_n(
        df,
        sort_col=args.sort_col,
        n=args.n,
        group_cols=parse_csv_items(args.group_cols),
        ascending=args.ascending,
    )
    _save_or_preview(result, args.output, label="TopN 结果")
    return 0


def handle_compare(args) -> int:
    df = _load_input(args.input, args.sep)
    result = compare_groups(
        df,
        group_col=args.group_col,
        value_col=args.value_col,
        case_group=args.case_group,
        control_group=args.control_group,
        feature_col=args.feature_col,
        aggfunc=args.agg,
        pseudocount=args.pseudocount,
    )
    _save_or_preview(result, args.output, label="对比结果")
    return 0


def handle_split(args) -> int:
    df = _load_input(args.input, args.sep)
    result = split_column(
        df,
        column=args.column,
        separator=args.separator,
        new_columns=parse_csv_items(args.new_columns),
        keep_original=not args.drop_original,
        prefix=args.prefix,
    )
    _save_or_preview(result, args.output, label="拆分结果")
    return 0


def handle_explode(args) -> int:
    df = _load_input(args.input, args.sep)
    result = explode_column(
        df,
        column=args.column,
        separator=args.separator,
        drop_empty=not args.keep_empty,
    )
    _save_or_preview(result, args.output, label="展开结果")
    return 0


def handle_deduplicate(args) -> int:
    df = _load_input(args.input, args.sep)
    keep = False if args.keep == "False" else args.keep
    result = deduplicate_rows(df, subset=parse_csv_items(args.subset), keep=keep)
    _save_or_preview(result, args.output, label="去重结果")
    return 0


def handle_concat(args) -> int:
    tables = [_load_input(path) for path in args.inputs]
    axis = 0 if args.axis == "rows" else 1
    result = concat_tables(tables, axis=axis, join=args.join, add_source=args.add_source, source_names=args.inputs)
    _save_or_preview(result, args.output, label="合并结果")
    return 0


def handle_pipeline(args) -> int:
    session = run_pipeline(args.config)
    print("流水线执行完成。")
    print(f"共生成 {len(session.tables)} 个数据集: {', '.join(session.names())}")
    return 0


def handle_interactive() -> int:
    from .interactive import main as interactive_main

    interactive_main()
    return 0


HANDLERS = {
    "interactive": lambda args: handle_interactive(),
    "inspect": handle_inspect,
    "profile": handle_profile,
    "clean": handle_clean,
    "filter": handle_filter,
    "derive": handle_derive,
    "aggregate": handle_aggregate,
    "multivalue": handle_multivalue,
    "crosstab": handle_crosstab,
    "join": handle_join,
    "matrix": handle_matrix,
    "melt": handle_melt,
    "pivot": handle_pivot,
    "normalize": handle_normalize,
    "topn": handle_topn,
    "compare": handle_compare,
    "split": handle_split,
    "explode": handle_explode,
    "deduplicate": handle_deduplicate,
    "concat": handle_concat,
    "pipeline": handle_pipeline,
}


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.command:
        parser.print_help()
        return 0
    try:
        return HANDLERS[args.command](args)
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

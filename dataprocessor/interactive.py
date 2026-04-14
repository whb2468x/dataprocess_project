from __future__ import annotations

from .io_utils import preview_text, read_table, write_table
from .operations import (
    aggregate_table,
    build_matrix,
    clean_table,
    compare_groups,
    crosstab_table,
    derive_columns,
    explode_column,
    filter_table,
    inspect_table,
    join_tables,
    melt_table,
    multivalue_statistics,
    normalize_columns,
    pivot_table_data,
    split_column,
    top_n,
)
from .pipeline import run_pipeline
from .session import DataSession
from .utils import coerce_scalar, parse_csv_items, parse_fill_values, parse_mapping


class InteractiveApp:
    def __init__(self) -> None:
        self.session = DataSession()

    def _input(self, prompt: str, default: str | None = None) -> str:
        raw = input(f"{prompt}{f' [{default}]' if default else ''}: ").strip()
        return raw or (default or "")

    def _yes_no(self, prompt: str, default: bool = False) -> bool:
        default_text = "y" if default else "n"
        value = self._input(f"{prompt} (y/n)", default_text).lower()
        return value in {"y", "yes", "1", "true"}

    def _require_datasets(self) -> bool:
        if self.session.names():
            return True
        print("当前还没有加载任何数据集。")
        return False

    def _choose_dataset(self, prompt: str = "请选择数据集") -> str | None:
        if not self._require_datasets():
            return None
        print("\n可用数据集:")
        for idx, name in enumerate(self.session.names(), start=1):
            df = self.session.get(name)
            print(f"{idx}. {name} ({len(df)} 行 x {len(df.columns)} 列)")
        choice = self._input(prompt)
        if choice.isdigit():
            index = int(choice) - 1
            names = self.session.names()
            if 0 <= index < len(names):
                return names[index]
        if choice in self.session.names():
            return choice
        print("未找到对应数据集。")
        return None

    def _store_result(self, name: str, df) -> None:
        self.session.add(name, df)
        print(f"\n已生成数据集 '{name}'，规模为 {len(df)} 行 x {len(df.columns)} 列")
        print(preview_text(df))

    def show_menu(self) -> None:
        print("\n" + "=" * 72)
        print("模块化数据处理软件")
        print("=" * 72)
        print("1. 加载数据文件")
        print("2. 查看当前数据集")
        print("3. 查看数据概览 / 列画像")
        print("4. 清洗数据")
        print("5. 筛选 / 排序 / 裁剪列")
        print("6. 计算新列")
        print("7. 分组聚合统计")
        print("8. 分隔值统计")
        print("9. 交叉表统计")
        print("10. 双表关联")
        print("11. 构建矩阵")
        print("12. 重塑 / 拆分 / 展开")
        print("13. 数值标准化")
        print("14. TopN / 组间比较")
        print("15. 运行批处理流水线")
        print("16. 保存数据集到文件")
        print("17. 退出")

    def load_dataset(self) -> None:
        path = self._input("请输入数据文件路径")
        default_name = f"dataset_{len(self.session.names()) + 1}"
        name = self._input("请输入数据集名称", default_name)
        sep = self._input("手动指定分隔符（留空自动识别）", "")
        df = read_table(path, sep=sep or None)
        self._store_result(name, df)

    def list_datasets(self) -> None:
        if not self._require_datasets():
            return
        print("\n当前数据集:")
        for item in self.session.describe():
            print(
                f"- {item['name']}: {item['rows']} 行 x {item['columns']} 列"
                f" | 列预览: {item['column_names']}"
            )

    def inspect_dataset(self) -> None:
        name = self._choose_dataset("请选择要查看的数据集")
        if not name:
            return
        summary = inspect_table(self.session.get(name))
        print(f"\n数据集 '{name}' 概览")
        print(f"行数: {summary['rows']}")
        print(f"列数: {summary['columns']}")
        print(f"列名: {', '.join(map(str, summary['column_names']))}")
        print("\n前几行预览:")
        print(summary["preview"].to_string(index=False))
        print("\n列画像:")
        print(summary["profile"].to_string(index=False))

    def clean_dataset(self) -> None:
        source = self._choose_dataset("请选择要清洗的数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_clean")
        rename_text = self._input("重命名列（old:new,old2:new2，留空跳过）", "")
        keep_raw = self._input("去重保留 first / last / False", "first")
        keep = False if keep_raw == "False" else keep_raw
        result = clean_table(
            self.session.get(source),
            normalize_column_names=self._yes_no("是否规范化列名（小写+下划线）", False),
            strip_values=self._yes_no("是否去除文本值两端空格", True),
            blank_to_na=not self._yes_no("是否保留空白字符串而不转缺失值", False),
            rename_map=parse_mapping(parse_csv_items(rename_text)) if rename_text else None,
            convert_numeric_columns=parse_csv_items(self._input("强制转数值的列（逗号分隔，留空跳过）", "")),
            auto_numeric=self._yes_no("是否自动识别并转换数值列", True),
            fill_values=parse_fill_values(parse_csv_items(self._input("按列填充缺失值（col=value，逗号分隔）", ""))),
            fill_numeric=self._parse_optional_float(self._input("统一填充所有数值缺失（留空跳过）", "")),
            fill_text=self._input("统一填充所有文本缺失（留空跳过）", "") or None,
            drop_empty_rows=self._yes_no("是否删除全空行", False),
            drop_empty_columns=self._yes_no("是否删除全空列", False),
            deduplicate_subset=parse_csv_items(self._input("按哪些列去重（逗号分隔，留空表示不去重）", "")) or None,
            keep=keep,
        )
        self._store_result(output, result)

    def filter_dataset(self) -> None:
        source = self._choose_dataset("请选择要筛选的数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_filtered")
        result = filter_table(
            self.session.get(source),
            query=self._input("请输入筛选表达式（留空跳过）", "") or None,
            include_columns=parse_csv_items(self._input("只保留哪些列（逗号分隔，留空跳过）", "")),
            exclude_columns=parse_csv_items(self._input("删除哪些列（逗号分隔，留空跳过）", "")),
            sort_by=parse_csv_items(self._input("按哪些列排序（逗号分隔，留空跳过）", "")),
            ascending=self._yes_no("是否升序排序", True),
            head=self._parse_optional_int(self._input("只保留前 N 行（留空跳过）", "")),
        )
        self._store_result(output, result)

    def derive_dataset(self) -> None:
        source = self._choose_dataset("请选择源数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_derived")
        print("请输入派生列表达式，格式为: 新列=表达式")
        print("例如: score_ratio=`score_a` / (`score_b` + 1e-9)")
        expressions: dict[str, str] = {}
        while True:
            line = self._input("表达式（直接回车结束）", "")
            if not line:
                break
            if "=" not in line:
                print("格式错误，应为 新列=表达式")
                continue
            key, expression = line.split("=", 1)
            expressions[key.strip()] = expression.strip()
        if not expressions:
            print("未输入任何表达式。")
            return
        result = derive_columns(self.session.get(source), expressions)
        self._store_result(output, result)

    def aggregate_dataset(self) -> None:
        source = self._choose_dataset("请选择要统计的数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_agg")
        result = aggregate_table(
            self.session.get(source),
            group_cols=parse_csv_items(self._input("分组列（逗号分隔，留空表示全表汇总）", "")) or None,
            value_cols=parse_csv_items(self._input("统计列（逗号分隔，留空表示非分组列全部）", "")) or None,
            aggfuncs=parse_csv_items(self._input("聚合函数（如 count,sum,mean）", "count")),
            include_group_size=self._yes_no("是否输出 row_count", True),
        )
        self._store_result(output, result)

    def multivalue_dataset(self) -> None:
        source = self._choose_dataset("请选择数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_multivalue")
        result = multivalue_statistics(
            self.session.get(source),
            group_cols=parse_csv_items(self._input("分组列（逗号分隔，留空表示全表汇总）", "")) or None,
            value_cols=parse_csv_items(self._input("分隔值列（逗号分隔）", "")),
            separator=self._input("分隔符", ";"),
        )
        self._store_result(output, result)

    def crosstab_dataset(self) -> None:
        source = self._choose_dataset("请选择数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_crosstab")
        result = crosstab_table(
            self.session.get(source),
            index_col=self._input("行索引列"),
            column_col=self._input("列索引列"),
            value_col=self._input("值列（留空则做计数）", "") or None,
            aggfunc=self._input("聚合方式", "count"),
            margins=self._yes_no("是否添加总计", True),
            normalize=self._input("是否归一化（all/index/columns，留空跳过）", "") or None,
        )
        self._store_result(output, result)

    def join_dataset(self) -> None:
        left_name = self._choose_dataset("请选择左表")
        if not left_name:
            return
        right_name = self._choose_dataset("请选择右表")
        if not right_name:
            return
        output = self._input("结果数据集名称", f"{left_name}_joined")
        result = join_tables(
            self.session.get(left_name),
            self.session.get(right_name),
            left_on=self._input("左表匹配列"),
            right_on=self._input("右表匹配列"),
            how=self._input("关联方式 inner/left/right/outer", "inner"),
            keep_columns=parse_csv_items(self._input("关联后保留列（逗号分隔，留空保留全部）", "")) or None,
        )
        if self._yes_no("是否对关联结果继续做分组统计", False):
            result = aggregate_table(
                result,
                group_cols=parse_csv_items(self._input("分组列（逗号分隔）", "")) or None,
                value_cols=parse_csv_items(self._input("统计列（逗号分隔）", "")) or None,
                aggfuncs=parse_csv_items(self._input("聚合函数", "count")),
            )
        self._store_result(output, result)

    def matrix_dataset(self) -> None:
        source = self._choose_dataset("请选择主数据集")
        if not source:
            return
        use_reference = self._yes_no("是否使用参考数据集限定矩阵列", False)
        reference_name = self._choose_dataset("请选择参考数据集") if use_reference else None
        output = self._input("结果数据集名称", f"{source}_matrix")
        result = build_matrix(
            self.session.get(source),
            group_col=self._input("矩阵行索引列"),
            feature_col=self._input("矩阵列表头列"),
            value_col=self._input("矩阵值列（留空按出现次数计数）", "") or None,
            aggfunc=self._input("聚合函数", "sum"),
            fill_value=coerce_scalar(self._input("空值填充值（留空保持缺失）", "") or None),
            reference_df=self.session.get(reference_name) if reference_name else None,
            reference_col=self._input("参考表特征列（留空跳过）", "") or None,
        )
        self._store_result(output, result)

    def reshape_dataset(self) -> None:
        source = self._choose_dataset("请选择数据集")
        if not source:
            return
        print("1. 宽表转长表")
        print("2. 长表转宽表")
        print("3. 拆分字段")
        print("4. 展开多值字段")
        choice = self._input("请选择操作")
        df = self.session.get(source)

        if choice == "1":
            output = self._input("结果数据集名称", f"{source}_melt")
            result = melt_table(
                df,
                id_vars=parse_csv_items(self._input("id_vars（逗号分隔）", "")),
                value_vars=parse_csv_items(self._input("value_vars（逗号分隔）", "")),
                var_name=self._input("变量列名", "variable"),
                value_name=self._input("值列名", "value"),
            )
        elif choice == "2":
            output = self._input("结果数据集名称", f"{source}_pivot")
            result = pivot_table_data(
                df,
                index=parse_csv_items(self._input("索引列（逗号分隔）", "")),
                columns=self._input("列名来源列"),
                values=self._input("值列"),
                aggfunc=self._input("聚合函数", "sum"),
                fill_value=coerce_scalar(self._input("空值填充值（留空跳过）", "") or None),
            )
        elif choice == "3":
            output = self._input("结果数据集名称", f"{source}_split")
            result = split_column(
                df,
                column=self._input("要拆分的列"),
                separator=self._input("分隔符", ";"),
                new_columns=parse_csv_items(self._input("新列名（逗号分隔，留空自动命名）", "")) or None,
                keep_original=not self._yes_no("是否删除原始列", False),
                prefix=self._input("自动命名前缀", "level"),
            )
        elif choice == "4":
            output = self._input("结果数据集名称", f"{source}_explode")
            result = explode_column(
                df,
                column=self._input("要展开的列"),
                separator=self._input("分隔符", ";"),
                drop_empty=not self._yes_no("是否保留空项", False),
            )
        else:
            print("无效选择。")
            return
        self._store_result(output, result)

    def normalize_dataset(self) -> None:
        source = self._choose_dataset("请选择数据集")
        if not source:
            return
        output = self._input("结果数据集名称", f"{source}_normalized")
        axis = 1 if self._input("按 row 还是 column 处理", "column") == "row" else 0
        result = normalize_columns(
            self.session.get(source),
            columns=parse_csv_items(self._input("目标数值列（逗号分隔）", "")),
            method=self._input("方法 relative/cpm/log2/log10/zscore/minmax", "zscore"),
            axis=axis,
            pseudocount=float(self._input("伪计数", "1e-9")),
            scale=float(self._input("scale（仅 relative 有效）", "1.0")),
        )
        self._store_result(output, result)

    def ranking_dataset(self) -> None:
        source = self._choose_dataset("请选择数据集")
        if not source:
            return
        print("1. TopN 排序")
        print("2. 两组比较")
        choice = self._input("请选择操作")
        df = self.session.get(source)

        if choice == "1":
            output = self._input("结果数据集名称", f"{source}_topn")
            result = top_n(
                df,
                sort_col=self._input("排序列"),
                n=int(self._input("N", "10")),
                group_cols=parse_csv_items(self._input("分组列（逗号分隔，留空表示全表）", "")) or None,
                ascending=self._yes_no("是否升序", False),
            )
        elif choice == "2":
            output = self._input("结果数据集名称", f"{source}_compare")
            result = compare_groups(
                df,
                group_col=self._input("组列"),
                value_col=self._input("值列"),
                case_group=self._input("实验组值"),
                control_group=self._input("对照组值"),
                feature_col=self._input("特征列（留空则整体比较）", "") or None,
                aggfunc=self._input("组内聚合方式", "mean"),
                pseudocount=float(self._input("伪计数", "1e-9")),
            )
        else:
            print("无效选择。")
            return
        self._store_result(output, result)

    def run_pipeline_config(self) -> None:
        path = self._input("请输入流水线配置 JSON 路径")
        pipeline_session = run_pipeline(path)
        for name in pipeline_session.names():
            self.session.add(name, pipeline_session.get(name))
        print(f"流水线运行完成，当前会话新增/更新 {len(pipeline_session.names())} 个数据集。")

    def save_dataset(self) -> None:
        name = self._choose_dataset("请选择要保存的数据集")
        if not name:
            return
        path = self._input("请输入输出路径")
        saved_path = write_table(self.session.get(name), path)
        print(f"已保存到 {saved_path}")

    def _parse_optional_int(self, value: str) -> int | None:
        return int(value) if value else None

    def _parse_optional_float(self, value: str) -> float | None:
        return float(value) if value else None

    def run(self) -> None:
        print("欢迎使用模块化数据处理软件。")
        while True:
            self.show_menu()
            choice = self._input("请选择功能")
            try:
                if choice == "1":
                    self.load_dataset()
                elif choice == "2":
                    self.list_datasets()
                elif choice == "3":
                    self.inspect_dataset()
                elif choice == "4":
                    self.clean_dataset()
                elif choice == "5":
                    self.filter_dataset()
                elif choice == "6":
                    self.derive_dataset()
                elif choice == "7":
                    self.aggregate_dataset()
                elif choice == "8":
                    self.multivalue_dataset()
                elif choice == "9":
                    self.crosstab_dataset()
                elif choice == "10":
                    self.join_dataset()
                elif choice == "11":
                    self.matrix_dataset()
                elif choice == "12":
                    self.reshape_dataset()
                elif choice == "13":
                    self.normalize_dataset()
                elif choice == "14":
                    self.ranking_dataset()
                elif choice == "15":
                    self.run_pipeline_config()
                elif choice == "16":
                    self.save_dataset()
                elif choice == "17":
                    print("已退出。")
                    break
                else:
                    print("无效选择，请重新输入。")
            except Exception as exc:  # noqa: BLE001
                print(f"执行失败: {exc}")


def main() -> None:
    InteractiveApp().run()


if __name__ == "__main__":
    main()

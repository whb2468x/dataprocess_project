# 模块化数据处理软件用户手册

## 1. 软件定位

这个软件基于原始 `dataprocess.py` 重构而来，重点不再局限于单一交互脚本，而是升级为一套更灵活的通用数据处理工具。  
它主要面向表格型、矩阵型、分组统计型数据处理场景，适合后续重复性分析、批量分析和流程固化。

它不做序列处理、比对、注释数据库检索那一套，核心目标是把“数据表处理能力”做强、做稳、做灵活。

## 2. 本次升级后的核心能力

### 2.1 架构升级

- 从单文件脚本升级为模块化软件。
- 保留原来直接运行 `python dataprocess.py` 的方式。
- 新增命令行子命令模式。
- 新增 JSON 批处理流水线模式。
- 从“主表 + 参考表”扩展为“多数据集会话”。

### 2.2 数据处理能力增强

- 数据概览与列画像。
- 清洗：列名规范化、空白转缺失、自动数值识别、填充、去重、删空行/空列。
- 筛选：条件过滤、列裁剪、排序、截取前 N 行。
- 派生列：通过表达式新增计算列。
- 分组聚合：`count`、`nunique`、`duplicate_count`、`sum`、`mean`、`min`、`max`、`std`、`median`、`first`、`last`。
- 分隔值字段统计：如 `A;B;C` 这类组合列的唯一值、总次数、重复率统计。
- 交叉表统计。
- 双表关联。
- 矩阵构建。
- 长宽表转换。
- 字段拆分与展开。
- 数值标准化：`relative`、`cpm`、`log2`、`log10`、`zscore`、`minmax`。
- TopN 选取。
- 两组比较：差值、fold change、log2 fold change。
- 批处理流水线执行。

## 3. 目录结构

```text
dataprocess.py                兼容原始入口；无参数时进入交互模式
dataprocessor/                新的软件核心包
dataprocessor/cli.py          命令行入口
dataprocessor/interactive.py  中文交互式菜单
dataprocessor/pipeline.py     JSON 流水线执行器
dataprocessor/operations/     功能模块
examples/pipeline_example.json  批处理示例
USER_MANUAL.md                当前手册
```

## 4. 安装与运行

### 4.1 直接运行交互模式

```bash
python dataprocess.py
```

### 4.2 命令行模式

```bash
python dataprocess.py inspect -i data.tsv
```

### 4.3 作为命令安装

```bash
pip install -e .
dataprocess inspect -i data.tsv
```

## 5. 支持的数据格式

- `csv`
- `tsv`
- `txt`
- `xls`
- `xlsx`
- `json`
- `parquet`
- `pkl`

文本文件会自动尝试识别分隔符；也可以通过 `--sep` 手动指定。

## 6. 交互模式说明

运行 `python dataprocess.py` 后会进入中文菜单。  
交互模式适合边看结果边处理，推荐用于探索性处理或一次性分析。

主要菜单包括：

1. 加载数据文件
2. 查看当前数据集
3. 查看数据概览 / 列画像
4. 清洗数据
5. 筛选 / 排序 / 裁剪列
6. 计算新列
7. 分组聚合统计
8. 分隔值统计
9. 交叉表统计
10. 双表关联
11. 构建矩阵
12. 重塑 / 拆分 / 展开
13. 数值标准化
14. TopN / 组间比较
15. 运行批处理流水线
16. 保存数据集到文件

## 7. 命令行使用说明

### 7.1 查看数据概览

```bash
python dataprocess.py inspect -i data.tsv
```

作用：

- 查看行数、列数、列名。
- 查看前几行。
- 查看每列缺失、唯一值、数值统计。

### 7.2 生成列画像表

```bash
python dataprocess.py profile -i data.tsv -o profile.tsv
```

适合用于：

- 先做全表摸底。
- 找出缺失严重列。
- 判断哪些列适合继续数值化。

### 7.3 清洗数据

```bash
python dataprocess.py clean \
  -i raw.tsv \
  -o cleaned.tsv \
  --normalize-column-names \
  --strip-values \
  --auto-numeric \
  --drop-empty-columns \
  --fill status=unknown \
  --fill-numeric 0
```

常见参数：

- `--normalize-column-names`：列名转小写并替换空格为下划线。
- `--strip-values`：去除文本值前后空格。
- `--auto-numeric`：自动识别数值列。
- `--convert-numeric col1,col2`：强制指定列转数值。
- `--fill col=value`：按列填充缺失。
- `--fill-numeric 0`：统一填充数值缺失。
- `--drop-empty-rows`
- `--drop-empty-columns`
- `--deduplicate-subset id,sample`

### 7.4 条件筛选与排序

```bash
python dataprocess.py filter \
  -i cleaned.tsv \
  -o filtered.tsv \
  --query "`abundance` > 0 and `group` == 'A'" \
  --include sample_id,feature_id,abundance \
  --sort-by abundance
```

### 7.5 计算新列

```bash
python dataprocess.py derive \
  -i filtered.tsv \
  -o derived.tsv \
  --expr "ratio=`value_a` / (`value_b` + 1e-9)" \
  --expr "log_value=np.log2(`abundance` + 1)"
```

说明：

- 支持 `pandas.eval(engine='python')` 风格表达式。
- 列名带空格时请使用反引号包裹，例如 `` `sample id` ``。

### 7.6 分组聚合

```bash
python dataprocess.py aggregate \
  -i derived.tsv \
  -o summary.tsv \
  --group-cols sample_type,group \
  --value-cols abundance,ratio \
  --agg count,sum,mean,max
```

可用聚合函数：

- `count`
- `nunique`
- `duplicate_count`
- `sum`
- `mean`
- `min`
- `max`
- `std`
- `median`
- `first`
- `last`

### 7.7 分隔值字段统计

适用于一列中包含 `A;B;C` 这样的组合值。

```bash
python dataprocess.py multivalue \
  -i data.tsv \
  -o multi_stat.tsv \
  --group-cols sample_id \
  --value-cols feature_list \
  --separator ";"
```

输出包括：

- 唯一值数量
- 总出现次数
- 重复值数量
- 重复率

### 7.8 交叉表统计

```bash
python dataprocess.py crosstab \
  -i data.tsv \
  -o crosstab.tsv \
  --index-col group \
  --column-col feature_type
```

如需对数值列求和：

```bash
python dataprocess.py crosstab \
  -i data.tsv \
  -o crosstab_sum.tsv \
  --index-col group \
  --column-col feature_type \
  --value-col abundance \
  --agg sum
```

### 7.9 双表关联

```bash
python dataprocess.py join \
  --left main.tsv \
  --right ref.tsv \
  -o joined.tsv \
  --left-on feature_id \
  --right-on feature_id \
  --how left
```

关联后继续统计：

```bash
python dataprocess.py join \
  --left main.tsv \
  --right ref.tsv \
  -o joined_summary.tsv \
  --left-on feature_id \
  --right-on feature_id \
  --how left \
  --post-group-cols sample_type \
  --post-value-cols abundance \
  --post-agg sum,mean
```

### 7.10 构建矩阵

```bash
python dataprocess.py matrix \
  -i main.tsv \
  -r ref.tsv \
  -o matrix.tsv \
  --group-col sample_id \
  --feature-col feature_id \
  --value-col abundance \
  --reference-col feature_id \
  --agg sum \
  --fill-value 0
```

如果不指定 `--value-col`，则会按出现次数计数。

### 7.11 长宽表转换

宽表转长表：

```bash
python dataprocess.py melt \
  -i matrix.tsv \
  -o long.tsv \
  --id-vars sample_id \
  --value-vars KO001,KO002,KO003
```

长表转宽表：

```bash
python dataprocess.py pivot \
  -i long.tsv \
  -o matrix.tsv \
  --index-cols sample_id \
  --columns-col feature_id \
  --values-col abundance \
  --agg sum \
  --fill-value 0
```

### 7.12 拆分字段与展开字段

拆分字段：

```bash
python dataprocess.py split \
  -i data.tsv \
  -o split.tsv \
  --column lineage \
  --separator ";" \
  --new-columns level1,level2,level3
```

展开字段：

```bash
python dataprocess.py explode \
  -i data.tsv \
  -o exploded.tsv \
  --column feature_list \
  --separator ";"
```

### 7.13 数值标准化

```bash
python dataprocess.py normalize \
  -i matrix.tsv \
  -o matrix_zscore.tsv \
  --columns KO001,KO002,KO003 \
  --method zscore \
  --axis column
```

常见方法：

- `relative`：相对比例
- `cpm`：每百万归一化
- `log2`
- `log10`
- `zscore`
- `minmax`

### 7.14 TopN 与两组比较

TopN：

```bash
python dataprocess.py topn \
  -i data.tsv \
  -o top10.tsv \
  --sort-col abundance \
  -n 10
```

按组取 TopN：

```bash
python dataprocess.py topn \
  -i data.tsv \
  -o top10_each_group.tsv \
  --sort-col abundance \
  --group-cols sample_type \
  -n 10
```

两组比较：

```bash
python dataprocess.py compare \
  -i data.tsv \
  -o compare.tsv \
  --group-col condition \
  --feature-col feature_id \
  --value-col abundance \
  --case-group case \
  --control-group control \
  --agg mean
```

输出包括：

- `case_value`
- `control_value`
- `difference`
- `fold_change`
- `log2_fold_change`

## 8. 批处理流水线

如果你后面会反复跑类似流程，推荐使用流水线模式。

运行方式：

```bash
python dataprocess.py pipeline -c examples/pipeline_example.json
```

### 8.1 流水线配置结构

```json
{
  "datasets": {
    "main": "input/main.tsv",
    "ref": "input/reference.tsv"
  },
  "steps": [
    {
      "operation": "clean",
      "input": "main",
      "output": "main_clean",
      "params": {
        "auto_numeric": true
      }
    }
  ],
  "exports": [
    {
      "dataset": "main_clean",
      "path": "output/main_clean.tsv"
    }
  ]
}
```

### 8.2 当前支持的流水线操作

- `load`
- `clean`
- `filter`
- `derive`
- `aggregate`
- `multivalue_stats`
- `crosstab`
- `join`
- `matrix`
- `normalize`
- `melt`
- `pivot`
- `split`
- `explode`
- `deduplicate`
- `topn`
- `compare`
- `concat`
- `save`
- `export`

## 9. 推荐工作流

### 方案 A：临时处理

1. 用交互模式加载数据。
2. 先看概览和列画像。
3. 先清洗，再筛选，再计算新列。
4. 最后做统计、关联、矩阵和导出。

### 方案 B：固定流程重复跑

1. 先在交互模式中试出一套步骤。
2. 再把步骤写进 JSON 流水线。
3. 以后只替换输入文件，直接批量执行。

## 10. 注意事项

- 表达式计算基于 `pandas.eval`，列名有空格时请使用反引号。
- `join`、`matrix`、`aggregate` 对列名拼写要求严格，建议先跑 `inspect` 或 `profile`。
- `normalize` 会尝试把目标列转成数值；无法转换的值会变成缺失。
- `fill-value`、`--fill col=value` 这类参数会自动尝试识别整数、浮点数、布尔值和空值。
- `parquet` 读写依赖本地环境中可用的相应引擎。

## 11. 最简使用建议

如果你只记三条：

1. 先用 `inspect`/`profile` 摸底。
2. 先做 `clean`，再做 `filter`、`derive`、`aggregate`。
3. 重复流程统一写成 `pipeline`。

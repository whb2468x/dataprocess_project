from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from dataprocessor.operations import (
    aggregate_table,
    build_matrix,
    clean_table,
    compare_groups,
    derive_columns,
    join_tables,
    multivalue_statistics,
    normalize_columns,
)
from dataprocessor.pipeline import run_pipeline


class DataProcessorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.df = pd.DataFrame(
            {
                "sample_id": ["S1", "S1", "S2", "S2"],
                "group": ["A", "A", "B", "B"],
                "feature_id": ["F1", "F2", "F1", "F2"],
                "abundance": [10, 5, 3, 8],
                "tags": ["x;y", "y;z", "x", "z;z"],
                "text_num": ["1", "2", "3", "4"],
            }
        )
        self.ref = pd.DataFrame(
            {
                "feature_id": ["F1", "F2", "F3"],
                "feature_name": ["Alpha", "Beta", "Gamma"],
            }
        )

    def test_clean_derive_and_aggregate(self) -> None:
        cleaned = clean_table(self.df, auto_numeric=True, convert_numeric_columns=["text_num"])
        self.assertTrue(pd.api.types.is_numeric_dtype(cleaned["text_num"]))

        derived = derive_columns(cleaned, {"double_abundance": "`abundance` * 2"})
        result = aggregate_table(
            derived,
            group_cols=["group"],
            value_cols=["abundance", "double_abundance"],
            aggfuncs=["sum", "mean"],
        )
        self.assertEqual(result.loc[result["group"] == "A", "abundance_sum"].iloc[0], 15)
        self.assertEqual(result.loc[result["group"] == "B", "double_abundance_sum"].iloc[0], 22)

    def test_multivalue_join_matrix_and_normalize(self) -> None:
        multi = multivalue_statistics(self.df, group_cols=["sample_id"], value_cols=["tags"], separator=";")
        self.assertIn("tags_duplicate_count", multi.columns)

        joined = join_tables(self.df, self.ref, left_on="feature_id", right_on="feature_id", how="left")
        matrix = build_matrix(
            joined,
            group_col="sample_id",
            feature_col="feature_id",
            value_col="abundance",
            aggfunc="sum",
            fill_value=0,
            reference_df=self.ref,
            reference_col="feature_id",
        )
        normalized = normalize_columns(matrix, columns=["F1", "F2", "F3"], method="relative", axis=1)
        self.assertAlmostEqual(float(normalized.loc[0, ["F1", "F2", "F3"]].sum()), 1.0)

    def test_compare_and_pipeline(self) -> None:
        compared = compare_groups(
            self.df,
            group_col="group",
            feature_col="feature_id",
            value_col="abundance",
            case_group="A",
            control_group="B",
        )
        self.assertIn("log2_fold_change", compared.columns)

        with tempfile.TemporaryDirectory() as tmp_dir:
            base = Path(tmp_dir)
            main_path = base / "main.tsv"
            ref_path = base / "ref.tsv"
            output_path = base / "summary.tsv"
            self.df.to_csv(main_path, sep="\t", index=False)
            self.ref.to_csv(ref_path, sep="\t", index=False)

            config = {
                "datasets": {"main": "main.tsv", "ref": "ref.tsv"},
                "steps": [
                    {
                        "operation": "clean",
                        "input": "main",
                        "output": "main_clean",
                        "params": {"auto_numeric": True},
                    },
                    {
                        "operation": "aggregate",
                        "input": "main_clean",
                        "output": "summary",
                        "params": {
                            "group_cols": ["group"],
                            "value_cols": ["abundance"],
                            "aggfuncs": ["sum"],
                        },
                    },
                ],
                "exports": [{"dataset": "summary", "path": "summary.tsv"}],
            }
            config_path = base / "pipeline.json"
            config_path.write_text(json.dumps(config, ensure_ascii=False), encoding="utf-8")

            session = run_pipeline(config_path)
            self.assertIn("summary", session.names())
            self.assertTrue(output_path.exists())


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass
class DataSession:
    tables: dict[str, pd.DataFrame] = field(default_factory=dict)

    def add(self, name: str, df: pd.DataFrame) -> None:
        self.tables[name] = df.copy()

    def get(self, name: str) -> pd.DataFrame:
        if name not in self.tables:
            raise KeyError(f"未找到数据集: {name}")
        return self.tables[name]

    def names(self) -> list[str]:
        return list(self.tables.keys())

    def remove(self, name: str) -> None:
        if name in self.tables:
            del self.tables[name]

    def describe(self) -> list[dict[str, object]]:
        summary: list[dict[str, object]] = []
        for name, df in self.tables.items():
            summary.append(
                {
                    "name": name,
                    "rows": len(df),
                    "columns": len(df.columns),
                    "column_names": ", ".join(map(str, df.columns[:8])),
                }
            )
        return summary

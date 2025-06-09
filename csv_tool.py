import pandas as pd

class CSVTool:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df = pd.read_csv(filepath)

    def display_rows(self, count=3):
        print(self.df.head(count))

    def filter_rows(self, column: str, op: str, value) -> pd.DataFrame:
        if op == ">":
            return self.df[self.df[column] > value]
        elif op == "<":
            return self.df[self.df[column] < value]
        elif op == ">=":
            return self.df[self.df[column] >= value]
        elif op == "<=":
            return self.df[self.df[column] <= value]
        elif op == "==":
            return self.df[self.df[column] == value]
        elif op == "contains":
            return self.df[self.df[column].astype(str).str.contains(str(value))]
        else:
            raise ValueError("Unsupported operator")

    def sort_rows(self, column: str, descending=False) -> pd.DataFrame:
        return self.df.sort_values(by=column, ascending=not descending)

    def aggregate_column(self, column: str, agg_func: str):
        if agg_func == "sum":
            return self.df[column].sum()
        elif agg_func == "avg":
            return self.df[column].mean()
        elif agg_func == "min":
            return self.df[column].min()
        elif agg_func == "max":
            return self.df[column].max()
        else:
            raise ValueError("Invalid aggregation type")

    def write_csv(self, df: pd.DataFrame, output_file: str):
        df.to_csv(output_file, index=False)

    def count_valid_palindromes(self) -> int:
        valid_letters = set("ADBVN")
        count = 0

        for val in self.df.astype(str).values.flatten():
            val_upper = val.upper()
            if set(val_upper).issubset(valid_letters) and val_upper == val_upper[::-1]:
                count += 1
        return count

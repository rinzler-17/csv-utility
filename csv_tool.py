import dask.dataframe as dd
import numpy as np

class CSVTool:
    def __init__(self, filepath: str):
        self.filepath = filepath

        try:
            # Read all columns as string for safe initial parsing
            self.df = dd.read_csv(
                filepath,
                dtype=str,
                blocksize="64MB",
                assume_missing=True,
                skip_blank_lines=True,
                on_bad_lines='skip'
            )
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            self.df = None
            return

        self.clean_csv_dataframe()
        print(f"Loaded and cleaned CSV with {self.df.shape[1]} columns.")

    def clean_csv_dataframe(self):
        self.df.columns = [col.strip() for col in self.df.columns]
        self.df = self.df.map_partitions(lambda df: df.map(lambda x: x.strip() if isinstance(x, str) else x))
        self.df = self.df.replace(r'^\s*$', np.nan, regex=True)

        # Convert columns with ≥60% numeric values to float
        inferred_dtypes = {}
        for col in self.df.columns:
            temp_col = dd.to_numeric(self.df[col], errors='coerce')
            numeric_ratio = (temp_col.notnull().sum() / len(temp_col)).compute()
            if numeric_ratio >= 0.6:
                inferred_dtypes[col] = 'float64'

        for col, dtype in inferred_dtypes.items():
            self.df[col] = dd.to_numeric(self.df[col], errors='coerce')

        self.df = self.df.dropna()

    def display_rows(self, count=3):
        print(self.df.head(count))

    def filter_rows(self, column: str, op: str, value):
        if op == ">":
            return self.df[self.df[column].astype(float) > float(value)]
        elif op == "<":
            return self.df[self.df[column].astype(float) < float(value)]
        elif op == ">=":
            return self.df[self.df[column].astype(float) >= float(value)]
        elif op == "<=":
            return self.df[self.df[column].astype(float) <= float(value)]
        elif op == "==":
            return self.df[self.df[column] == value]
        elif op == "contains":
            return self.df[self.df[column].astype(str).str.contains(str(value), na=False)]
        else:
            raise ValueError("Unsupported operator")

    def sort_rows(self, column: str, descending=False):
        try:
            if not descending:
                sorted_df = self.df.set_index(column, sort=True, drop=True)
                return sorted_df
            else:
                # Fallback to Pandas for descending sort (TODO: can be improved for larger datasets)
                sorted_pdf = self.df.compute().sort_values(by=column, ascending=False).reset_index(drop=True)
                
                # Wrap back into Dask dataframe
                return dd.from_pandas(sorted_pdf)
        except Exception as e:
            print(f"Sorting failed: {e}")
            return None

    def aggregate_column(self, column: str, agg_func: str):
        col = self.df[column].astype(float)
        if agg_func == "sum":
            return col.sum().compute()
        elif agg_func == "avg":
            return col.mean().compute()
        elif agg_func == "min":
            return col.min().compute()
        elif agg_func == "max":
            return col.max().compute()
        else:
            raise ValueError("Invalid aggregation type")

    def write_csv(self, df: dd.DataFrame, output_file: str):
        df.compute().to_csv(output_file, index=False)

    def count_valid_palindromes(self) -> int:
        valid_letters = set("ADBVN")

        def is_valid(val):
            s = str(val).upper()
            return set(s).issubset(valid_letters) and s == s[::-1]

        bool_df = self.df.map_partitions(lambda df: df.map(is_valid))
        count_series = bool_df.sum(axis=1)
        return count_series.sum().compute()

import pandas as pd
import numpy as np

class CSVTool:
    def __init__(self, filepath: str):
        self.filepath = filepath
        self.df = pd.read_csv(filepath)

        try:
            self.df = pd.read_csv(
                filepath,
                skip_blank_lines=True,
                on_bad_lines='skip'
            )
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            return None

        self.clean_csv_dataframe()

        print(f"Loaded and cleaned CSV with {self.df.shape[0]} rows and {self.df.shape[1]} columns.")

    def clean_csv_dataframe(self):
        """
        Cleans a DataFrame:
        - Drops fully empty rows
        - Strips whitespace from headers and cell values
        - Converts empty strings to NaN
        - Converts numeric columns to proper dtype (if possible)
        """
        # Record shape before cleaning
        initial_shape = self.df.shape

        # Strip whitespace from headers
        self.df.columns = self.df.columns.str.strip()

        # Convert blank strings to NaN
        self.df.replace(r'^\s*$', np.nan, regex=True, inplace=True)

        # Strip whitespace from string cells
        self.df = self.df.map(lambda x: x.strip() if isinstance(x, str) else x)

        for col in self.df.columns:
            # Try to convert to numeric with coercion
            converted = pd.to_numeric(self.df[col], errors='coerce')
            numeric_ratio = converted.notna().mean()
            
            if numeric_ratio >= 0.6:    # if 60% values are numeric, the column's dtype is numeric, replace other value by NaN
                self.df[col] = converted

        # Remove rows containing NaN
        self.df.dropna(how='any', inplace=True)

        dropped_rows = initial_shape[0] - self.df.shape[0]
        if dropped_rows > 0:
            print(f"Dropped {dropped_rows} empty row(s).")
        
        print(self.df)


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

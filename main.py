from csv_tool import CSVTool

def main():
    tool = CSVTool("resources/data.csv")

    print("== Display First 3 Rows ==")
    tool.display_rows()

    print("\n== Filter: Quantity > 10 ==")
    filtered = tool.filter_rows("Quantity", ">", 10)
    print(filtered.compute())

    print("\n== Filter: Product containing substring \"an\" ==")
    filtered = tool.filter_rows("Product", "contains", "an")
    print(filtered.compute())

    print("\n== Sort by Price Descending ==")
    sorted_df = tool.sort_rows("Price", descending=True)
    print(sorted_df.compute())

    print("\n== Aggregate: Average Quantity ==")
    avg = tool.aggregate_column("Quantity", "avg")
    print("Average Quantity:", avg)

    print("\n== Write Filtered Rows to CSV ==")
    tool.write_csv(filtered, "resources/output.csv")

    print("\n== Count Valid Palindromes ==")
    print("Valid Palindromes:", tool.count_valid_palindromes())

if __name__ == "__main__":
    main()

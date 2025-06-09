import unittest
import numpy as np
from csv_tool import CSVTool

class TestCSVTool(unittest.TestCase):
    def setUp(self):
        self.tool = CSVTool("resources/data.csv")

    def test_display(self):
        self.assertFalse(self.tool.df.empty)

    def test_filter(self):
        df = self.tool.filter_rows("Quantity", ">", 10)
        self.assertTrue((df["Quantity"] > 10).all())

    def test_sort(self):
        df = self.tool.sort_rows("Price", descending=True)
        prices = df["Price"].values
        self.assertTrue(all(prices[i] >= prices[i+1] for i in range(len(prices)-1)))

    def test_aggregate(self):
        result = self.tool.aggregate_column("Quantity", "sum")
        self.assertIsInstance(result, (np.int64, float))

    def test_palindromes(self):
        count = self.tool.count_valid_palindromes()
        self.assertEqual(count, 1)

if __name__ == "__main__":
    unittest.main()

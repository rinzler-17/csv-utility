## CSV Processing utiliy
This is a simple project to process a CSV file implemented using pandas library in Python.

Sample csv file is at `resources/data.csv`

## Instructions
Please ensure you have `Python` and `pip` installed in your localhost.
To install pandas, run
    
    $ pip install pandas

## Running the Code
    $ python main.py

To run tests

    $ python tests.py

## Possible improvements
To handle large files, the code can be ported to use the Dask library APIs (https://docs.dask.org/en/latest/install.html) which has interface similar to pandas and supports chunked processing and parallelism.
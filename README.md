## CSV Processing utiliy
This is a simple project to process a CSV file implemented using dask library (https://docs.dask.org/en/latest/install.html) in Python. Dask dataframes have interface similar to pandas and support chunked processing and parallelism to handle large files.

Sample csv file is at `resources/data.csv`

## Instructions
Please ensure you have `Python` and `pip` installed in your localhost.
To install dask, run
    
    $ pip install dask[complete]

## Running the Code
    $ python main.py

To run tests

    $ python tests.py

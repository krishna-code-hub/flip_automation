from IPython.core.magic import Magics, magics_class, cell_magic
import pandas as pd
from pyspark.sql import SparkSession

@magics_class
class PySparkSQLMagic(Magics):
    def __init__(self, shell, **kwargs):
        super().__init__(shell)
        # Initialize SparkSession
        self.spark = SparkSession.builder \
            .appName("JupyterPySparkSQLMagic") \
            .getOrCreate()
        print("Initialized Spark session")

    @cell_magic
    def sql(self, line, cell):
        """
        Execute a multi-line SQL query with PySpark and display the result as a pandas DataFrame.
        
        Usage:
        %%sql
        SELECT * 
        FROM employee
        WHERE dept = 'accounting';
        """
        try:
            # Execute the multi-line SQL query
            query = cell.strip()
            df = self.spark.sql(query)
            # Convert to pandas DataFrame and display
            pandas_df = df.toPandas()
            return pandas_df
        except Exception as e:
            print(f"Error executing query: {e}")

def load_ipython_extension(ipython):
    """This function is called when the extension is loaded."""
    ipython.register_magics(PySparkSQLMagic)

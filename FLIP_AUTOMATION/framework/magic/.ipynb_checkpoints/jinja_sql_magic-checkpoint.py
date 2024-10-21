from IPython.core.magic import Magics, magics_class, cell_magic
import pandas as pd
from pyspark.sql import SparkSession
from jinja2 import Template

@magics_class
class PySparkSQLMagic(Magics):
    def __init__(self, shell, **kwargs):
        super().__init__(shell)
        # Initialize SparkSession with time zone setting
        self.spark = SparkSession.builder \
            .appName("JupyterPySparkSQLMagic") \
            .config("spark.sql.session.timeZone", "UTC") \
            .getOrCreate()
        self.shell = shell  # Store the interactive shell to access notebook variables
        print("Initialized Spark session with UTC timezone")

    @cell_magic
    def sql(self, line, cell):
        """
        Execute a multi-line SQL query using Jinja templating with variables defined in the notebook.

        Usage:
        Define parameters in one cell, e.g.,
        loading_date = '2024-01-01'
        country = 'SG'
        system = 'RLS'

        Then use the SQL magic in another cell:
        %%sql
        SELECT * 
        FROM my_table
        WHERE loading_date = '{{ loading_date }}'
        AND country = '{{ country }}'
        AND system = '{{ system }}';
        """
        try:
            # Get the user's variables from the notebook's interactive namespace
            user_ns = self.shell.user_ns

            # Create a Jinja2 template for the SQL query
            template = Template(cell.strip())
            
            # Render the SQL template using variables from the notebook's namespace
            query = template.render(**user_ns)

            # Execute the SQL query
            df = self.spark.sql(query)
            
            # Convert to pandas DataFrame and return it
            pandas_df = df.toPandas()
            return pandas_df

        except Exception as e:
            print(f"Error executing query: {e}")

def load_ipython_extension(ipython):
    """This function is called when the extension is loaded."""
    ipython.register_magics(PySparkSQLMagic)

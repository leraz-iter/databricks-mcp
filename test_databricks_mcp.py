import unittest
import time
import os
from dotenv import load_dotenv
from main import DatabricksMCP  # Import from main.py (or save DatabricksMCP separately if preferred)

load_dotenv()

class TestDatabricksMCP(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up test data: create a schema, table, and insert rows."""
        token = os.getenv("DATABRICKS_TOKEN")
        host = os.getenv("DATABRICKS_HOST")
        warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
        if not all([host, token, warehouse_id]):
            raise ValueError("Missing required environment variables for tests")
        cls.databricks = DatabricksMCP(host, token, warehouse_id)
        
        # Use an existing catalog (e.g., "main")
        catalog = "main"
        schema = f"test_schema_{int(time.time())}"  # Unique schema name
        cls.databricks.create_schema(catalog, schema)
        cls.catalog = catalog
        cls.schema = schema
        
        # Create a test table
        table = "test_table"
        columns = [
            {"name": "id", "type": "INT"},
            {"name": "name", "type": "STRING"}
        ]
        cls.databricks.create_table(catalog, schema, table, columns)
        cls.table = table
        
        # Insert test data
        values = [(1, "Alice"), (2, "Bob")]
        cls.databricks.insert_data(f"{catalog}.{schema}.{table}", values)

    def test_execute_sql_query(self):
        """Test executing a SQL query."""
        sql = f"SELECT * FROM {self.catalog}.{self.schema}.{self.table}"
        rows = self.databricks.execute_sql_query(sql)
        self.assertEqual(len(rows), 2, "Should have two rows")
        self.assertEqual(rows[0][1], "Alice", "First row name should be Alice")
        self.assertEqual(rows[1][1], "Bob", "Second row name should be Bob")

    def test_list_schemas(self):
        """Test listing schemas in the catalog."""
        schemas = self.databricks.list_schemas(self.catalog)
        self.assertIn(self.schema, schemas, "Test schema should be listed")

    def test_list_tables(self):
        """Test listing tables in the schema."""
        tables = self.databricks.list_tables(self.catalog, self.schema)
        self.assertIn(self.table, tables, "Test table should be listed")

    def test_describe_table(self):
        """Test describing the table structure."""
        description = self.databricks.describe_table(self.catalog, self.schema, self.table)
        self.assertEqual(description["columns"], ["id", "name"], "Columns should match")
        self.assertEqual(description["data_types"], ["INT", "STRING"], "Data types should match")

if __name__ == "__main__":
    unittest.main()

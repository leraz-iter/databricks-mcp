import os
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP
import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

load_dotenv()

class DatabricksMCP:
    def __init__(self, host: str, token: str, warehouse_id: str):
        """Initialize the Databricks client with host, token, and warehouse ID."""
        self.client = WorkspaceClient(host=host, token=token)
        self.warehouse_id = warehouse_id

    def execute_sql_query(self, sql: str) -> list:
        """Execute a SQL query and return the results as a list of rows if applicable."""
        # Submit the SQL statement
        response = self.client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=sql
        )
        statement_id = response.statement_id

        # Polling parameters
        timeout = 60  # Maximum wait time in seconds
        interval = 5  # Polling interval in seconds
        start_time = time.time()

        # Poll the statement status
        while time.time() - start_time < timeout:
            status_response = self.client.statement_execution.get_statement(statement_id)
            state = status_response.status.state

            if state == StatementState.SUCCEEDED:
                # For SELECT queries, return the data; for DDL like CREATE TABLE, return empty list
                if status_response.result and status_response.result.data_array:
                    return [list(row) for row in status_response.result.data_array]
                return []  # No results for statements like CREATE TABLE
            elif state == StatementState.FAILED:
                raise Exception(f"SQL execution failed")
            elif state == StatementState.CANCELED:
                raise Exception("SQL execution was canceled")
            else:
                raise Exception(f"Unexpected statement state: {state}")

        # If polling exceeds timeout, raise an exception
        raise Exception("SQL execution timed out")

    def list_catalogs(self) -> list:
        """List all catalogs in the workspace."""
        catalogs = self.client.catalogs.list()
        return [c.name for c in catalogs]

    def list_schemas(self, catalog: str) -> list:
        """List all schemas in a given catalog."""
        schemas = self.client.schemas.list(catalog_name=catalog)
        return [s.name for s in schemas]

    def list_tables(self, catalog: str, schema: str) -> list:
        """List all tables in a given schema."""
        tables = self.client.tables.list(catalog_name=catalog, schema_name=schema)
        return [t.name for t in tables]

    def describe_table(self, catalog: str, schema: str, table: str) -> dict:
        """Describe the schema of a given table using SQL.

        Args:
            catalog (str): The catalog name.
            schema (str): The schema name.
            table (str): The table name.

        Returns:
            dict: A dictionary with 'columns' (list of column names) and 'data_types' (list of data types).
        """
        # Construct the fully qualified table name
        table_full_name = f"{catalog}.{schema}.{table}"
        # SQL query to describe the table
        sql = f"DESCRIBE TABLE {table_full_name}"
        # Execute the query using the existing method
        result = self.execute_sql_query(sql)
        # Extract column names and data types from the result
        columns = [row[0] for row in result]
        data_types = [row[1].upper() for row in result]
        # Return the schema as a dictionary
        return {"columns": columns, "data_types": data_types}

    def create_schema(self, catalog: str, schema: str):
        """Create a new schema in a given catalog."""
        self.client.schemas.create(name=schema, catalog_name=catalog)

    def create_table(self, catalog: str, schema: str, table: str, columns: list):
        """Create a new table with specified columns."""
        table_full_name = f"{catalog}.{schema}.{table}"
        column_defs = [f"{col['name']} {col['type']}" for col in columns]
        sql = f"CREATE TABLE {table_full_name} ({', '.join(column_defs)})"
        self.execute_sql_query(sql)

    def insert_data(self, table_full_name: str, values: list):
        """Insert data into a table (using literals for simplicity)."""
        def format_value(val):
            if isinstance(val, str):
                return f"'{val}'"  # Quote strings
            else:
                return str(val)    # Convert non-strings (e.g., int) to string without quotes
        
        values_str = ", ".join([f"({', '.join(map(format_value, row))})" for row in values])
        sql = f"INSERT INTO {table_full_name} VALUES {values_str}"
        self.execute_sql_query(sql)

# Initialize MCP server
mcp = FastMCP("Databricks MCP Server")

# Load Databricks credentials from environment
host = os.getenv("DATABRICKS_HOST")
token = os.getenv("DATABRICKS_TOKEN")
warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")

if not all([host, token, warehouse_id]):
    raise ValueError("Missing required environment variables: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID")

databricks = DatabricksMCP(host, token, warehouse_id)

# Register Databricks methods as MCP tools
@mcp.tool()
def execute_sql_query(sql: str) -> list:
    """Execute a SQL query on Databricks and return results as a list of rows."""
    return databricks.execute_sql_query(sql)

@mcp.tool()
def list_catalogs() -> list:
    """List all catalogs in the Databricks workspace."""
    return databricks.list_catalogs()

@mcp.tool()
def list_schemas(catalog: str) -> list:
    """List all schemas in a given Databricks catalog."""
    return databricks.list_schemas(catalog)

@mcp.tool()
def list_tables(catalog: str, schema: str) -> list:
    """List all tables in a given Databricks schema."""
    return databricks.list_tables(catalog, schema)

@mcp.tool()
def describe_table(catalog: str, schema: str, table: str) -> dict:
    """Describe the schema of a given Databricks table."""
    return databricks.describe_table(catalog, schema, table)

@mcp.tool()
def create_schema(catalog: str, schema: str):
    """Create a new schema in a given Databricks catalog."""
    databricks.create_schema(catalog, schema)

@mcp.tool()
def create_table(catalog: str, schema: str, table: str, columns: list):
    """Create a new table in Databricks with specified columns."""
    databricks.create_table(catalog, schema, table, columns)

@mcp.tool()
def insert_data(table_full_name: str, values: list):
    """Insert data into a Databricks table."""
    databricks.insert_data(table_full_name, values)

if __name__ == "__main__":
    mcp.run()

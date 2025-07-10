# MCP Databricks Server

This repository implements a Model Context Protocol (MCP) server for Databricks using the Databricks SDK. It allows LLMs or MCP clients to interact with Databricks SQL warehouses, executing queries, managing schemas/tables, and more.

## Features
- Execute SQL queries
- List catalogs, schemas, and tables
- Describe table schemas
- Create schemas and tables
- Insert data into tables

## Prerequisites
- Python 3.8+
- A Databricks workspace with a SQL warehouse
- A Databricks personal access token with appropriate permissions (e.g., Can Use on the warehouse)
- Ensure your SQL warehouse is running (or set to auto-start if serverless)

## Setup
1. Clone the repository:
```bash
git clone https://github.com/your-username/mcp-databricks-server.git
cd mcp-databricks-server
```

2. Create a virtual environment (recommended):

```bash
uv venv
source .venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:

```bash
uv pip install -r requirements.txt
```

4. Create a `.env` file in the root directory with your Databricks credentials:

```
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-personal-access-token
DATABRICKS_WAREHOUSE_ID=your-warehouse-id
```

- Find `DATABRICKS_HOST` and `DATABRICKS_WAREHOUSE_ID` in your Databricks UI (SQL > Warehouses > Connection details).
- Generate a token in Databricks (User Settings > Developer > Access Tokens).

## Running the Server
Run the server with:

```bash
python main.py
```

This starts the MCP server on stdio transport by default. For development/testing:
- Use `mcp dev main.py` (requires MCP CLI installed via `pip install "mcp[cli]"` if not already).
- Test with MCP Inspector: `npx @modelcontextprotocol/inspector python main.py`.

For production, consider deploying with Streamable HTTP transport (see MCP docs).

## Usage with LLMs
Once running, connect via an MCP client (e.g., Claude Desktop or custom AI app). Tools like `execute_sql_query` can be called with natural language prompts.

Example: "Run SELECT * FROM main.default.my_table" → Invokes `execute_sql_query`.

## Testing
Run the unit tests:
```bash
python -m unittest test_databricks_mcp.py
```

Update the test file with your actual host, token, and warehouse ID before running.

## Troubleshooting
- If the warehouse is stopped, start it in the Databricks UI.
- Check `.env` for correct credentials.
- For connection timeouts, increase polling timeout in `DatabricksMCP`.
- Ensure token has "Can Use" permission on the warehouse.

## Security
- Never commit your `.env` file.
- Use least-privilege tokens.
- For production, implement MCP authentication (e.g., OAuth) as per SDK docs.

For more on MCP, see https://modelcontextprotocol.io/.

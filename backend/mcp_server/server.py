"""
MCP Server for MSSQL - Fixed version with proper initialization
"""
import asyncio
import json
import os
import re
from typing import Any, Sequence
import pyodbc
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent
from dotenv import load_dotenv
from shared.connection_manager import connection_manager

load_dotenv(override=True)

# Initialize MCP Server
app = Server("mssql-server")

class MCPDatabaseTools:
    """Database tools for MCP server with RBAC support"""
    
    def __init__(self):
        """Initialize with connection manager"""
        self.connection_manager = connection_manager
    
    def _get_connection(self):
        """Get database connection"""
        return self.connection_manager.create_connection()
    
    def describe_table(self, table_name: str, schema: str = "dbo") -> dict:
        """
        Describe a table's structure including columns, types, and constraints.
        Respects user's RBAC permissions.
        """
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
            # Query to get column information
            query = """
            SELECT 
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                CASE 
                    WHEN pk.COLUMN_NAME IS NOT NULL THEN 'YES'
                    ELSE 'NO'
                END AS IS_PRIMARY_KEY
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT ku.TABLE_SCHEMA, ku.TABLE_NAME, ku.COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE ku
                    ON tc.CONSTRAINT_NAME = ku.CONSTRAINT_NAME
                WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ) pk ON c.TABLE_SCHEMA = pk.TABLE_SCHEMA 
                AND c.TABLE_NAME = pk.TABLE_NAME 
                AND c.COLUMN_NAME = pk.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ?
            ORDER BY c.ORDINAL_POSITION
            """
            
            cursor.execute(query, (schema, table_name))
            columns = []
            
            for row in cursor.fetchall():
                col_info = {
                    "name": row.COLUMN_NAME,
                    "type": row.DATA_TYPE,
                    "max_length": row.CHARACTER_MAXIMUM_LENGTH,
                    "nullable": row.IS_NULLABLE == "YES",
                    "default": row.COLUMN_DEFAULT,
                    "is_primary_key": row.IS_PRIMARY_KEY == "YES"
                }
                columns.append(col_info)
            
            if not columns:
                return {
                    "status": "error",
                    "message": f"Table {schema}.{table_name} not found or you don't have permission to access it."
                }
            
            # Get row count (if accessible)
            try:
                cursor.execute(f"SELECT COUNT(*) as row_count FROM [{schema}].[{table_name}]")
                row_count = cursor.fetchone().row_count
            except:
                row_count = "Unknown (no permission or table doesn't exist)"
            
            return {
                "status": "success",
                "schema": schema,
                "table_name": table_name,
                "columns": columns,
                "row_count": row_count
            }
            
        except pyodbc.Error as e:
            return {
                "status": "error",
                "message": f"Database error: {str(e)}"
            }
        finally:
            if cursor:
                cursor.close()
    
    def read_data(self, query: str, limit: int = 100) -> dict:
        """
        Execute a SELECT query and return results.
        Respects user's RBAC permissions.
        """
        conn = None
        cursor = None
        try:
            # Validate it's a SELECT query
            query_upper = query.strip().upper()
            if not query_upper.startswith("SELECT"):
                return {
                    "status": "error",
                    "message": "Only SELECT queries are allowed for security reasons."
                }
            
            # Check for dangerous keywords
            dangerous_keywords = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
            # Split query into potential tokens to avoid partial matches (like 'created_at' matching 'CREATE')
            tokens = set(re.split(r'[\s\n\t;()]', query_upper))
            for keyword in dangerous_keywords:
                if keyword in tokens:
                    return {
                        "status": "error",
                        "message": f"Query contains prohibited keyword: {keyword}"
                    }
            
            # Enforce limit
            limit = min(max(1, limit), 1000)
            
            # Add TOP clause if not present
            if "TOP" not in query_upper and "LIMIT" not in query_upper:
                query = query.strip()
                select_pos = query_upper.find("SELECT")
                if select_pos != -1:
                    query = query[:select_pos + 6] + f" TOP {limit}" + query[select_pos + 6:]
            
            conn = self._get_connection()
            cursor = conn.cursor()
            
            cursor.execute(query)
            
            # Get column names
            columns = [column[0] for column in cursor.description]
            
            # Fetch results
            rows = []
            for row in cursor.fetchall():
                row_dict = {}
                for idx, col_name in enumerate(columns):
                    value = row[idx]
                    # Convert datetime to string
                    if hasattr(value, 'isoformat'):
                        value = value.isoformat()
                    row_dict[col_name] = value
                rows.append(row_dict)
            
            return {
                "status": "success",
                "columns": columns,
                "row_count": len(rows),
                "rows": rows,
                "message": f"Retrieved {len(rows)} rows"
            }
            
        except pyodbc.Error as e:
            return {
                "status": "error",
                "message": f"Query execution failed: {str(e)}. You may not have permission to access this data."
            }
        finally:
            if cursor:
                cursor.close()

# Initialize database tools
db_tools = MCPDatabaseTools()

@app.list_tools()
async def list_tools() -> list[Tool]:
    """List available MCP tools"""
    return [
        Tool(
            name="describe_table",
            description="Describes the structure of a database table",
            inputSchema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Name of the table to describe"
                    },
                    "schema": {
                        "type": "string",
                        "description": "Schema name (default: 'dbo')",
                        "default": "dbo"
                    }
                },
                "required": ["table_name"]
            }
        ),
        Tool(
            name="read_data",
            description="Executes a SELECT query to read data",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "SELECT SQL query to execute"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows (1-1000, default: 100)",
                        "default": 100,
                        "minimum": 1,
                        "maximum": 1000
                    }
                },
                "required": ["query"]
            }
        )
    ]

@app.call_tool()
async def call_tool(name: str, arguments: Any) -> Sequence[TextContent]:
    """Handle tool execution requests"""
    try:
        if name == "describe_table":
            table_name = arguments.get("table_name")
            schema = arguments.get("schema", "dbo")
            
            if not table_name:
                result = {
                    "status": "error",
                    "message": "table_name is required"
                }
            else:
                result = db_tools.describe_table(table_name, schema)
            
        elif name == "read_data":
            query = arguments.get("query")
            limit = arguments.get("limit", 100)
            
            if not query:
                result = {
                    "status": "error",
                    "message": "query is required"
                }
            else:
                result = db_tools.read_data(query, limit)
        else:
            result = {
                "status": "error",
                "message": f"Unknown tool: {name}"
            }
        
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
    except Exception as e:
        error_result = {
            "status": "error",
            "message": f"Tool execution failed: {str(e)}"
        }
        return [TextContent(type="text", text=json.dumps(error_result, indent=2))]

async def main():
    """Run the MCP server"""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )

if __name__ == "__main__":
    asyncio.run(main())
"""
Direct Database Query Tool - Simplified version without MCP
This bypasses MCP complexity and directly uses the database tools
"""
import json
import pyodbc
import re  # <-- ADD THIS IMPORT
from shared.connection_manager import connection_manager

class DirectDatabaseTools:
    """Direct database access tools"""
    
    def __init__(self):
        self.connection_manager = connection_manager
    
    def _get_connection(self):
        """Get database connection"""
        return self.connection_manager.create_connection()
    
    def describe_table(self, table_name: str, schema: str = "dbo") -> dict:
        """Describe a table's structure"""
        # ... (this method is unchanged)
        conn = None
        cursor = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()
            
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
                    "message": f"Table {schema}.{table_name} not found or you don't have permission."
                }
            
            # Get row count
            try:
                cursor.execute(f"SELECT COUNT(*) as row_count FROM [{schema}].[{table_name}]")
                row_count = cursor.fetchone().row_count
            except:
                row_count = "Unknown"
            
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
        """Execute a SELECT query"""
        conn = None
        cursor = None
        try:
            # Validate SELECT only
            query_upper = query.strip().upper()
            if not query_upper.startswith("SELECT"):
                return {
                    "status": "error",
                    "message": "Only SELECT queries are allowed."
                }
            
            # --- START OF MODIFIED SECTION ---
            # Check for dangerous keywords
            dangerous = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "CREATE", "TRUNCATE"]
            # Split query into potential tokens to avoid partial matches (like 'created_at' matching 'CREATE')
            tokens = set(re.split(r'[\s\n\t;()]', query_upper))
            for keyword in dangerous:
                if keyword in tokens:
                    return {
                        "status": "error",
                        "message": f"Query contains prohibited keyword: {keyword}"
                    }
            # --- END OF MODIFIED SECTION ---
            
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
                "message": f"Query failed: {str(e)}"
            }
        finally:
            if cursor:
                cursor.close()

# Global instance
_db_tools = DirectDatabaseTools()

def query_database(action: str, table_name: str = None, schema: str = "dbo", 
                   query: str = None, limit: int = 100) -> str:
    """
    Query the database directly. Supports:
    1. Describe table structures
    2. Read data using SQL queries
    
    Args:
        action: Either 'describe' or 'read'
        table_name: Table name (for 'describe')
        schema: Schema name (default: 'dbo')
        query: SELECT query (for 'read')
        limit: Max rows (1-1000, default: 100)
    
    Returns:
        JSON string with results
    """
    try:
        if action == "describe":
            if not table_name:
                return json.dumps({
                    "status": "error",
                    "message": "table_name required for describe"
                })
            result = _db_tools.describe_table(table_name, schema)
            
        elif action == "read":
            if not query:
                return json.dumps({
                    "status": "error",
                    "message": "query required for read"
                })
            result = _db_tools.read_data(query, limit)
            
        else:
            return json.dumps({
                "status": "error",
                "message": f"Unknown action: {action}. Use 'describe' or 'read'"
            })
        
        return json.dumps(result, indent=2)
        
    except Exception as e:
        return json.dumps({
            "status": "error",
            "message": f"Error: {str(e)}"
        })
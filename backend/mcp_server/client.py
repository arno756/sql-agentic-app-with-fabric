"""
MCP Client for integrating MSSQL tools with the banking agent
"""
import json
import subprocess
import sys
from typing import Any, Dict, List
import asyncio
from contextlib import asynccontextmanager

class MCPClient:
    """Client for communicating with the MCP server"""
    
    def __init__(self):
        self.process = None
        self.reader = None
        self.writer = None
        self.request_id = 0
        
    async def start(self):
       """Start the MCP server subprocess"""
       self.process = await asyncio.create_subprocess_exec(
           sys.executable, "-m", "mcp_server.server",
           stdin=asyncio.subprocess.PIPE,
           stdout=asyncio.subprocess.PIPE,
           stderr=asyncio.subprocess.PIPE
       )
       
       self.reader = self.process.stdout
       self.writer = self.process.stdin
       
       # Send initialize request with correct format
       await self._send_request("initialize", {
           "protocolVersion": "2024-11-05",
           "capabilities": {
               "tools": {}
           },
           "clientInfo": {
               "name": "banking-agent",
               "version": "1.0.0"
           }
       })
       
       response = await self._read_response()
       if "error" in response:
           raise Exception(f"MCP initialization failed: {response['error']}")
        
    async def stop(self):
        """Stop the MCP server subprocess"""
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()
        
        if self.process:
            self.process.terminate()
            await self.process.wait()
    
    async def _send_request(self, method: str, params: Dict[str, Any]) -> int:
        """Send a JSON-RPC request to the MCP server"""
        self.request_id += 1
        request = {
            "jsonrpc": "2.0",
            "id": self.request_id,
            "method": method,
            "params": params
        }
        
        request_json = json.dumps(request) + "\n"
        self.writer.write(request_json.encode())
        await self.writer.drain()
        
        return self.request_id
    
    async def _read_response(self) -> Dict[str, Any]:
        """Read a JSON-RPC response from the MCP server"""
        line = await self.reader.readline()
        if not line:
            raise Exception("MCP server closed connection")
        
        return json.loads(line.decode())
    
    async def list_tools(self) -> List[Dict[str, Any]]:
        """List available tools from the MCP server"""
        await self._send_request("tools/list", {})
        response = await self._read_response()
        
        if "error" in response:
            raise Exception(f"Error listing tools: {response['error']}")
        
        return response.get("result", {}).get("tools", [])
    
    async def call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Any:
        """Call a tool on the MCP server"""
        await self._send_request("tools/call", {
            "name": tool_name,
            "arguments": arguments
        })
        
        response = await self._read_response()
        
        if "error" in response:
            raise Exception(f"Error calling tool {tool_name}: {response['error']}")
        
        result = response.get("result", {})
        
        # Extract the actual content from the MCP response
        if "content" in result and len(result["content"]) > 0:
            content = result["content"][0]
            if content.get("type") == "text":
                return json.loads(content.get("text", "{}"))
        
        return result

@asynccontextmanager
async def mcp_client():
    """Context manager for MCP client lifecycle"""
    client = MCPClient()
    try:
        await client.start()
        yield client
    finally:
        await client.stop()

# Synchronous wrapper functions for use in Flask routes
def describe_table_sync(table_name: str, schema: str = "dbo") -> Dict[str, Any]:
    """Synchronous wrapper for describe_table tool"""
    async def _describe():
        async with mcp_client() as client:
            return await client.call_tool("describe_table", {
                "table_name": table_name,
                "schema": schema
            })
    
    return asyncio.run(_describe())

def read_data_sync(query: str, limit: int = 100) -> Dict[str, Any]:
    """Synchronous wrapper for read_data tool"""
    async def _read():
        async with mcp_client() as client:
            return await client.call_tool("read_data", {
                "query": query,
                "limit": limit
            })
    
    return asyncio.run(_read())
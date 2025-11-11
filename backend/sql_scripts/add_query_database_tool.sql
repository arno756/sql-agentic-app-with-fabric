-- Add query_database tool definition to tool_definitions table
-- Run this on your database if the tool is missing

-- Check if it already exists
IF NOT EXISTS (SELECT 1 FROM tool_definitions WHERE name = 'query_database')
BEGIN
    INSERT INTO tool_definitions (
        tool_id,
        name,
        description,
        input_schema,
        version,
        is_active,
        cost_per_call_cents,
        created_at,
        updated_at
    )
    VALUES (
        'tooldef_' + CAST(NEWID() AS NVARCHAR(36)),
        'query_database',
        'Query the database using direct tools to describe tables or read data',
        '{"type": "object", "properties": {"action": {"type": "string", "enum": ["describe", "read"]}, "table_name": {"type": "string"}, "schema": {"type": "string"}, "query": {"type": "string"}, "limit": {"type": "integer"}}, "required": ["action"]}',
        '1.0.0',
        1,
        1,
        GETDATE(),
        GETDATE()
    );
    
    PRINT 'query_database tool definition added successfully';
END
ELSE
BEGIN
    PRINT 'query_database tool definition already exists';
END
GO
import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { CallToolRequestSchema, ListToolsRequestSchema } from '@modelcontextprotocol/sdk/types.js';
import pg from 'pg';

const { Pool } = pg;

const connectionString = process.argv[2];
if (!connectionString) {
  process.stderr.write('Usage: node index.mjs <postgresql-connection-string>\n');
  process.exit(1);
}

const pool = new Pool({ connectionString });

const server = new Server(
  { name: 'postgres-rw', version: '1.0.0' },
  { capabilities: { tools: {} } }
);

server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [{
    name: 'query',
    description: 'Run a SQL query (read and write)',
    inputSchema: {
      type: 'object',
      properties: {
        sql: { type: 'string', description: 'SQL to execute' }
      },
      required: ['sql']
    }
  }]
}));

server.setRequestHandler(CallToolRequestSchema, async (request) => {
  if (request.params.name !== 'query') throw new Error(`Unknown tool: ${request.params.name}`);
  const { sql } = request.params.arguments;
  const client = await pool.connect();
  try {
    const result = await client.query(sql);
    return {
      content: [{ type: 'text', text: JSON.stringify(result.rows, null, 2) }]
    };
  } finally {
    client.release();
  }
});

await server.connect(new StdioServerTransport());

# MCP Simple SSH Tool

A simple MCP server that exposes a website fetching tool over SSH transport.

## Usage

Start the server using either stdio (default), SSE, or SSH transport:

```bash
# Using stdio transport (default)
uv run mcp-simple-ssh-tool

# Using SSE transport on custom port
uv run mcp-simple-ssh-tool --transport sse --port 8000

# Using SSH transport on default port 8022
uv run mcp-simple-ssh-tool --transport ssh

# Using SSH transport on custom port with specific host keys
uv run mcp-simple-ssh-tool --transport ssh --ssh-port 2222 --server-host-keys /path/to/key1 --server-host-keys /path/to/key2
```

The server exposes a tool named "fetch" that accepts one required argument:

- `url`: The URL of the website to fetch

## SSH Transport Details

When using SSH transport:

- By default, it will use or generate an Ed25519 key in `~/.ssh/mcp_server_key`
- It will look for authorized client keys in `~/.ssh/authorized_keys`
- You can specify custom server host keys with `--server-host-keys` (can be used multiple times)
- You can specify a custom authorized keys file with `--authorized-client-keys`

## Client Examples

### Using the SSH transport:

```python
import asyncio
from mcp.client.session import ClientSession
from mcp.client.ssh import SSHClientParameters, ssh_client

async def main():
    async with ssh_client(
        SSHClientParameters(
            host="localhost", 
            port=8022,
            # Uncomment and specify if needed:
            # username="your_username", 
            # client_keys=["path/to/client/private_key"]
        )
    ) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print(tools)

            # Call the fetch tool
            result = await session.call_tool("fetch", {"url": "https://example.com"})
            print(result)

asyncio.run(main())
```

### Using the STDIO transport:

```python
import asyncio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client


async def main():
    async with stdio_client(
        StdioServerParameters(command="uv", args=["run", "mcp-simple-ssh-tool"])
    ) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print(tools)

            # Call the fetch tool
            result = await session.call_tool("fetch", {"url": "https://example.com"})
            print(result)


asyncio.run(main())
```

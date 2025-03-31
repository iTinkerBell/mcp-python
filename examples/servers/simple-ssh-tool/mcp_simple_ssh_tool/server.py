import anyio
import click
import httpx
import mcp.types as types
from mcp.server.lowlevel import Server


async def fetch_website(
    url: str,
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    headers = {
        "User-Agent": "MCP Test Server (github.com/modelcontextprotocol/python-sdk)"
    }
    async with httpx.AsyncClient(follow_redirects=True, headers=headers) as client:
        response = await client.get(url)
        response.raise_for_status()
        return [types.TextContent(type="text", text=response.text)]


@click.command()
@click.option("--port", default=8000, help="Port to listen on for SSE")
@click.option("--ssh-port", default=8022, help="Port to listen on for SSH")
@click.option("--host", default="0.0.0.0", help="Host address to bind to for SSH")
@click.option(
    "--transport",
    type=click.Choice(["stdio", "sse", "ssh"]),
    default="stdio",
    help="Transport type",
)
@click.option(
    "--server-host-keys",
    multiple=True,
    help="Path to SSH host key files (can be specified multiple times)"
)
@click.option(
    "--authorized-client-keys",
    help="Path to authorized client keys file"
)
def main(port: int, ssh_port: int, host: str, transport: str,
         server_host_keys: list[str], authorized_client_keys: str) -> int:
    app = Server("mcp-website-fetcher")

    @app.call_tool()
    async def fetch_tool(
        name: str, arguments: dict
    ) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
        if name != "fetch":
            raise ValueError(f"Unknown tool: {name}")
        if "url" not in arguments:
            raise ValueError("Missing required argument 'url'")
        return await fetch_website(arguments["url"])

    @app.list_tools()
    async def list_tools() -> list[types.Tool]:
        return [
            types.Tool(
                name="fetch",
                description="Fetches a website and returns its content",
                inputSchema={
                    "type": "object",
                    "required": ["url"],
                    "properties": {
                        "url": {
                            "type": "string",
                            "description": "URL to fetch",
                        }
                    },
                },
            )
        ]

    if transport == "sse":
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        sse = SseServerTransport("/messages/")

        async def handle_sse(request):
            async with sse.connect_sse(
                request.scope, request.receive, request._send
            ) as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        starlette_app = Starlette(
            debug=True,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
        )

        import uvicorn

        uvicorn.run(starlette_app, host=host, port=port)
    elif transport == "ssh":
        from mcp.server.ssh import ssh_server

        async def arun():
            host_keys = server_host_keys if server_host_keys else None
            async with ssh_server(
                host=host,
                port=ssh_port,
                server_host_keys=host_keys,
                authorized_client_keys=authorized_client_keys
            ) as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        print(f"Starting SSH server on {host}:{ssh_port}")
        anyio.run(arun)
    else:
        from mcp.server.stdio import stdio_server

        async def arun():
            async with stdio_server() as streams:
                await app.run(
                    streams[0], streams[1], app.create_initialization_options()
                )

        anyio.run(arun)

    return 0

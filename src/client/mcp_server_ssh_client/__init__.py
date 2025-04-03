import argparse
import logging

import anyio
from mcp.server.stdio import stdio_server
from mcp.client.session import ClientSession

from .proxy_server import create_proxy_server
from .ssh_client import SSHServerParameters, ssh_client

def main() -> None:
    """M2M Remote MCP Client - Use tools Hosted Remotely Over SSH"""
    parser = argparse.ArgumentParser(description=
                  "M2M Remote MCP Client - Use tools Hosted Remotely Over SSH")

    # Add arguments
    parser.add_argument("--host", default="localhost",
                        help="Remote SSH server host")
    parser.add_argument("--port", type=int, default=8022,
                        help="Remote SSH server port")
    parser.add_argument("--username", default="mcp", help="SSH username")
    parser.add_argument("--client-key", default="~/.ssh/id_rsa",
                        help="Path to client private key file")
    parser.add_argument("--known-hosts", default="~/.ssh/known_hosts",
                        help="Path to known hosts file")
    parser.add_argument("--passphrase", default=None,
                        help="Passphrase for the private key")
    parser.add_argument("--encoding", default="utf-8",
                        help="The text encoding for communication")
    parser.add_argument("--encoding-error-handler", default="strict",
                        choices=["strict", "ignore", "replace"],
                        help="The text encoding error handler")
    parser.add_argument("--disable-host-key-checking", action="store_true",
                        help="Disable host key checking (insecure, use only for development)")
    parser.add_argument("--log-level", default="DEBUG",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Set the logging level")

    # Parse arguments
    args = parser.parse_args()

    # Configure logging with more details
    logging_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    logging.basicConfig(level=getattr(logging, args.log_level),
                        format=logging_format)
    logger = logging.getLogger(__name__)
    logger.debug("MCP SSH Client starting with debug logging enabled")

    # Start the server
    async def arun():
        logger.debug(f"Connecting to SSH server at {args.host}:{args.port}")
        params = SSHServerParameters(
                host=args.host,
                port=args.port,
                username=args.username,
                client_keys=args.client_key,
                known_hosts=args.known_hosts,
                passphrase=args.passphrase,
                encoding=args.encoding,
                encoding_error_handler=args.encoding_error_handler,
                disable_host_key_checking=args.disable_host_key_checking
            )
        logger.debug(f"Connection parameters: {params}")
        async with ssh_client(params) as streams:
            logger.debug("SSH client connection established, creating session")
            async with ClientSession(*streams) as session:
                logger.debug("Creating proxy server")
                app = await create_proxy_server(session)
                logger.debug(f"Proxy server created for {app.name}")
                async with stdio_server() as (read_stream, write_stream):
                    logger.debug("Starting server run loop")
                    await app.run(
                        read_stream,
                        write_stream,
                        app.create_initialization_options(),
                    )

    # Run the async function
    anyio.run(arun)


if __name__ == "__main__":
    main()

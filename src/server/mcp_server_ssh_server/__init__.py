import argparse
import logging

import anyio

from .ssh_server import run_ssh_server


def main() -> None:
    """M2M Remote MCP Server - Serve tools Hosted Locally Over SSH"""
    parser = argparse.ArgumentParser(description=
                  "M2M Remote MCP Server - Serve Tools Locally Over SSH")

    # Add arguments
    parser.add_argument("--host", default="0.0.0.0",
                        help="SSH server host address to bind to")
    parser.add_argument("--port", type=int, default=8022,
                        help="SSH server port to listen on")
    parser.add_argument("--authorized-clients",
                        default="~/.ssh/authorized_keys",
                        help="Authorized clients file")
    parser.add_argument("--server-key", default="~/.ssh/mcp_server_key",
                        help="Path to server private key file")
    parser.add_argument("--passphrase", default=None,
                        help="Passphrase for the private key")
    parser.add_argument("--encoding", default="utf-8",
                        help="The text encoding for communication")
    parser.add_argument("--encoding-error-handler", default="strict",
                        choices=["strict", "ignore", "replace"],
                        help="The text encoding error handler")
    parser.add_argument("--servers-config",
                        default="servers_config.json",
                        help="Path to server configurations JSON")
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
    logger.debug("MCP SSH Server starting with debug logging enabled")

    # Run the async function
    async def run_server():
        logger.debug(f"Starting SSH server on {args.host}:{args.port}")
        return await run_ssh_server(
            host=args.host,
            port=args.port,
            server_host_keys=args.server_key,
            authorized_client_keys=args.authorized_clients,
            passphrase=args.passphrase,
            servers_config=args.servers_config,
        )

    anyio.run(run_server)


if __name__ == "__main__":
    main()

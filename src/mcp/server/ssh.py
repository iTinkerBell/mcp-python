"""
SSH Server Transport Module

This module provides functionality for creating an SSH-based transport layer
that can be used to communicate with an MCP client through an SSH connection.

Example usage:
```
    async def run_server():
        async with ssh_server(host='0.0.0.0', port=8022) as (read_stream, write_stream):
            # read_stream contains incoming JSONRPCMessages from SSH clients
            # write_stream allows sending JSONRPCMessages to SSH clients
            server = await create_my_server()
            await server.run(read_stream, write_stream, init_options)

    anyio.run(run_server)
```
"""

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

import anyio
import asyncssh
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic_core import ValidationError

import mcp.types as types

logger = logging.getLogger(__name__)


class McpSSHServer(asyncssh.SSHServer):
    """
    SSH server implementation for MCP connections.
    """
    def __init__(self):
        self.conn = None

    def connection_made(self, conn: asyncssh.SSHServerConnection):
        """Called when a connection is established"""
        self.conn = conn
        logger.info(f"SSH connection established from {conn.get_extra_info('peername')[0]}")

    def connection_lost(self, exc: Optional[BaseException]):
        """Called when a connection is closed"""
        if exc:
            logger.error(f"SSH connection error: {str(exc)}")
        logger.info("SSH connection closed")


@asynccontextmanager
async def ssh_server(host: str = '0.0.0.0',
                     port: int = 8022,
                     server_host_keys: Optional[list[str]] = None,
                     authorized_client_keys: Optional[str] = None):
    """
    Server transport for SSH: this communicates with an MCP client by accepting
    SSH connections and processing MCP messages over the connection.

    Args:
        host: The host address to bind to
        port: The port to listen on
        server_host_keys: List of paths to SSH host key files
        authorized_client_keys: Path to authorized keys file
    """
    if not server_host_keys:
        # Generate a default key if none provided
        default_private_key_path = Path.home() / ".ssh" / "mcp_server_key"
        if not default_private_key_path.exists():
            try:
                ssh_key = asyncssh.generate_private_key(alg_name='ssh-ed25519')

                # Save the private key
                with open(default_private_key_path, 'wb') as f:
                    f.write(ssh_key.export_private_key())

                # Save the public key (optional)
                default_public_key_path = default_private_key_path.with_suffix('.pub')
                with open(default_public_key_path, 'wb') as f:
                    f.write(ssh_key.export_public_key())

                print(f"Ed25519 key pair generated and saved to: {default_private_key_path}")

            except Exception as e:
                print(f"Error generating Ed25519 key: {e}")
        server_host_keys = [str(default_private_key_path)]

    if not authorized_client_keys:
        # Default to standard authorized_keys location
        authorized_client_keys = [str(Path.home() / ".ssh" / "authorized_keys")]

    read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[types.JSONRPCMessage | Exception]

    write_stream: MemoryObjectSendStream[types.JSONRPCMessage]
    write_stream_reader: MemoryObjectReceiveStream[types.JSONRPCMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    class ConnectionHandler(asyncssh.SSHServer):
        def connection_made(self, conn: asyncssh.SSHServerConnection):
            """Called when a connection is established"""
            logger.info(f"SSH connection established from {conn.get_extra_info('peername')[0]}")

        def connection_lost(self, exc: Optional[BaseException]):
            """Called when a connection is closed"""
            if exc:
                logger.error(f"SSH connection error: {str(exc)}")
            logger.info("SSH connection closed")

        def session_requested(self):
            """Handle a new session request"""
            return SSHSessionHandler()

    class SSHSessionHandler(asyncssh.SSHServerSession):
        def __init__(self):
            self._chan = None

        def connection_made(self, chan):
            """Called when a connection is made"""
            self._chan = chan

        def session_started(self):
            """Called when the session starts"""
            remote_addr = self._chan.get_extra_info('peername')[0]
            logger.info(f"SSH session started from {remote_addr}")

            # Start processing the connection
            anyio.create_task(process_connection(self._chan))

        def connection_lost(self, exc):
            """Called when the connection is lost"""
            if exc:
                logger.error(f"SSH session error: {exc}")
            logger.info("SSH session closed")

    async def process_connection(chan):
        """Handle a single SSH connection"""
        remote_addr = chan.get_extra_info('peername')[0]
        logger.info(f"Client connected: {remote_addr}")

        async def ssh_reader():
            """Read JSON-RPC messages from the SSH connection"""
            try:
                async with read_stream_writer:
                    async for line in chan.stdin:
                        line = line.rstrip('\n')
                        try:
                            message = types.JSONRPCMessage.model_validate_json(line)
                        except ValidationError as exc:
                            await read_stream_writer.send(exc)
                            continue

                        await read_stream_writer.send(message)
            except anyio.ClosedResourceError:
                logger.debug("SSH reader stream closed")
            except Exception as e:
                logger.error(f"Error in SSH reader: {str(e)}")
                await read_stream_writer.send(Exception(f"SSH transport error: {str(e)}"))

        async def ssh_writer():
            """Write JSON-RPC messages to the SSH connection"""
            try:
                async with write_stream_reader:
                    async for message in write_stream_reader:
                        json = message.model_dump_json(by_alias=True, exclude_none=True)
                        chan.stdout.write(json + '\n')
                        await chan.stdout.drain()
            except anyio.ClosedResourceError:
                logger.debug("SSH writer stream closed")
            except Exception as e:
                logger.error(f"Error in SSH writer: {str(e)}")

        async with anyio.create_task_group() as tg:
            tg.start_soon(ssh_reader)
            tg.start_soon(ssh_writer)

            # Wait for channel closure
            await chan.wait_closed()

    shutdown_event = anyio.Event()

    async def connection_handler():
        """Handle incoming connections continuously"""
        try:
            server = await asyncssh.create_server(
                ConnectionHandler,
                host,
                port,
                server_host_keys=server_host_keys,
                authorized_client_keys=authorized_client_keys
            )

            logger.info(f"SSH server listening on {host}:{port}")

            async with server:
                # Keep the server running until shutdown
                while not shutdown_event.is_set():
                    await anyio.sleep(1)

        except (OSError, asyncssh.Error) as exc:
            logger.error(f"Error starting SSH server: {exc}")

    async with anyio.create_task_group() as server_tg:
        server_tg.start_soon(connection_handler)
        try:
            yield read_stream, write_stream
        finally:
            # Signal shutdown
            shutdown_event.set()

            # Cancel the server task group
            server_tg.cancel_scope.cancel()

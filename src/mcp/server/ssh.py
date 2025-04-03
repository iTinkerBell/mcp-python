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

import asyncio
from contextlib import asynccontextmanager
import logging
from pathlib import Path
from typing import Mapping, Optional, List, Tuple

import anyio
import asyncssh
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream

import mcp.types as types

logger = logging.getLogger(__name__)

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
                logger.error(f"Error generating Ed25519 key: {e}")
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

    # Track active connections and channels
    active_sessions: List["SSHSessionHandler"] = []

    class SSHServerHandler(asyncssh.SSHServer):
        def connection_made(self, conn: asyncssh.SSHServerConnection):
            """Called when a connection is established"""
            print(f"SSH connection established from {conn.get_extra_info('peername')[0]}")

        def connection_lost(self, exc: Optional[BaseException]):
            """Called when a connection is closed"""
            if exc:
                logger.error(f"SSH connection error: {str(exc)}")
            print("SSH connection closed")

        def session_requested(self):
            """Handle a new session request"""
            return SSHSessionHandler()

    class SSHSessionHandler(asyncssh.SSHServerSession[str]):
        def __init__(self):
            self._chan: Optional[asyncssh.SSHServerChannel[str]] = None
            self._input_buffer = ''
            self._pending_lines = []
            self._line_available = asyncio.Event()
            active_sessions.append(self)
            print(f"New SSH session created: {len(active_sessions)} active sessions")

        def connection_made(self, chan: asyncssh.SSHServerChannel[str]):
            """Called when a connection is made"""
            self._chan = chan
            remote_addr = self._chan.get_extra_info('peername')[0]
            print(f"Connection made from {remote_addr}")
            for session in active_sessions:
                print(f"Active session channel: {session._chan}")

        def shell_requested(self) -> bool:
            """Handle shell requests"""
            print("Shell requested")
            return True

        def pty_requested(self, term_type: str,
                      term_size: Tuple[int, int, int, int],
                      term_modes: Mapping[int, int]) -> bool:
            """Handle pseudo-terminal requests"""
            print("Pseudo-terminal requested")
            return False

        def session_started(self):
            """Called when the session starts"""
            if self._chan is None:
                logger.error("SSH channel is None during session start")
                return

            remote_addr = self._chan.get_extra_info('peername')[0]
            print(f"SSH session started from {remote_addr}")

            # Start processing the connection
            asyncio.create_task(self.process_session())

        def data_received(self, data: str, datatype: asyncssh.DataType):
            """Called when data is received on the channel"""
            if self._chan is None:
                logger.error("SSH channel is None during data reception")
                return

            # Add the received data to the input buffer
            self._input_buffer += data

            # Process any complete lines
            lines = self._input_buffer.splitlines(keepends=True)
            if lines:
                # If the last line doesn't end with a newline, keep it in the buffer
                if not lines[-1].endswith('\n'):
                    self._input_buffer = lines.pop()
                else:
                    self._input_buffer = ''

                # Add complete lines to the pending lines queue
                for line in lines:
                    self._pending_lines.append(line.rstrip('\n'))

                # Signal that new lines are available
                self._line_available.set()

        async def readline(self):
            """Read a line asynchronously from the input buffer"""
            while not self._pending_lines:
                # Wait for new data
                self._line_available.clear()
                await self._line_available.wait()

            # Return the next available line
            return self._pending_lines.pop(0)

        def connection_lost(self, exc):
            """Called when the connection is lost"""
            if exc:
                logger.error(f"SSH session error: {exc}")
            print("SSH session closed")

            if self._chan is not None:
                self._chan.close()

            # Remove from active sessions
            if self in active_sessions:
                active_sessions.remove(self)

            # Signal the read loop to exit
            self._line_available.set()

        async def process_session(self):
            """Process the SSH session"""
            if self._chan is None:
                logger.error("SSH channel is None during process_session")
                return

            remote_addr = self._chan.get_extra_info('peername')[0]
            print(f"Processing session from: {remote_addr}")

            try:
                async def ssh_reader():
                    """Read JSON-RPC messages from the SSH connection"""
                    try:
                        while True:
                            line = await self.readline()
                            if not line:
                                continue

                            try:
                                message = types.JSONRPCMessage.model_validate_json(line)
                                print(f"Received message: {message}")
                                await read_stream_writer.send(message)
                            except Exception as exc:
                                logger.error(f"Error parsing message: {exc}")
                                await read_stream_writer.send(exc)
                    except asyncio.CancelledError:
                        logger.debug("SSH reader task cancelled")
                    except Exception as e:
                        logger.error(f"Error in SSH reader: {str(e)}")
                        await read_stream_writer.send(Exception(f"SSH transport error: {str(e)}"))

                async def ssh_writer():
                    """Write JSON-RPC messages to the SSH connection"""
                    try:
                        async for message in write_stream_reader:
                            # Check if the channel is still open before writing
                            if self._chan is None or self._chan.is_closing():
                                logger.warning("Cannot send message: SSH channel is closed")
                                continue

                            json = message.model_dump_json(by_alias=True, exclude_none=True)
                            print(f"Sending message: {json}")
                            self._chan.write(json + '\n')
                    except asyncio.CancelledError:
                        logger.debug("SSH writer task cancelled")
                    except Exception as e:
                        logger.error(f"Error in SSH writer: {str(e)}")

                # Create tasks for reading and writing
                async with anyio.create_task_group() as tg:
                    tg.start_soon(ssh_reader)
                    tg.start_soon(ssh_writer)

                    # Wait for channel closure
                    await self._chan.wait_closed()

            except Exception as e:
                logger.error(f"Session processing error: {e}")

    shutdown_event = anyio.Event()

    async def connection_handler():
        """Handle incoming connections continuously"""
        try:
            server = await asyncssh.create_server(
                SSHServerHandler,
                host,
                port,
                server_host_keys=server_host_keys,
                authorized_client_keys=authorized_client_keys
            )

            print(f"SSH server listening on {host}:{port}")

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

            # # Clean up active sessions
            # for session in active_sessions[:]:
            #     if session._chan:
            #         session._chan.close()

            # Cancel the server task group
            server_tg.cancel_scope.cancel()

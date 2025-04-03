"""
SSH Client Transport Module

This module provides functionality for connecting to an MCP server through SSH.

Example usage:
async def run_client():
    params = SSHServerParameters(
        host='example.com',
        port=8022,
        username='user'
    )

    async with ssh_client(params) as (read_stream, write_stream):
        # read_stream contains incoming JSONRPCMessages from the server
        # write_stream allows sending JSONRPCMessages to the server
        client = await create_my_client()
        await client.run(read_stream, write_stream)

anyio.run(run_client)
"""

import sys
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Optional, TextIO, Literal, Union

import anyio
import anyio.lowlevel
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
import asyncssh
from pydantic import BaseModel, Field

import mcp.types as types

logger = logging.getLogger(__name__)


class SSHServerParameters(BaseModel):
    """Parameters for establishing an SSH connection to an MCP server."""

    env: dict[str, str] | None = None
    """
    Environment variables to set for the server process.

    If None, uses the default environment variables.
    """

    host: str = "localhost"
    """The hostname or IP address of the SSH server."""

    port: int = 8022
    """The port number of the SSH server, defaults to 8022."""

    username: str = 'mcp'
    """The username for authentication. If empty, uses 'mcp'."""

    client_keys: Optional[Union[str, list[str]]] = None
    """Paths to the client private key files."""

    known_hosts: Optional[Union[str, list[str]]] = None
    """Path to known_hosts file or list of host keys."""

    passphrase: Optional[str] = None
    """Passphrase if the private key is encrypted."""

    encoding: str = "utf-8"
    """The text encoding used when sending/receiving messages to the server."""

    encoding_error_handler: Literal["strict", "ignore", "replace"] = "strict"
    """
    The text encoding error handler.

    See https://docs.python.org/3/library/codecs.html#codec-base-classes for
    explanations of possible values.
    """

    disable_host_key_checking: bool = False
    """Whether to disable host key checking (use only for development/testing)."""


@asynccontextmanager
async def ssh_client(params: SSHServerParameters, errlog: TextIO = sys.stderr):
    """
    Client transport for SSH: this connects to an MCP server over SSH.

    Args:
        params: SSH connection parameters
        errlog: Stream for error logging

    Yields:
        A tuple of (read_stream, write_stream) for message exchange with the server
    """
    logger.debug(f"SSH client parameters: {params}")
    read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[types.JSONRPCMessage | Exception]

    write_stream: MemoryObjectSendStream[types.JSONRPCMessage]
    write_stream_reader: MemoryObjectReceiveStream[types.JSONRPCMessage]

    logger.debug("Creating memory object streams")
    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    # Set up default client keys if not provided
    if not params.client_keys:
        default_key_path = Path.home() / ".ssh" / "id_rsa"
        logger.debug(f"No client keys provided, checking for default key at {default_key_path}")
        if default_key_path.exists():
            logger.debug(f"Using default client key: {default_key_path}")
            params.client_keys = [str(default_key_path)]
        else:
            logger.debug("No default client key found")

    username = params.username
    logger.debug(f"Using SSH username: {username}")

    try:
        # Establish SSH connection with host key checking options
        conn_options = {
            'host': params.host,
            'port': params.port,
            'username': username,
            'client_keys': params.client_keys if params.client_keys else None,
            'passphrase': params.passphrase
        }

        # Set known_hosts policy based on disable_host_key_checking flag
        if params.disable_host_key_checking:
            conn_options['known_hosts'] = None
            logger.warning("Host key checking is disabled (insecure)")
        else:
            conn_options['known_hosts'] = params.known_hosts
            logger.debug(f"Using known_hosts: {params.known_hosts}")

        logger.debug(f"Establishing SSH connection with options: {conn_options}")
        conn = await asyncssh.connect(**conn_options)
        logger.info(f"SSH connection established to {params.host}:{params.port}")
        logger.debug(f"Connection details: {conn.get_extra_info('peer_addr')}:{conn.get_extra_info('peer_port')}")
        logger.debug(f"Server version: {conn.get_extra_info('server_version', 'unknown')}")

        # Open a session using the open_session method which returns stdin, stdout, stderr
        logger.debug("Opening SSH session")
        stdin, stdout, stderr = await conn.open_session()
        logger.debug(f"STDERR from connection: {stderr.read() if stderr else 'None'}")

        logger.info(f"SSH session established to {params.host}:{params.port}")

        async def ssh_reader():
            """Read JSON-RPC messages from the SSH connection."""
            try:
                logger.debug("SSH reader started")
                async with read_stream_writer:
                    buffer = ""
                    async for chunk in stdout:
                        logger.debug(f"Received chunk: {chunk[:100]}{'...' if len(chunk)>100 else ''}")
                        lines = (buffer + chunk).split("\n")
                        buffer = lines.pop()
                        logger.debug(f"Split into {len(lines)} lines, buffer: {buffer[:50]}")

                        for line in lines:
                            logger.debug(f"Processing line: {line[:100]}{'...' if len(line)>100 else ''}")
                            try:
                                message = types.JSONRPCMessage.model_validate_json(line)
                                logger.debug(f"Parsed message: {message}")
                                await read_stream_writer.send(message)
                            except Exception as exc:
                                logger.error(f"Error parsing JSON-RPC: {exc}")
                                logger.debug(f"Invalid JSON: {line[:200]}")
                                await read_stream_writer.send(exc)

                    # Process any remaining data in buffer
                    if buffer:
                        logger.debug(f"Processing remaining buffer: {buffer[:100]}")
                        try:
                            message = types.JSONRPCMessage.model_validate_json(buffer)
                            logger.debug(f"Parsed final message: {message}")
                            await read_stream_writer.send(message)
                        except Exception as exc:
                            logger.debug(f"Error parsing final buffer: {exc}")
                            pass

            except anyio.ClosedResourceError:
                logger.debug("Resource closed in ssh_reader")
                await anyio.lowlevel.checkpoint()
            except Exception as e:
                logger.error(f"SSH reader error: {str(e)}")
                logger.debug(f"SSH reader error details:", exc_info=True)
                await read_stream_writer.send(Exception(f"SSH transport error: {str(e)}"))

        async def ssh_writer():
            """Write JSON-RPC messages to the SSH connection."""
            try:
                logger.debug("SSH writer started")
                async with write_stream_reader:
                    async for message in write_stream_reader:
                        json = message.model_dump_json(by_alias=True, exclude_none=True)
                        logger.debug(f"Sending JSON: {json[:200]}")
                        stdin.write(json + "\n")
                        await stdin.drain()
                        logger.debug("Message sent and drained")
            except anyio.ClosedResourceError:
                logger.debug("Resource closed in ssh_writer")
                await anyio.lowlevel.checkpoint()
            except Exception as e:
                logger.error(f"SSH writer error: {str(e)}")
                logger.debug(f"SSH writer error details:", exc_info=True)

        logger.debug("Starting reader and writer tasks")
        async with anyio.create_task_group() as tg:
            tg.start_soon(ssh_reader)
            tg.start_soon(ssh_writer)
            try:
                logger.debug("Yielding streams to caller")
                yield read_stream, write_stream
            finally:
                logger.debug("Context exiting, closing SSH connection")
                stdin.close()
                conn.close()
                logger.info("SSH connection closed")

    except Exception as e:
        logger.error(f"SSH connection error: {str(e)}")
        logger.debug(f"SSH connection error details:", exc_info=True)
        raise

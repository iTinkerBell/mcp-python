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
from typing import Optional, TextIO, Literal, Union

import anyio
import anyio.lowlevel
import asyncssh
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
from pydantic import BaseModel, Field

import mcp.types as types

logger = logging.getLogger(__name__)


class SSHServerParameters(BaseModel):
    """Parameters for establishing an SSH connection to an MCP server."""

    command: str
    """The executable to run to start the server."""

    args: list[str] = Field(default_factory=list)
    """Command line arguments to pass to the executable."""

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

    @classmethod
    def from_args(cls, command: str, args: list[str]) -> "SSHServerParameters":
        """
        Create SSHServerParameters from command-line arguments.

        Args:
            command: The executable to run
            args: Command line arguments (e.g., ["--host", "localhost", "--port=8022"])

        Returns:
            Configured SSHServerParameters instance
        """
        params: dict[str, Any] = {"command": command}

        # Process the arguments
        i = 0
        while i < len(args):
            arg = args[i]

            # Handle --option=value format
            if arg.startswith("--") and "=" in arg:
                key, value = arg[2:].split("=", 1)
                params[key] = value
                i += 1

            # Handle --option value format
            elif arg.startswith("--"):
                key = arg[2:]
                key = key.replace("-", "_")  # Convert to snake_case
                if i + 1 < len(args) and not args[i + 1].startswith("--"):
                    value = args[i + 1]
                    if key == "port":
                        value = int(value)
                    params[key] = value
                    i += 2
                else:
                    # Flag without value
                    params[key] = True
                    i += 1
            else:
                # This is a positional argument - add to the args list
                if "args" not in params:
                    params["args"] = []
                params["args"].append(arg)
                i += 1

        return cls(**params)


    @classmethod
    def from_args_and_env(cls, command: str, args: list[str], env: Optional[dict[str, str]] = None) -> "SSHServerParameters":
        """
        Create SSHServerParameters from command-line arguments and environment variables.

        Args:
            command: The executable to run
            args: Command line arguments (e.g., ["--host", "localhost", "--port=8022"])
            env: Environment variables to set for the server process

        Returns:
            Configured SSHServerParameters instance
        """
        # First parse the command-line arguments
        params = cls.from_args(command, args)

        # Then update with environment variables if provided
        if env is not None:
            params.env = env

        return params


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
    print(params)
    read_stream: MemoryObjectReceiveStream[types.JSONRPCMessage | Exception]
    read_stream_writer: MemoryObjectSendStream[types.JSONRPCMessage | Exception]

    write_stream: MemoryObjectSendStream[types.JSONRPCMessage]
    write_stream_reader: MemoryObjectReceiveStream[types.JSONRPCMessage]

    read_stream_writer, read_stream = anyio.create_memory_object_stream(0)
    write_stream, write_stream_reader = anyio.create_memory_object_stream(0)

    # Set up default client keys if not provided
    if not params.client_keys:
        default_key_path = Path.home() / ".ssh" / "id_rsa"
        if default_key_path.exists():
            params.client_keys = [str(default_key_path)]

    username = params.username

    try:
        # Establish SSH connection
        conn = await asyncssh.connect(
            host=params.host,
            port=params.port,
            username=username,
            client_keys=params.client_keys if params.client_keys else None,
            known_hosts=params.known_hosts,
            passphrase=params.passphrase
        )
        logger.info(f"SSH connection established to {params.host}:{params.port}")

        # Open a session using the open_session method which returns stdin, stdout, stderr
        stdin, stdout, stderr = await conn.open_session()

        logger.info(f"SSH session established to {params.host}:{params.port}")

        async def ssh_reader():
            """Read JSON-RPC messages from the SSH connection."""
            try:
                async with read_stream_writer:
                    buffer = ""
                    async for chunk in stdout:
                        lines = (buffer + chunk).split("\n")
                        buffer = lines.pop()

                        for line in lines:
                            try:
                                message = types.JSONRPCMessage.model_validate_json(line)
                                print(f"Received message: {message}")
                                await read_stream_writer.send(message)
                            except Exception as exc:
                                logger.error(f"Error parsing JSON-RPC: {exc}")
                                await read_stream_writer.send(exc)

                    # Process any remaining data in buffer
                    if buffer:
                        try:
                            message = types.JSONRPCMessage.model_validate_json(buffer)
                            await read_stream_writer.send(message)
                        except Exception:
                            pass

            except anyio.ClosedResourceError:
                await anyio.lowlevel.checkpoint()
            except Exception as e:
                logger.error(f"SSH reader error: {str(e)}")
                await read_stream_writer.send(Exception(f"SSH transport error: {str(e)}"))

        async def ssh_writer():
            """Write JSON-RPC messages to the SSH connection."""
            try:
                async with write_stream_reader:
                    async for message in write_stream_reader:
                        json = message.model_dump_json(by_alias=True, exclude_none=True)
                        print(f"Sending message: {json}")
                        stdin.write(json + "\n")
                        await stdin.drain()
            except anyio.ClosedResourceError:
                await anyio.lowlevel.checkpoint()
            except Exception as e:
                logger.error(f"SSH writer error: {str(e)}")

        async with anyio.create_task_group() as tg:
            tg.start_soon(ssh_reader)
            tg.start_soon(ssh_writer)
            try:
                yield read_stream, write_stream
            finally:
                stdin.close()
                conn.close()
                logger.info("SSH connection closed")

    except Exception as e:
        print(f"SSH connection error: {str(e)}", file=errlog)
        raise

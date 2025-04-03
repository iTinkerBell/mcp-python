import asyncio
import json
import logging
import uuid
from typing import Dict, List, Mapping, Optional, Tuple, Any

import anyio
import asyncssh

import mcp.types as types
from mcp import server
from mcp.server.stdio import stdio_server

from .proxy_server import create_merged_proxy_server, Server

logger = logging.getLogger(__name__)

class SSHSessionHandler(asyncssh.SSHServerSession[str]):
    def __init__(self, server_configs_path: str):
        self._chan: Optional[asyncssh.SSHServerChannel[str]] = None
        self._input_buffer = ''
        self._pending_lines = []
        self._line_available = asyncio.Event()
        self._session_id = str(uuid.uuid4())
        self._server_configs_path = server_configs_path
        self._servers: List[Server] = []
        self._merged_server: Optional[server.Server[object]] = None
        logger.debug(f"SSH session handler {self._session_id} initialized")

    def connection_made(self, chan: asyncssh.SSHServerChannel[str]):
        """Called when a connection is made"""
        self._chan = chan
        remote_addr = self._chan.get_extra_info('peername')[0]
        logger.info(f"Connection made from {remote_addr}, session ID: {self._session_id}")
        logger.debug(f"SSH channel properties: {self._chan.get_extra_info('connection')}")

    def shell_requested(self) -> bool:
        """Handle shell requests"""
        logger.info(f"Shell requested for session {self._session_id}")
        logger.debug(f"Shell environment: {self._chan.get_environment() if self._chan else 'No channel'}")
        # Return True to accept the shell request
        # The actual handling is done in session_started
        return True

    def pty_requested(self, term_type: str, term_size: Tuple[int, int, int, int],
                     term_modes: Mapping[int, int]) -> bool:
        """Handle pseudo-terminal requests"""
        logger.info(f"Pseudo-terminal requested for session {self._session_id}")
        logger.debug(f"Terminal type: {term_type}, size: {term_size}, modes: {term_modes}")
        return False

    def session_started(self):
        """Called when the session starts"""
        if self._chan is None:
            logger.error("SSH channel is None during session start")
            return

        remote_addr = self._chan.get_extra_info('peername')[0]
        logger.info(f"SSH session {self._session_id} started from {remote_addr}")
        logger.debug(f"Session protocol version: {self._chan.get_extra_info('server_version', 'unknown')}")

        # Start processing the connection
        asyncio.create_task(self.process_session())

    def data_received(self, data: str, datatype: asyncssh.DataType):
        """Called when data is received on the channel"""
        if self._chan is None:
            logger.error("SSH channel is None during data reception")
            return

        # Add the received data to the input buffer
        logger.debug(f"Session {self._session_id} received data: {data[:100]}{'...' if len(data) > 100 else ''}")
        self._input_buffer += data

        # Process any complete lines
        lines = self._input_buffer.splitlines(keepends=True)
        if lines:
            # If the last line doesn't end with a newline, keep it in the buffer
            if not lines[-1].endswith('\n'):
                self._input_buffer = lines.pop()
                logger.debug(f"Keeping incomplete line in buffer: {self._input_buffer[:100]}")
            else:
                self._input_buffer = ''

            # Add complete lines to the pending lines queue
            for line in lines:
                clean_line = line.rstrip('\n')
                self._pending_lines.append(clean_line)
                logger.debug(f"Added complete line to pending queue: {clean_line[:100]}")

            # Signal that new lines are available
            logger.debug(f"Signaling {len(lines)} new lines available")
            self._line_available.set()

    async def readline(self):
        """Read a line asynchronously from the input buffer"""
        logger.debug(f"Readline called, pending lines: {len(self._pending_lines)}")
        while not self._pending_lines:
            # Wait for new data
            logger.debug("Waiting for new lines...")
            self._line_available.clear()
            await self._line_available.wait()
            logger.debug(f"New lines available, count: {len(self._pending_lines)}")

        # Return the next available line
        line = self._pending_lines.pop(0)
        logger.debug(f"Returning line: {line[:100]}")
        return line

    def connection_lost(self, exc):
        """Called when the connection is lost"""
        if exc:
            logger.error(f"SSH session {self._session_id} error: {exc}")
            logger.debug(f"Connection lost exception details: {type(exc).__name__}: {str(exc)}")
        logger.info(f"SSH session {self._session_id} closed")

        if self._chan is not None:
            logger.debug(f"Closing SSH channel for session {self._session_id}")
            self._chan.close()

        # Clean up client sessions
        logger.debug(f"Cleaning up {len(self._servers)} server connections")
        for server in self._servers:
            asyncio.create_task(server.cleanup())

        # Signal the read loop to exit
        logger.debug("Signaling read loop to exit")
        self._line_available.set()

    async def load_server_configs(self) -> Dict[str, Any]:
        """Load server configurations from file"""
        try:
            logger.debug(f"Loading server configs from {self._server_configs_path}")
            with open(self._server_configs_path, 'r') as f:
                config = json.load(f)
                logger.debug(f"Loaded server configs: {len(config.get('mcpServers', {}))}")
                return config
        except Exception as e:
            logger.error(f"Error loading server configs: {e}")
            logger.debug(f"Stack trace for config loading error:", exc_info=True)
            return {"mcpServers": {}}

    async def initialize_servers(self) -> None:
        """Initialize MCP proxy servers from configuration"""
        try:
            logger.debug("Beginning server initialization...")
            config = await self.load_server_configs()

            for name, srv_config in config.get("mcpServers", {}).items():
                logger.info(f"Initializing proxy server for {name}")
                logger.debug(f"Server config for {name}: {srv_config}")
                # Create a new server
                server = Server(name, srv_config)
                await server.initialize()
                self._servers.append(server)
                logger.debug(f"Server {name} initialized successfully")

            logger.debug("Creating merged proxy server")
            self._merged_server = await create_merged_proxy_server(self._servers)
            logger.info(f"Merged proxy server initialized with {len(self._servers)} servers")

        except Exception as e:
            logger.error(f"Error initializing proxy servers: {e}")
            logger.debug(f"Stack trace for initialization error:", exc_info=True)

    async def pipe_streams(self, src_stream, dst_stream):
        """Pipe messages from source stream to destination stream"""
        try:
            logger.debug(f"Starting pipe_streams from {src_stream} to {dst_stream}")
            async for message in src_stream:
                logger.debug(f"Piping message: {type(message).__name__}")
                await dst_stream.send(message)
        except Exception as e:
            logger.error(f"Error in pipe_streams: {e}")
            logger.debug(f"Stack trace for pipe_streams error:", exc_info=True)

    async def process_session(self):
        """Process the SSH session"""
        if self._chan is None:
            logger.error("SSH channel is None during process_session")
            return

        remote_addr = self._chan.get_extra_info('peername')[0]
        logger.info(f"Processing session {self._session_id} from: {remote_addr}")

        try:
            # Initialize proxy servers for this session
            logger.debug(f"Initializing servers for session {self._session_id}")
            await self.initialize_servers()

            # Create streams for SSH communication
            logger.debug("Creating memory streams for SSH communication")
            ssh_read_writer, ssh_read_stream = anyio.create_memory_object_stream(0)
            ssh_write_stream, ssh_write_reader = anyio.create_memory_object_stream(0)

            async def ssh_reader():
                """Read JSON-RPC messages from the SSH connection"""
                try:
                    logger.debug(f"SSH reader started for session {self._session_id}")
                    while True:
                        logger.debug("Waiting for next line from SSH...")
                        line = await self.readline()
                        if not line:
                            logger.debug("Empty line received, skipping")
                            continue

                        try:
                            logger.debug(f"Parsing JSON-RPC message: {line[:100]}")
                            message = types.JSONRPCMessage.model_validate_json(line)
                            logger.debug(f"Session {self._session_id} received: {message}")
                            await ssh_read_writer.send(message)
                        except Exception as exc:
                            logger.error(f"Error parsing message: {exc}")
                            logger.debug(f"Invalid message: {line[:200]}")
                            await ssh_read_writer.send(exc)
                except asyncio.CancelledError:
                    logger.debug(f"SSH reader for session {self._session_id} cancelled")
                except Exception as e:
                    logger.error(f"Error in SSH reader: {e}")
                    logger.debug(f"SSH reader exception details:", exc_info=True)
                    await ssh_read_writer.send(Exception(f"SSH transport error: {e}"))

            async def ssh_writer():
                """Write JSON-RPC messages to the SSH connection"""
                try:
                    logger.debug(f"SSH writer started for session {self._session_id}")
                    async for message in ssh_write_reader:
                        # Check if the channel is still open before writing
                        if self._chan is None or self._chan.is_closing():
                            logger.warning(f"Session {self._session_id}: Cannot send message: SSH channel is closed")
                            continue

                        json_str = message.model_dump_json(by_alias=True, exclude_none=True)
                        logger.debug(f"Session {self._session_id} sending: {json_str[:200]}")
                        self._chan.write(json_str + '\n')
                except asyncio.CancelledError:
                    logger.debug(f"SSH writer for session {self._session_id} cancelled")
                except Exception as e:
                    logger.error(f"Error in SSH writer: {e}")
                    logger.debug(f"SSH writer exception details:", exc_info=True)

            # Run the server with stdio transport
            logger.debug("Setting up task group for processing")
            async with anyio.create_task_group() as tg:
                # Start SSH reader and writer tasks
                logger.debug("Starting SSH reader and writer tasks")
                tg.start_soon(ssh_reader)
                tg.start_soon(ssh_writer)

                # Create stdio server for handling MCP protocol
                logger.debug("Creating stdio server")
                # Run the merged server
                if self._merged_server:
                    logger.debug(f"Running merged server for session {self._session_id}")
                    init_options = self._merged_server.create_initialization_options()
                    logger.debug(f"Server initialization options: {init_options}")
                    await self._merged_server.run(ssh_read_stream, ssh_write_stream, init_options)
                else:
                    logger.error("No merged server available to run")

        except Exception as e:
            logger.error(f"Session {self._session_id} processing error: {e}")
            logger.debug(f"Session processing exception details:", exc_info=True)
        finally:
            # Clean up resources if needed
            logger.debug(f"Cleaning up session {self._session_id}")
            for _server in self._servers:
                try:
                    logger.debug(f"Cleaning up server {_server.name}")
                    await _server.cleanup()
                except Exception as e:
                    logger.error(f"Error closing client session: {e}")
                    logger.debug(f"Cleanup exception details:", exc_info=True)
            logger.info(f"Session {self._session_id} processing finished")

"""Create an MCP server that proxies requests through multiple MCP clients.

This server is created independent of any transport mechanism.

This code is inspired by: https://github.com/sparfenyuk/mcp-proxy
Original author: sparfenyuk
"""

import asyncio
from contextlib import AsyncExitStack
import logging
import shutil
from typing import Any
import typing as t
import traceback
import re

from mcp import server, types, ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

logger = logging.getLogger(__name__)

class Server:
    """Manages MCP server connections and tool execution."""

    def __init__(self, name: str, config: dict[str, Any]) -> None:
        self.name: str = name
        self.config: dict[str, Any] = config
        self.stdio_context: Any | None = None
        self.session: ClientSession | None = None
        self._cleanup_lock: asyncio.Lock = asyncio.Lock()
        self.exit_stack: AsyncExitStack = AsyncExitStack()
        logger.debug(f"Server '{name}' initialized with config: {config}")

    async def initialize(self) -> None:
        """Initialize the server connection."""
        logger.debug(f"Initializing server '{self.name}'")
        command = (
            shutil.which("npx")
            if self.config["command"] == "npx"
            else self.config["command"]
        )
        if command is None:
            error_msg = "The command must be a valid string and cannot be None."
            logger.error(f"Server '{self.name}': {error_msg}")
            raise ValueError(error_msg)

        logger.debug(f"Server '{self.name}' using command: {command}")

        server_params = StdioServerParameters(
            command=command,
            args=self.config["args"],
            # TODO - handle environment variables properly per session
            # env={**os.environ, **self.config["env"]}
            # if self.config.get("env")
            # else None,
        )
        logger.debug(f"Server '{self.name}' params: {server_params}")

        try:
            logger.debug(f"Server '{self.name}' creating stdio client")
            stdio_transport = await self.exit_stack.enter_async_context(
                stdio_client(server_params)
            )
            read, write = stdio_transport
            logger.debug(f"Server '{self.name}' creating client session")
            session = await self.exit_stack.enter_async_context(
                ClientSession(read, write)
            )
            logger.debug(f"Server '{self.name}' initializing session")
            await session.initialize()
            self.session = session
            logger.debug(f"Server '{self.name}' initialized successfully")
        except Exception as e:
            logging.error(f"Error initializing server '{self.name}': {e}")
            logger.debug(f"Server '{self.name}' initialization error details:", exc_info=True)
            await self.cleanup()
            raise

    async def cleanup(self) -> None:
        """Clean up server resources."""
        async with self._cleanup_lock:
            logger.debug(f"Cleaning up server '{self.name}'")
            try:
                await self.exit_stack.aclose()
                self.session = None
                self.stdio_context = None
                logger.debug(f"Server '{self.name}' cleanup completed")
            except Exception as e:
                logging.error(f"Error during cleanup of server '{self.name}': {e}")
                logger.debug(f"Server '{self.name}' cleanup error details:", exc_info=True)

async def create_merged_proxy_server(servers: list[Server]) \
    -> server.Server[object]:
    """Create a merged server instance from multiple servers.

    Args:
        servers: List of servers to merge

    Returns:
        A server that combines and routes requests to the appropriate session
    """
    logger.debug(f"Creating merged proxy server with {len(servers)} servers")

    # Initialize all sessions and collect their metadata
    sessions_info = []
    for local_server in servers:
        logger.debug(f"Getting metadata for server '{local_server.name}'")
        if local_server.session is None:
            error_msg = "Server session is not initialized."
            logger.error(f"Server '{local_server.name}': {error_msg}")
            raise ValueError(error_msg)

        try:
            response = await local_server.session.initialize()
            logger.debug(f"Server '{local_server.name}' responded with: {response.serverInfo.name}")
            logger.debug(f"Server '{local_server.name}' capabilities: {response.capabilities}")
            sessions_info.append({
                "session": local_server.session,
                "server_name": response.serverInfo.name,
                "capabilities": response.capabilities
            })
        except Exception as e:
            logger.error(f"Error initializing server '{local_server.name}': {e}")
            logger.debug(f"Initialization error details:", exc_info=True)
            raise

    # If no sessions, return empty server
    if not sessions_info:
        logger.warning("No valid sessions found, creating empty server")
        return server.Server(name="Empty Merged Server")

    # Create a merged server with a composite name
    server_names = [info["server_name"] for info in sessions_info]
    if len(server_names) == 1:
        app = server.Server(name=server_names[0])
        logger.debug(f"Created single-server proxy: {server_names[0]}")
    else:
        app = server.Server(name=f"Merged: {', '.join(server_names)}")
        logger.debug(f"Created merged server: {app.name}")

    # Mappings for routing and name resolution
    # Maps display names to (server_session, original_name) tuples
    name_to_server_map = {
        "prompt": {},  # Maps prompt names to (session, original_name)
        "resource": {},  # Maps resource URIs to (session, original_uri)
        "tool": {}     # Maps tool names to (session, original_name)
    }

    # Additional map for regex-based resource URIs
    regex_resource_map = []  # List of (pattern, session, original_uri) tuples

    # Helper function to ensure unique names by adding suffixes
    def ensure_unique_name(name_map: dict, original_name: str,
                           server_session: ClientSession,
                           server_name: str, original_item_name: str) -> str:
        """
        Ensures names are unique by adding suffixes if needed.

        Args:
            name_map: Dict mapping display names to (session, original_name)
            original_name: The name without any modifications
            server_session: The session object for the server
            server_name: The name of the server
            original_item_name: The original name of the item in its server

        Returns:
            A unique name, possibly with a suffix added
        """
        if original_name not in name_map:
            name_map[original_name] = (server_session, original_item_name)
            return original_name

        # Name collision found, add suffix
        suffix = 1
        while f"{original_name}_{suffix}" in name_map:
            suffix += 1

        unique_name = f"{original_name}_{suffix}"
        logger.debug(f"Renamed duplicate '{original_name}' from '{server_name}' to '{unique_name}'")
        name_map[unique_name] = (server_session, original_item_name)
        return unique_name

    # Helper function to find the session and original name for a given display name
    def find_session_for_name(name_map: dict, display_name: str) -> tuple[ClientSession, str]:
        """
        Finds the session and original name for a given display name.

        Args:
            name_map: The mapping dictionary to search in
            display_name: The name displayed to the client

        Returns:
            A tuple of (session, original_name)
        """
        if display_name not in name_map:
            error_msg = f"Unknown name: {display_name}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        return name_map[display_name]

    # Enhanced version of find_session_for_name that also checks regex patterns for resources
    def find_resource_session(uri: str) -> tuple[ClientSession, str]:
        """
        Finds the session and original URI for a resource, checking both exact matches
        and regex pattern matches.

        Args:
            uri: The URI to find

        Returns:
            A tuple of (session, original_uri)

        Raises:
            ValueError: If the URI doesn't match any known resource
        """
        # First try exact match
        if uri in name_to_server_map["resource"]:
            return name_to_server_map["resource"][uri]

        # Then try regex patterns
        for pattern, session, original_uri in regex_resource_map:
            match = pattern.match(uri)
            if match:
                # Extract parameters from the match
                params = match.groupdict()
                # Replace parameters in the original URI
                processed_uri = original_uri
                for param_name, param_value in params.items():
                    processed_uri = processed_uri.replace(f"{{{param_name}}}", param_value)
                logger.debug(f"Matched regex pattern for {uri}, original={original_uri}, processed={processed_uri}")
                return session, processed_uri

        # If we get here, no match was found
        error_msg = f"Unknown resource URI: {uri}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # Check which capabilities are available in at least one server
    any_has_prompts = any(info["capabilities"].prompts for info in sessions_info)
    any_has_resources = any(info["capabilities"].resources for info in sessions_info)
    any_has_logging = any(info["capabilities"].logging for info in sessions_info)
    any_has_tools = any(info["capabilities"].tools for info in sessions_info)

    logger.debug(f"Merged capabilities: prompts={any_has_prompts}, resources={any_has_resources}, logging={any_has_logging}, tools={any_has_tools}")

    # Implement handlers based on available capabilities
    if any_has_prompts:
        logger.debug("Setting up prompt handlers")
        async def _list_prompts(_: t.Any) -> types.ServerResult:
            logger.debug("Handling list_prompts request")
            results = []
            name_to_server_map["prompt"].clear()  # Reset prompt mappings

            for info in sessions_info:
                if info["capabilities"].prompts:
                    logger.debug(f"Requesting prompts from {info['server_name']}")
                    try:
                        prompts = await info["session"].list_prompts()
                        # Process each prompt to ensure unique names
                        for prompt in prompts.prompts:
                            original_name = prompt.name
                            prompt.name = ensure_unique_name(
                                name_to_server_map["prompt"],
                                original_name,
                                info["session"],
                                info["server_name"],
                                original_name
                            )
                            logger.debug(f"Added prompt: {original_name} as {prompt.name}")
                        results.extend(prompts.prompts)
                        logger.debug(f"Added {len(prompts.prompts)} prompts from {info['server_name']}")
                    except Exception as e:
                        logger.error(f"Error listing prompts from {info['server_name']}: {e}")
                        logger.debug("Error details:", exc_info=True)

            logger.debug(f"Returning {len(results)} total prompts")
            return types.ServerResult(types.ListPromptsResult(prompts=results))

        app.request_handlers[types.ListPromptsRequest] = _list_prompts

        async def _get_prompt(req: types.GetPromptRequest) -> types.ServerResult:
            logger.debug(f"Handling get_prompt request for: {req.params.name}")
            try:
                # Look up the server and original prompt name
                session, original_name = find_session_for_name(
                    name_to_server_map["prompt"],
                    req.params.name
                )
                logger.debug(f"Forwarding request to server with prompt name: {original_name}")
                result = await session.get_prompt(original_name, req.params.arguments)
                logger.debug(f"Got prompt result: {result}")
                return types.ServerResult(result)
            except ValueError as e:
                logger.error(f"Error getting prompt: {e}")
                logger.debug("Returning empty result due to error")
                return types.ServerResult(types.EmptyResult())
            except Exception as e:
                logger.error(f"Unexpected error in get_prompt: {e}")
                logger.debug("Error details:", exc_info=True)
                return types.ServerResult(types.EmptyResult())

        app.request_handlers[types.GetPromptRequest] = _get_prompt

    if any_has_resources:
        logger.debug("Setting up resource handlers")
        async def _list_resources(_: t.Any) -> types.ServerResult:
            logger.debug("Handling list_resources request")
            results = []
            name_to_server_map["resource"].clear()  # Reset resource mappings
            regex_resource_map.clear()  # Reset regex resource mappings

            for info in sessions_info:
                if info["capabilities"].resources:
                    try:
                        logger.debug(f"Requesting resources from {info['server_name']}")
                        resources = await info["session"].list_resources()
                        # Process each resource to ensure unique URIs
                        for resource in resources.resources:
                            original_uri = resource.uri
                            resource.uri = ensure_unique_name(
                                name_to_server_map["resource"],
                                str(original_uri),
                                info["session"],
                                info["server_name"],
                                str(original_uri)
                            )
                            logger.debug(f"Added resource: {original_uri} as {resource.uri}")

                            # If URI contains regex pattern indicators like {id}, add to regex map
                            if '{' in str(resource.uri) and '}' in str(resource.uri):
                                pattern_string = str(resource.uri).replace('{', '(?P<').replace('}', '>[^/]+)')
                                pattern = re.compile(f"^{pattern_string}$")
                                regex_resource_map.append((pattern, info["session"], str(original_uri)))
                                logger.debug(f"Added regex resource pattern: {pattern_string}")

                        results.extend(resources.resources)
                        logger.debug(f"Added {len(resources.resources)} resources from {info['server_name']}")
                    except Exception as e:
                        logger.error(f"Error listing resources from {info['server_name']}: {e}")
                        logger.debug("Error details:", exc_info=True)

            logger.debug(f"Returning {len(results)} total resources")
            return types.ServerResult(types.ListResourcesResult(resources=results))

        app.request_handlers[types.ListResourcesRequest] = _list_resources

        async def _read_resource(req: types.ReadResourceRequest) -> types.ServerResult:
            logger.debug(f"Handling read_resource request for: {req.params.uri}")
            try:
                # Look up the server and original resource URI
                session, original_uri = find_resource_session(str(req.params.uri))
                logger.debug(f"Forwarding request to server with resource URI: {original_uri}")
                result = await session.read_resource(types.AnyUrl(original_uri))
                return types.ServerResult(result)
            except ValueError as e:
                logger.error(f"Error reading resource: {e}")
                logger.debug("Returning empty result due to error")
                return types.ServerResult(types.EmptyResult())
            except Exception as e:
                logger.error(f"Unexpected error in read_resource: {e}")
                logger.debug("Error details:", exc_info=True)
                return types.ServerResult(types.EmptyResult())

        app.request_handlers[types.ReadResourceRequest] = _read_resource

        async def _subscribe_resource(req: types.SubscribeRequest) -> types.ServerResult:
            logger.debug(f"Handling subscribe_resource request for: {req.params.uri}")
            try:
                session, original_uri = find_resource_session(str(req.params.uri))
                logger.debug(f"Forwarding subscription to server with resource URI: {original_uri}")
                await session.subscribe_resource(types.AnyUrl(original_uri))
                logger.debug("Subscription successful")
                return types.ServerResult(types.EmptyResult())
            except ValueError as e:
                logger.error(f"Error subscribing to resource: {e}")
                return types.ServerResult(types.EmptyResult())
            except Exception as e:
                logger.error(f"Unexpected error in subscribe_resource: {e}")
                logger.debug("Error details:", exc_info=True)
                return types.ServerResult(types.EmptyResult())

        app.request_handlers[types.SubscribeRequest] = _subscribe_resource

        async def _unsubscribe_resource(req: types.UnsubscribeRequest) -> types.ServerResult:
            logger.debug(f"Handling unsubscribe_resource request for: {req.params.uri}")
            try:
                session, original_uri = find_resource_session(str(req.params.uri))
                logger.debug(f"Forwarding unsubscription to server with resource URI: {original_uri}")
                await session.unsubscribe_resource(types.AnyUrl(original_uri))
                logger.debug("Unsubscription successful")
                return types.ServerResult(types.EmptyResult())
            except ValueError as e:
                logger.error(f"Error unsubscribing from resource: {e}")
                return types.ServerResult(types.EmptyResult())
            except Exception as e:
                logger.error(f"Unexpected error in unsubscribe_resource: {e}")
                logger.debug("Error details:", exc_info=True)
                return types.ServerResult(types.EmptyResult())

        app.request_handlers[types.UnsubscribeRequest] = _unsubscribe_resource

    if any_has_logging:
        logger.debug("Setting up logging handlers")
        async def _set_logging_level(req: types.SetLevelRequest) -> types.ServerResult:
            logger.debug(f"Handling set_logging_level request: {req.params.level}")
            # Set logging level on all servers that support it
            for info in sessions_info:
                if info["capabilities"].logging:
                    logger.debug(f"Setting logging level on {info['server_name']} to {req.params.level}")
                    try:
                        await info["session"].set_logging_level(req.params.level)
                    except Exception as e:
                        logger.error(f"Error setting logging level on {info['server_name']}: {e}")
                        logger.debug("Error details:", exc_info=True)
            return types.ServerResult(types.EmptyResult())

        app.request_handlers[types.SetLevelRequest] = _set_logging_level

    if any_has_tools:
        logger.debug("Setting up tool handlers")
        async def _list_tools(_: t.Any) -> types.ServerResult:
            logger.debug("Handling list_tools request")
            all_tools = []
            name_to_server_map["tool"].clear()  # Reset tool mappings

            for info in sessions_info:
                if info["capabilities"].tools:
                    try:
                        logger.debug(f"Requesting tools from {info['server_name']}")
                        tools = await info["session"].list_tools()
                        # Process each tool to ensure unique names
                        for tool in tools.tools:
                            original_name = tool.name
                            tool.name = ensure_unique_name(
                                name_to_server_map["tool"],
                                original_name,
                                info["session"],
                                info["server_name"],
                                original_name
                            )
                            logger.debug(f"Added tool: {original_name} as {tool.name}")
                        all_tools.extend(tools.tools)
                        logger.debug(f"Added {len(tools.tools)} tools from {info['server_name']}")
                    except Exception as e:
                        logger.error(f"Error listing tools from {info['server_name']}: {e}")
                        logger.debug("Error details:", exc_info=True)

            logger.debug(f"Returning {len(all_tools)} total tools")
            return types.ServerResult(types.ListToolsResult(tools=all_tools))

        app.request_handlers[types.ListToolsRequest] = _list_tools

        async def _call_tool(req: types.CallToolRequest) -> types.ServerResult:
            logger.debug(f"Handling call_tool request for: {req.params.name}")
            try:
                session, original_name = find_session_for_name(
                    name_to_server_map["tool"],
                    req.params.name
                )
                logger.debug(f"Forwarding request to server with tool name: {original_name}")
                logger.debug(f"Tool arguments: {req.params.arguments}")
                result = await session.call_tool(
                    original_name,
                    (req.params.arguments or {}),
                )
                logger.debug(f"Tool execution result: isError={result.isError}, content length={len(result.content) if result.content else 0}")
                return types.ServerResult(result)
            except ValueError as e:
                logger.error(f"Error calling tool: {e}")
                return types.ServerResult(
                    types.CallToolResult(
                        content=[types.TextContent(type="text", text=str(e))],
                        isError=True,
                    ),
                )
            except Exception as e:
                logger.error(f"Unexpected error in call_tool: {e}")
                logger.debug("Error details:", exc_info=True)
                tb_str = traceback.format_exc()
                return types.ServerResult(
                    types.CallToolResult(
                        content=[types.TextContent(type="text", text=f"Error: {str(e)}\n\n{tb_str}")],
                        isError=True,
                    ),
                )

        app.request_handlers[types.CallToolRequest] = _call_tool

    # Progress notification - handle without server name prefix
    async def _send_progress_notification(req: types.ProgressNotification) -> None:
        logger.debug(f"Handling progress notification: token={req.params.progressToken}, progress={req.params.progress}/{req.params.total}")
        # We don't have a way to know which server this is for, so send to all
        # TODO - find a way to do this without sending this to all servers
        for info in sessions_info:
            try:
                logger.debug(f"Forwarding progress notification to {info['server_name']}")
                await info["session"].send_progress_notification(
                    req.params.progressToken,
                    req.params.progress,
                    req.params.total,
                )
            except Exception as e:
                logger.error(f"Error sending progress notification to {info['server_name']}: {e}")
                logger.debug("Error details:", exc_info=True)

    app.notification_handlers[types.ProgressNotification] = _send_progress_notification

    async def _complete(req: types.CompleteRequest) -> types.ServerResult:
        logger.debug(f"Handling complete request for ref: {req.params.ref}")
        # Try all servers until one succeeds
        # TODO - find a way to do this without trying all servers
        for info in sessions_info:
            try:
                logger.debug(f"Trying completion with {info['server_name']}")
                result = await info["session"].complete(
                    req.params.ref,
                    req.params.argument.model_dump(),
                )
                logger.debug(f"Got completion result from {info['server_name']}")
                return types.ServerResult(result)
            except Exception as e:
                logger.debug(f"Completion with {info['server_name']} failed: {e}")
                continue

        # If all fail, return an error
        logger.warning("All completion attempts failed")
        return types.ServerResult(types.EmptyResult())

    app.request_handlers[types.CompleteRequest] = _complete

    logger.debug(f"Merged proxy server created with {len(app.request_handlers)} request handlers")
    return app

import asyncio
import logging
from pathlib import Path
from typing import Dict, List, Optional

import asyncssh

from .ssh_session import SSHSessionHandler

logger = logging.getLogger(__name__)

async def run_ssh_server(host: str = '0.0.0.0',
                         port: int = 8022,
                         server_host_keys: Optional[List[str]] = None,
                         authorized_client_keys: Optional[str] = None,
                         passphrase: Optional[str] = None,
                         servers_config: str = 'servers_config.json'):
    """
    Run an SSH server that accepts connections and creates MCP server sessions.

    Args:
        host: The host address to bind to
        port: The port to listen on
        server_host_keys: List of paths to SSH host key files
        authorized_client_keys: Path to authorized keys file
        passphrase: Passphrase for the private key
        servers_config: Path to server configurations JSON
    """
    logger.debug(f"Starting SSH server on {host}:{port}")
    logger.debug(f"Server config file: {servers_config}")

    if not server_host_keys:
        # Generate a default key if none provided
        default_private_key_path = Path.home() / ".ssh" / "mcp_server_key"
        logger.debug(f"No server host keys provided, checking for default key at: {default_private_key_path}")

        if not default_private_key_path.exists():
            try:
                logger.debug("Generating Ed25519 key pair...")
                ssh_key = asyncssh.generate_private_key(alg_name='ssh-ed25519')

                # Save the private key
                logger.debug(f"Saving private key to {default_private_key_path}")
                with open(default_private_key_path, 'wb') as f:
                    f.write(ssh_key.export_private_key())

                # Save the public key (optional)
                default_public_key_path = default_private_key_path.with_suffix('.pub')
                logger.debug(f"Saving public key to {default_public_key_path}")
                with open(default_public_key_path, 'wb') as f:
                    f.write(ssh_key.export_public_key())

                logger.info(f"Ed25519 key pair generated and saved to: {default_private_key_path}")

            except Exception as e:
                logger.error(f"Error generating Ed25519 key: {e}")
                logger.debug(f"Key generation error details:", exc_info=True)
        else:
            logger.debug(f"Using existing key: {default_private_key_path}")

        server_host_keys = [str(default_private_key_path)]
        logger.debug(f"Using server host keys: {server_host_keys}")

    if not authorized_client_keys:
        # Default to standard authorized_keys location
        authorized_client_keys = str(Path.home() / ".ssh" / "authorized_keys")
        logger.debug(f"Using default authorized client keys: {authorized_client_keys}")

    # Track active sessions by UUID
    active_sessions: Dict[str, SSHSessionHandler] = {}
    logger.debug("Setting up SSH server handler")

    class SSHServerHandler(asyncssh.SSHServer):
        def connection_made(self, conn: asyncssh.SSHServerConnection):
            """Called when a connection is established"""
            peer_addr = conn.get_extra_info('peername')[0]
            logger.info(f"SSH connection established from {peer_addr}")
            logger.debug(f"Connection details: {conn.get_extra_info('peer_addr')}:{conn.get_extra_info('peer_port')}")
            logger.debug(f"Client version: {conn.get_extra_info('client_version', 'unknown')}")

        def connection_lost(self, exc: Optional[BaseException]):
            """Called when a connection is closed"""
            if exc:
                logger.error(f"SSH connection error: {str(exc)}")
                logger.debug(f"Connection lost exception details: {type(exc).__name__}: {str(exc)}")
            logger.info("SSH connection closed")

        def session_requested(self):
            """Handle a new session request"""
            logger.debug(f"New session requested, using config: {servers_config}")
            session = SSHSessionHandler(servers_config)
            session_id = session._session_id
            active_sessions[session_id] = session
            logger.info(f"New SSH session created: {session_id} ({len(active_sessions)} active sessions)")
            logger.debug(f"Active sessions: {', '.join(active_sessions.keys())}")
            return session

    try:
        logger.debug("Creating SSH server with the following options:")
        logger.debug(f"  Host: {host}")
        logger.debug(f"  Port: {port}")
        logger.debug(f"  Server host keys: {server_host_keys}")
        logger.debug(f"  Authorized client keys: {authorized_client_keys}")
        logger.debug(f"  Passphrase provided: {'Yes' if passphrase else 'No'}")

        server = await asyncssh.create_server(
            SSHServerHandler,
            host,
            port,
            server_host_keys=server_host_keys,
            authorized_client_keys=authorized_client_keys,
            passphrase=passphrase
        )

        logger.info(f"SSH server listening on {host}:{port}")

        # Keep the server running indefinitely
        while True:
            logger.debug(f"SSH server alive with {len(active_sessions)} active sessions")
            await asyncio.sleep(3600)  # Sleep for an hour and continue

    except (OSError, asyncssh.Error) as exc:
        logger.error(f"Error in SSH server: {exc}")
        logger.debug(f"SSH server error details:", exc_info=True)

    except asyncio.CancelledError:
        logger.info("SSH server shutting down")

    finally:
        # Clean up any remaining sessions
        logger.debug(f"Cleaning up {len(active_sessions)} active sessions")
        for session_id, session in list(active_sessions.items()):
            logger.debug(f"Cleaning up session {session_id}")
            if session._chan:
                try:
                    session._chan.close()
                except Exception as e:
                    logger.debug(f"Error closing session channel: {e}")
            del active_sessions[session_id]
        logger.debug("All sessions cleaned up")

# MCP Server SSH Server

A secure SSH server for accessing and interacting with MCP (Model Context Protocol) tools remotely. This server allows clients to connect to multiple MCP tool providers through a unified secure interface.

## Features

- **Secure Remote Access**: Access MCP tools remotely over SSH
- **Server Aggregation**: Proxy and merge multiple MCP servers into a unified interface
- **Full MCP Support**: Compatible with all MCP capabilities including:
  - Prompts
  - Resources
  - Tools
  - Logging
- **Dynamic Configuration**: Configure available MCP servers through a simple JSON configuration
- **Automatic Key Management**: Generates SSH keys automatically if not provided

## Installation

### Using `pip`

```bash
pip install mcp-server-ssh-server
```

## Usage

### Basic Usage

Start the SSH server with default settings:

```bash
mcp-server-ssh-server
```

This will:
1. Listen on 0.0.0.0:8022 by default
2. Use or generate SSH keys in `~/.ssh/mcp_server_key`
3. Use `~/.ssh/authorized_keys` for client authorization
4. Load server configurations from `servers_config.json`

### Command Line Options

```bash
mcp-server-ssh-server --host 0.0.0.0 --port 8022 --servers-config my_config.json
```

Available options:
- `--host`: SSH server host address to bind to (default: 0.0.0.0)
- `--port`: SSH server port to listen on (default: 8022)
- `--authorized-clients`: Path to authorized keys file (default: ~/.ssh/authorized_keys)
- `--server-key`: Path to server private key file (default: ~/.ssh/mcp_server_key)
- `--passphrase`: Passphrase for the private key (optional)
- `--servers-config`: Path to server configurations JSON (default: servers_config.json)
- `--log-level`: Set logging level (default: INFO)

## Configuration

Configure available MCP servers in a JSON file (default: `servers_config.json`):

```json
{
  "mcpServers": {
    "puppeteer": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-puppeteer"]
    },
    "everything": {
      "command": "npx",
      "args": ["-y", "@modelcontextprotocol/server-everything"]
    }
    // Add more servers here
  }
}
```

Each server configuration needs:
- `command`: The executable to run
- `args`: Arguments to pass to the command

## Security Considerations

- Keys are automatically generated if not provided
- Default key location is `~/.ssh/mcp_server_key`
- Client authorization uses standard SSH key-based authentication
- Ensure proper permissions on key files (600 for private keys, 644 for public keys)

## Contributing

We encourage contributions to help expand and improve `mcp-server-ssh-server`. Whether you want to add new features, enhance existing functionality, or improve documentation, your input is valuable.

Pull requests are welcome! Feel free to contribute new ideas, bug fixes, or enhancements.

## License

MIT License - See LICENSE file for details.

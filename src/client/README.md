# MCP Server SSH Client

A Model Context Protocol Server (Remote SSH client) that connects to remote MCP servers over SSH. This client enables accessing and utilizing MCP tools hosted on remote servers, providing a secure channel for MCP communication.

## Features

- Connect to any MCP server over SSH
- Proxy all MCP protocol commands through a secure SSH channel
- Support for key-based authentication
- Full support for MCP capabilities including tools, prompts, and resources

## Installation

### Using `pip`

```
pip install mcp-server-ssh-client
```

After installation, you can run it directly:

```
mcp-server-ssh-client --host example.com --port 8022 --username mcp --client-key ~/.ssh/mcp_client_key --known-hosts ~/.ssh/mcp_remote_server
```

## Usage

### Command Line Options

- `--host`: Remote SSH server host (default: "localhost")
- `--port`: Remote SSH server port (default: 8022)
- `--username`: SSH username (default: "mcp")
- `--client-key`: Client private key file (default: "~/.ssh/id_rsa")
- `--known-hosts`: Path to known hosts file (default: "~/.ssh/known_hosts")
- `--passphrase`: Passphrase for the private key (default: None)
- `--disable_host_key_checking`: Skip server signature verification (default: False)
- `--log-level`: Set logging level (default: INFO)

### Configure for Claude Desktop

Add to your Claude settings:

```json
"mcpServers": {
  "remote-servers": {
    "command": "mcp-server-ssh-client",
    "args": ["--host", "example.com", "--port", "8022", "--username", "mcp"]
  }
}
```

## How It Works

The MCP Server SSH Client establishes an SSH connection to a remote server running MCP tools. It then:

1. Creates a local proxy server that mirrors the capabilities of the remote MCP server
2. Forwards all MCP requests to the remote server through the SSH connection
3. Returns responses from the remote server to the local client

This allows you to use tools running on remote machines as if they were installed locally.

## Debugging

You can use the MCP inspector to debug the client:

```
npx @modelcontextprotocol/inspector mcp-server-ssh-client --host example.com
```

## Contributing

We encourage contributions to help expand and improve `mcp-server-ssh-client`. Whether you want to add new features, enhance existing functionality, or improve documentation, your input is valuable.

Pull requests are welcome! Feel free to contribute new ideas, bug fixes, or enhancements.

## License

MIT License - See LICENSE file for details.

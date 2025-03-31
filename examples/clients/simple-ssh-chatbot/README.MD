# MCP Simple SSH Chatbot

This example demonstrates how to integrate the Model Context Protocol (MCP) into a simple CLI chatbot over SSH. The implementation showcases MCP's flexibility by supporting tools through SSH-based MCP servers.

## Requirements

- Python 3.10
- `python-dotenv`
- `requests`
- `mcp`
- `uvicorn`
- `asyncssh`

## Installation

1. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables:**

   Create a `.env` file in the root directory and add your API key:

   ```plaintext
   LLM_API_KEY=your_api_key_here
   ```

3. **Configure SSH server connection:**

   The `servers_config.json` allows you to configure SSH connection details:
   
   ```json
   {
     "mcpServers": {
       "simple-ssh-tool": {
         "host": "localhost",
         "port": 8022,
         "username": "mcp",
         "password": null,
         "client_keys": ["~/.ssh/id_rsa"],
         "known_hosts": null
       }
     }
   }
   ```

4. **Setup SSH keys (if needed):**

   If using key-based authentication, ensure your SSH keys are properly set up:

   ```bash
   # Generate new key if needed
   ssh-keygen -t ed25519 -f ~/.ssh/mcp_key
   
   # Add to target server's authorized_keys
   ssh-copy-id -i ~/.ssh/mcp_key user@hostname
   ```

## Usage

1. **Start the SSH MCP server:**

   Ensure your MCP SSH server is running and accessible.

2. **Run the client:**

   ```bash
   python main.py
   ```

3. **Interact with the assistant:**
   
   The assistant will automatically detect available tools and can respond to queries based on the tools provided by the SSH server.

4. **Exit the session:**

   Type `quit` or `exit` to end the session.

## Architecture

- **SSH Connection**: Uses SSH for secure communication with MCP servers
- **Tool Discovery**: Tools are automatically discovered from connected SSH servers
- **System Prompt**: Tools are dynamically included in the system prompt
- **Server Integration**: Supports any MCP-compatible SSH server

### Class Structure
- **Configuration**: Manages environment variables and server configurations
- **Server**: Handles MCP server initialization over SSH, tool discovery, and execution
- **Tool**: Represents individual tools with their properties and formatting
- **LLMClient**: Manages communication with the LLM provider
- **ChatSession**: Orchestrates the interaction between user, LLM, and tools

### Security Considerations

- SSH provides secure, encrypted communication between the client and server
- Support for various authentication methods (password, key-based)
- Can leverage existing SSH infrastructure and security practices

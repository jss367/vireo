"""A bounded, read-only stdio probe, independent of assistant registration.

Never echo child stderr or arbitrary protocol errors: they may carry runtime
credentials. Only known, credential-free diagnostics reach Settings.
"""

import json
import os

import anyio
from mcp.client.session import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from mcp.shared.exceptions import MCPError
from mcp_server import (
    BAD_TOKEN_MESSAGE,
    NON_LOOPBACK_URL_MESSAGE,
    NOT_RUNNING_MESSAGE,
    UNPAIRED_URL_MESSAGE,
    server_command,
)

STARTUP_MESSAGE = (
    "Could not start or initialize the Vireo MCP server. Check the generated "
    "command's executable exists; for a source install, install the project "
    "dependencies in that Python environment. Reload Settings after moving or "
    "updating Vireo and register the new command."
)
RESPONSE_MESSAGE = (
    "The MCP server started but its read-only catalog check failed. Open Vireo, "
    "check its log panel, and retry. If VIREO_URL or VIREO_TOKEN overrides are "
    "set, check that they target the intended running instance."
)
TIMEOUT_MESSAGE = (
    "The Vireo MCP check timed out. Make sure Vireo is responsive, try the "
    "generated command in a terminal, and retry."
)


def _safe_failure(error, fallback):
    if isinstance(error, ExceptionGroup):
        for child in error.exceptions:
            message = _safe_failure(child, "")
            if message:
                return message
    if isinstance(error, TimeoutError):
        return TIMEOUT_MESSAGE
    for message in (NOT_RUNNING_MESSAGE, BAD_TOKEN_MESSAGE, UNPAIRED_URL_MESSAGE, NON_LOOPBACK_URL_MESSAGE):
        if message in str(error):
            return message
    return fallback


async def _read_tool(session, name):
    result = await session.call_tool(name, {})
    text = next((block.text for block in result.content if block.type == "text"), "")
    if result.is_error:
        raise ValueError(_safe_failure(text, RESPONSE_MESSAGE))
    return json.loads(text)


async def _probe(timeout):
    command = server_command()
    # Match the credentials/discovery an assistant-launched process uses;
    # StdioServerParameters supplies the SDK's normal safe environment too.
    overrides = {key: os.environ[key] for key in ("VIREO_URL", "VIREO_TOKEN") if key in os.environ}
    params = StdioServerParameters(command=command[0], args=command[1:], env=overrides)
    stage = "startup"
    try:
        with anyio.fail_after(timeout), open(os.devnull, "w") as errlog:
            async with (
                stdio_client(params, errlog=errlog) as (read, write),
                ClientSession(read, write) as session,
            ):
                await session.initialize()
                tools = await session.list_tools()
                if not {"vireo_status", "list_folders"} <= {tool.name for tool in tools.tools}:
                    return {"ok": False, "message": STARTUP_MESSAGE}
                stage = "catalog"
                status = await _read_tool(session, "vireo_status")
                folders = await _read_tool(session, "list_folders")
                return {"ok": True, "status": status, "folders": folders["folders"]}
    except (OSError, ValueError, KeyError, TypeError, RuntimeError, MCPError,
            anyio.EndOfStream, anyio.BrokenResourceError, ExceptionGroup) as error:
        fallback = STARTUP_MESSAGE if stage == "startup" else RESPONSE_MESSAGE
        return {"ok": False, "message": _safe_failure(error, fallback)}


def check_connection(timeout=15):
    """Start the generated command, initialize MCP, and read catalog tools.

    This checks the default MCP target, which can differ from the Settings
    window when runtime discovery or explicit overrides name another instance.
    It neither reads nor changes an assistant's configuration.
    """
    return anyio.run(_probe, timeout)

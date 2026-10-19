"""Bundle the MCP SDK without its optional command-line dependencies."""

from PyInstaller.utils.hooks import collect_submodules, is_module_or_submodule

# mcp.cli exits at import time when its optional typer dependency is absent.
# Filter before collection recurses into it; an Analysis exclusion alone is
# too late to prevent collect_submodules() from importing the CLI.
hiddenimports = collect_submodules(
    "mcp", filter=lambda name: not is_module_or_submodule(name, "mcp.cli")
)
excludedimports = ["mcp.cli"]

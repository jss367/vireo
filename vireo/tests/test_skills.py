"""The skills in ``skills/`` call MCP tools that exist, with parameters they take.

A skill is prose an assistant follows, so a renamed tool or parameter would
otherwise go unnoticed until someone's session fails.
"""

import ast
import re
from pathlib import Path

import anyio
import pytest
from mcp_server import VireoClient, build_server

SKILLS = Path(__file__).resolve().parents[2] / "skills"
SKILL_FILES = sorted(SKILLS.glob("*/SKILL.md"))


def _tool_calls(text):
    """``(name, {parameter})`` for each call written as inline code."""
    calls = []
    # A fence's own backticks would pair with the next inline span's.
    text = re.sub(r"^```.*?^```", "", text, flags=re.DOTALL | re.MULTILINE)
    for span in re.findall(r"`([^`]+)`", text):
        if re.fullmatch(r"[a-z_]+\(.*\)", span, re.DOTALL) is None:
            continue
        source = " ".join(span.split()).replace("true", "True").replace("false", "False")
        call = ast.parse(source, mode="eval").body
        assert isinstance(call, ast.Call) and isinstance(call.func, ast.Name), span
        assert not call.args, f"name the parameters in {span}"
        calls.append((call.func.id, {keyword.arg for keyword in call.keywords}))
    return calls


def test_there_are_skills_to_check():
    assert SKILL_FILES


@pytest.mark.parametrize("path", SKILL_FILES, ids=lambda path: path.parent.name)
def test_skill_frontmatter_names_the_folder(path):
    _, frontmatter, body = path.read_text(encoding="utf-8").split("---\n", 2)
    fields = dict(line.split(": ", 1) for line in frontmatter.strip().splitlines())
    assert fields["name"] == path.parent.name
    assert fields["description"].strip()
    assert body.strip()


@pytest.mark.parametrize("path", SKILL_FILES, ids=lambda path: path.parent.name)
def test_skill_calls_real_tools(path):
    server = build_server(VireoClient(base_url="http://127.0.0.1:9", token="unused"))
    parameters = {tool.name: set(tool.input_schema["properties"])
                  for tool in anyio.run(server.list_tools)}
    calls = _tool_calls(path.read_text(encoding="utf-8"))
    assert calls, "the skill names no tool call"
    for name, used in calls:
        assert name in parameters, f"{name} is not an MCP tool"
        assert used <= parameters[name], f"{name} takes no {sorted(used - parameters[name])}"


@pytest.mark.parametrize("path", SKILL_FILES, ids=lambda path: path.parent.name)
def test_skill_is_listed_in_the_readme(path):
    assert f"({path.parent.name}/SKILL.md)" in (SKILLS / "README.md").read_text(encoding="utf-8")

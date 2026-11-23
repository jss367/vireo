# Agent skills for Vireo

Each folder holds one skill: instructions an assistant such as Claude Code or
Codex follows to do a photographer's task in Vireo. The skills use the tools of
Vireo's MCP server, so connect that first (Settings, **Claude and AI
assistants**; see [the headless API guide](../docs/headless-api.md)). The tools
say what Vireo can do. A skill adds the order of the work, what to look at, and
when to leave a photo undecided.

| Skill | What it does |
| --- | --- |
| [vireo-adjudicate-bursts](vireo-adjudicate-bursts/SKILL.md) | Compares the frames of a folder's bursts at 1:1 and records picks and rejects with reasons. |
| [vireo-species-views](vireo-species-views/SKILL.md) | Chooses distinct views of a species from a recent outing, edits them for detail, and exports them with location. |

## Installing a skill

Link the folder into the directory your assistant reads skills from, so it
stays current with the checkout:

```bash
ln -s /path/to/vireo/skills/vireo-adjudicate-bursts ~/.claude/skills/   # Claude Code
ln -s /path/to/vireo/skills/vireo-adjudicate-bursts ~/.codex/skills/    # Codex
```

## Writing a skill

- Name the folder after the skill, in lowercase words joined by hyphens, and
  repeat that name in the `SKILL.md` frontmatter. The `description` is all an
  assistant sees when it decides whether to use the skill, so name the requests
  that should start it.
- Write tool calls as code: `burst(photo_id=...)`. `vireo/tests/test_skills.py`
  checks that each one names a real tool and real parameters.
- Call tools rather than HTTP routes. When a step has no tool, add the tool.
- Write for any photographer and any assistant: no personal names, folders or
  catalog ids.

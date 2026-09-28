# Codex Instructions

## Pull Requests

When creating GitHub pull requests, create them ready for review by default, not as drafts.

- If using `gh pr create`, do not pass `--draft`.
- If using a GitHub API or connector, set `draft: false`.
- Only create a draft PR when the user explicitly asks for a draft.

## Tests

Several agents run tests on this machine at once. Never pass `-n auto` to pytest locally; use `-n 4` at most, and prefer the impact-selected subset (`python scripts/select_tests.py --run -- -n 4 -q`) to the full suite. See `CLAUDE.md` for details.

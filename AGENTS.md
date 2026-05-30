# Codex Instructions

## Pull Requests

When creating GitHub pull requests, create them ready for review by default, not as drafts.

- If using `gh pr create`, do not pass `--draft`.
- If using a GitHub API or connector, set `draft: false`.
- Only create a draft PR when the user explicitly asks for a draft.

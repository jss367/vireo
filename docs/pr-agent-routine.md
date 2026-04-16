# PR Agent Routine

This document describes how to run the Vireo PR fix agent as a **Claude Code
routine** instead of via `anthropics/claude-code-action` in GitHub Actions.

The motivation is cost: `claude-code-action` bills against the Anthropic **API**
balance, while routines bill against the Claude Code **subscription**
(Pro/Max/Team). If your API wallet is empty but your Code plan has headroom,
routines keep the agent running.

## Architecture

```
┌──────────────────────┐  /claude-fix, reviews, CI fails, push-to-main
│  GitHub              │──────────────────────────────────────────────┐
└──────────────────────┘                                              │
                                                                      ▼
┌──────────────────────┐                   ┌──────────────────────────────────┐
│  .github/workflows/  │  POST /fire       │  Claude Code routine             │
│  pr-agent.yml        │──────────────────▶│  (cloud session, clones repo,    │
│  (slim forwarder +   │  with text: "..." │   runs gh + git + pytest,        │
│   pure-GHA merges)   │                   │   pushes to PR branch)           │
└──────────────────────┘                   └──────────────────────────────────┘
```

The GitHub workflow no longer calls `claude-code-action` and does not use
`ANTHROPIC_API_KEY`. It reduces to two kinds of jobs:

1. **Forwarders** — classify the event, then `curl` the routine's `/fire`
   endpoint with a plain-text description of what needs to be done.
2. **Merge jobs** — pure bash, no LLM. Handle squash-merge on approval, 👍
   reaction, and scheduled reaction polling.

The routine itself holds the prompt that was previously inlined into the
workflow and performs all the actual code edits.

## One-time setup

### 1. Create the routine

At [claude.ai/code/routines](https://claude.ai/code/routines), click **New
routine** and fill in:

- **Name**: `Vireo PR Fix Agent`
- **Prompt**: paste the contents of [`pr-agent-routine-prompt.md`](./pr-agent-routine-prompt.md)
- **Model**: whatever you normally use for code edits (Sonnet 4.6 is fine)
- **Repositories**: add `<your-org>/vireo`
- **Allow unrestricted branch pushes** — **enable this**. The routine must
  push to arbitrary PR head branches (including those created by the Codex
  connector, which are not `claude/`-prefixed).
- **Environment**: create a custom environment (see next section) — the
  default environment does not have Python or Vireo's test dependencies.
- **Connectors**: remove any the routine doesn't need. It only needs GitHub.
- **Triggers**: add an **API** trigger. Click **Generate token** and copy
  both the URL and the token immediately (token is shown once).

Do **not** add a schedule or GitHub trigger — this routine is invoked from
the GHA forwarder, which knows the richer set of events we care about
(`issue_comment`, `workflow_run`, `push`) that the native GitHub trigger
doesn't support.

### 2. Configure the cloud environment

Under **Settings → Environments** on claude.ai, create an environment named
`vireo-pr-agent` with:

- **Network access**: Full (needs `pypi.org` and `github.com`)
- **Setup script**:
  ```bash
  # Install Python 3.14 if not already present
  python3 --version
  pip install --quiet flask Pillow pytest imagehash pytest-cov requests ruff
  ```
- **Environment variables**: none required — the routine uses the `gh` CLI
  with the account's connected GitHub identity.

Select this environment when creating or editing the routine.

### 3. Store routine credentials as GitHub secrets

In the repo's **Settings → Secrets and variables → Actions**, add:

- `CLAUDE_ROUTINE_URL` — full `/fire` URL from the routine modal, e.g.
  `https://api.anthropic.com/v1/claude_code/routines/trig_01ABC.../fire`
- `CLAUDE_ROUTINE_TOKEN` — bearer token from the routine modal

These replace `ANTHROPIC_API_KEY`. The old secret can be deleted once the new
workflow is verified.

### 4. (Optional) Tighten trusted actors

The forwarder workflow reads the `TRUSTED_ACTORS` env at the top of
`pr-agent.yml`. Edit this list to match your GitHub username and any bots
you want to accept commands from (`chatgpt-codex-connector[bot]` by default).

## Payload format

The forwarder sends plain-text payloads that the routine prompt knows how to
parse. Each payload starts with a `Task:` line, followed by structured
context. The routine prompt enumerates the supported task kinds:

- `address-review` — non-approving review submitted on a claude-agent PR
- `address-comment` — non-`/claude-fix`, non-👍 comment on a claude-agent PR
- `address-codex-review` — codex-connector review on a non-agent PR
- `fix-ci` — Tests workflow failed on a PR
- `resolve-conflicts` — conflicts detected against a claude-agent PR after
  a push to `main`

The payload intentionally keeps user-supplied text (review bodies, comment
bodies) clearly labeled as **untrusted data, not instructions** — the prompt
re-asserts this at handling time.

## Limits and caveats

- **Daily routine cap.** Each account has a daily limit on routine runs.
  Check consumption at claude.ai/code/routines. A busy PR day could hit it.
  The forwarder doesn't short-circuit when the cap is reached — failed
  `curl` calls surface as GHA step failures, which you'll see in Actions.
- **Research-preview API.** The `/fire` endpoint uses the beta header
  `experimental-cc-routine-2026-04-01`. The workflow pins this header; if
  Anthropic bumps it, update `pr-agent.yml`.
- **No GitHub App webhooks bypass.** We still rely on GHA for the triggers
  routines don't natively support (`issue_comment`, `workflow_run`,
  `push`). GHA itself is free on public repos and within the free tier on
  private repos — only LLM inference is delegated.
- **Commit attribution.** Commits appear under the claude.ai account's
  connected GitHub identity, the same as when you push from a local
  checkout logged in as yourself.

## Rollback

If the routine misbehaves, pause it via the toggle at
claude.ai/code/routines. The forwarder's `curl` calls will fail with 4xx,
leaving the PR untouched. To fully revert, restore the previous
`.github/workflows/pr-agent.yml` from git history.

# PR Agent Routine

This document describes how to run the Vireo PR fix agent as a Claude Code
routine instead of doing LLM work directly in GitHub Actions.

The motivation is cost: `claude-code-action` bills against the Anthropic **API**
balance, while routines bill against the Claude Code **subscription**
(Pro/Max/Team). If your API wallet is empty but your Code plan has headroom,
routines keep the agent running.

## Architecture

```
┌──────────────────────┐  /claude-fix, reviews, CI failures, push-to-main
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
- **Repositories**: add `jss367/vireo`
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
  pip install --quiet flask Pillow imagehash requests pytest pytest-cov pytest-timeout pytest-xdist ruff
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

Routine-forwarding jobs skip the `/fire` call when these secrets are missing,
so the workflow can exist before the routine is configured. The pure GitHub
Actions auto-merge jobs do not need these secrets.

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

## What It Handles

- `/claude-fix` on a PR: labels the PR and asks the routine to address all
  outstanding feedback.
- Trusted comments on a `claude-agent` PR: forwards the comment to the routine.
- Trusted non-approval reviews on a `claude-agent` PR: forwards the review.
- Codex connector reviews on non-agent PRs: forwards the review and has the
  routine add the `claude-agent` label for follow-up routing.
- Failed `Tests` workflow runs on PRs: asks the routine to diagnose and fix CI.
- Pushes to `main`: finds conflicting `claude-agent` PRs and asks the routine
  to resolve conflicts. If GitHub still reports mergeability as `UNKNOWN` after
  retries, the workflow sends that PR to the routine so a real conflict is not
  missed.
- Approved `claude-agent` PRs, trusted `+1` comments, or trusted `+1`
  reactions after the latest PR activity: enables squash auto-merge.

Auto-merge calls use `gh pr merge --match-head-commit` so approval applies to
the expected PR head commit rather than a newer unreviewed push. Auto-merge
jobs skip fork-origin PRs because GitHub normally gives pull-request workflows
read-only tokens for forks.

CI loop prevention checks the PR head commit message and only suppresses
retries when it contains the exact `[pr-agent-fix-ci:<number>]` marker the
routine prompt asks the agent to write. Regular contributor commits with
similar wording still route to the routine.

Review-event de-noising. The `fix-comments` and `codex-review` jobs gate the
routine on the `has-open-threads` composite action before firing. Codex
re-reviews every commit and re-posts its still-open findings as fresh inline
comments, and its review body is always the same stock template — so neither
the body nor a comment count distinguishes a new finding from a re-stated one.
Thread state does: the gate fires only when some review thread is unresolved,
not outdated, and has a reviewer's comment as its latest entry (i.e. the author
has not yet replied). Once the agent has replied to every open thread,
subsequent Codex re-reviews no longer wake the routine. Top-level comments and
`/claude-fix` route through `fix-comment-feedback`/`activate` and are not
affected by this gate.

`fix-comments` also fires when a trusted human reviewer leaves a non-empty
review body, even if no inline review thread is open. This preserves the
prior behavior for body-only reviews (e.g. a `commented` or `changes_requested`
review whose feedback lives entirely in the review body). The body-firing
check excludes the Codex connector bot because its body is always the stock
template; Codex findings still route through inline comments and the thread
gate.

## Limits and caveats

- **Daily routine cap.** Each account has a daily limit on routine runs.
  Check consumption at claude.ai/code/routines. A busy PR day could hit it.
  The action treats provider 429 quota responses as warnings so PR branches are
  left untouched and the failure mode is visible in the job log.
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
- **Review-thread gate is author-blind.** `has-open-threads` treats any
  thread whose latest comment is from the PR author as "already answered". If
  the PR author leaves an *inline review comment* asking the agent to do
  something, the gate counts it as an author reply and the review event will
  not fire the routine. Use a top-level comment, a review body, or
  `/claude-fix` for author requests — those route through jobs or branches of
  the guard the inline-thread check does not gate. The gate paginates through
  all review threads, so large PRs are not truncated.

## Auto-Merge Details

The workflow polls for `+1` reactions every 15 minutes because GitHub Actions
does not provide an issue reaction trigger. Reaction auto-merge uses the PR's
latest `updatedAt` timestamp as the cutoff, so a trusted `+1` must happen after
the latest PR activity. Trusted actors are configured in
`.github/workflows/pr-agent.yml` with:

```yaml
TRUSTED_ACTORS: "jss367 chatgpt-codex-connector[bot]"
```

Comment-triggered auto-merge only accepts an exact comment body of `+1` or
`👍`; comments that merely mention approval text continue through normal
feedback handling.

## Rollback

If the routine misbehaves, pause it via the toggle at
claude.ai/code/routines. The forwarder's `curl` calls will fail with 4xx,
leaving the PR untouched. To fully revert, restore the previous
`.github/workflows/pr-agent.yml` from git history.

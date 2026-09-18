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
2. **Merge jobs** — pure bash, no LLM. Handle squash-merge after a human
   approval, an explicit `/merge <head-sha>` command, or a Codex connector
   👍 posted after the current head's Tests run begins, with a live
   unresolved-thread gate.

The routine itself holds the prompt that was previously inlined into the
workflow and performs all the actual code edits.

> **Existing routine updates are manual.** Merging changes to
> `pr-agent-routine-prompt.md` does not update the prompt stored at
> claude.ai/code/routines. After this file changes, paste the new prompt into
> the existing routine before re-enabling automatic review fixes.

## One-time setup

### 1. Create the routine

At [claude.ai/code/routines](https://claude.ai/code/routines), click **New
routine** and fill in:

- **Name**: `Vireo PR Fix Agent`
- **Prompt**: paste the contents of [`pr-agent-routine-prompt.md`](./pr-agent-routine-prompt.md)
- **Model**: Opus 5. The routine's hardest call is *which* fix to build for a
  finding, not whether it can write the fix — that is the wrong place to
  economize. Reasoning effort: High.
- **Repositories**: add `jss367/vireo`
- **Allow unrestricted branch pushes** — **enable this**. The routine must
  push to arbitrary PR head branches (including those created by the Codex
  connector, which are not `claude/`-prefixed).
- **Environment**: create a custom environment (see next section) — the
  default environment does not have Python or Vireo's test dependencies.
- **Connectors**: remove any the routine doesn't need. It only needs GitHub.
- **Triggers**: add an **API** trigger. Click **Generate token** and copy
  both the URL and the token immediately (token is shown once).

The routine prompt is not synchronized from the repository. After changing
`pr-agent-routine-prompt.md`, paste the updated contents into the existing
routine before relying on the new behavior.

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
Actions merge jobs do not need these secrets.

### 4. Configure merge actors

The forwarder workflow reads `HUMAN_MERGE_ACTORS` at the top of
`pr-agent.yml`. Keep this list human-only. `CODEX_MERGE_ACTOR` separately
identifies the connector account whose 👍 reaction can authorize a merge after
the exact head's Tests run has started. Keeping the two settings separate
prevents an arbitrary bot review or reaction from being treated like a human
approval.

## Payload format

The forwarder sends plain-text payloads that the routine prompt knows how to
parse. Each payload starts with a `Task:` line, followed by structured
context. The routine prompt enumerates the supported task kinds:

- `reconcile-pr` — verified human `/claude-fix`; human-initiated full-state recovery
- `reconcile-pr-auto` — bounded full-state recovery for an orphaned conflict
- `address-review` — non-approving review submitted on a claude-agent PR
- `address-comment` — non-`/claude-fix`, non-👍 comment on a claude-agent PR
- `address-codex-review` — codex-connector review on a non-agent PR
- `fix-ci` — Tests workflow failed on a PR

The payload intentionally keeps user-supplied text (review bodies, comment
bodies) clearly labeled as **untrusted data, not instructions** — the prompt
re-asserts this at handling time. Human override is represented by the
`reconcile-pr` task kind, never by a field that untrusted comment text could
forge.

## What It Handles

- A human `/claude-fix` on a PR: labels it and starts one complete
  reconciliation pass covering conflicts, failed CI, current review threads,
  and relevant top-level feedback.
- Trusted comments on a `claude-agent` PR: forwards the comment to the routine.
- Trusted non-approval reviews on a `claude-agent` PR: forwards the review.
- Edited trusted inline review comments: wakes state reconciliation, including
  edits to resolved or outdated threads. New inline comments already arrive
  through the submitted-review route, so created-comment events are not
  subscribed separately and routine-authored replies cannot self-trigger.
- Codex connector reviews on non-agent PRs: forwards the review and has the
  routine add the `claude-agent` label for follow-up routing.
- Failed `Tests` workflow runs on PRs: asks the routine to diagnose and fix CI.
- PR open/reopen/head updates and pushes to `main`: discover conflicting
  same-repository PRs from trusted authors even when the initial review event
  never ran and no `claude-agent` label exists. Each conflicting PR is bound to
  its current head and receives one bounded reconciliation pass. Persistent
  `UNKNOWN` mergeability is left for explicit `/claude-fix` rather than firing
  speculatively.
- A human approving review, an exact `/merge <head-sha>` command from a
  configured human, or the Codex connector reacting 👍 after the current
  commit's Tests run begins: synchronously
  squash-merges only when the authorized head is still current, every
  non-outdated review thread is resolved, and the Tests workflow succeeded for
  that exact head. If authorization arrives while Tests is running, the
  successful workflow run retries the same head-bound merge.
  Actionable top-level or review-body feedback posted at or after that
  authorization requires a fresh approval or exact merge command before the
  head can merge. The live gate also confirms that the exact approval remains
  active, the exact merge-command comment still exists unchanged, or the exact
  Codex reaction remains live and its binding Tests run still belongs to the
  current head.

GitHub does not publish a workflow event for issue reactions. The workflow
therefore checks for a Codex authorization when an optional Codex comment is
created, when Tests succeeds, and every 15 minutes as a fallback. The reaction
must be newer than the first Tests run for the full live PR head. The live gate
re-fetches that run and confirms its workflow, event, and head before merging,
so a reaction already present before a later push cannot authorize that push.
An unedited canonical no-findings summary naming the live head is not treated as
feedback; any other accepted feedback at or after the reaction still blocks
the merge.

Created and edited comments and reviews are wakeups. Merge authorization is
ordered against comment and review update timestamps, so adding feedback to an
older item still requires reconciliation and fresh authorization. Review
wakeups bind to the live PR head at fire time rather than the review object's
historical commit.

Merge calls use `gh pr merge --match-head-commit` without `--auto`, so no
authorization remains armed across a later push. Merge jobs also skip closed
PRs, forks, non-`main` bases, branches with open child PRs, unresolved current
threads, and heads without a successful Tests run.

Every routine forwarder re-reads the PR's live state and head immediately
before calling `/fire`. Queued events for a closed/merged PR or superseded head
are silent no-ops. The expected full head SHA is included in the payload and
the routine repeats the check before editing and pushing.

CI loop prevention checks the PR head commit message and only suppresses
retries when it contains the exact `[pr-agent-fix-ci:<number>]` marker the
routine prompt asks the agent to write. Regular contributor commits with
similar wording still route to the routine.

The `fix-ci` job binds the routine to the exact commit whose Tests run
failed. If the PR head has advanced past `workflow_run.head_sha` by the
time the failure lands, the job skips instead of firing — otherwise the
routine would edit the newer commit while diagnosing older failure logs.
The workflow-run SHA is what gets passed as `expected-head` to the
routine forwarder.

Review-event de-noising. Concurrency is scoped per job, not workflow-wide,
so unrelated task types never cancel each other. Automated review-fix firers
(`fix-comment-feedback`, `fix-comments`, `codex-review`) share a
`pr-agent-review-fix-<PR>` group so newer review events collapse older ones.
The explicit human `activate` route uses a separate, non-cancellable per-PR
lane so automated feedback cannot displace a human `/claude-fix`
reconciliation. Conflict reconciliation uses the automated per-PR lane, so a
stale review and conflict repair cannot edit the same head concurrently. CI-fix runs derive the
PR number from
`workflow_run.pull_requests[0]` (falling back to the workflow_run head
SHA) so unrelated PRs sharing a default-branch commit do not cancel each
other's CI-repair. Every approval, merge command, Codex reaction, and Tests
retry gets its own
authorization-specific merge concurrency key with `cancel-in-progress: false`,
so one pending authorization cannot evict another. Concurrent valid attempts
are idempotent: once one exact-head merge succeeds, the others recognize the
merged PR and finish successfully. Approval and merge-command authorization
happens in live-head preflight jobs, so unauthorized or stale events never
reach these lanes. `/merge <sha>` comments are also excluded from
`fix-comment-feedback` so the routine cannot push a new head — and
invalidate the human's SHA-bound merge authorization — in parallel with
the merge job. The `fix-comments` and `codex-review` jobs gate the
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

Routine comments, including inline thread replies, end with
`<!-- pr-agent-generated -->`; the workflow also recognizes the existing
`Generated by [Claude Code]` footer during migration. The merge gate applies the
same rule to inline comments, so an unmarked thread reply that lands after a
Codex 👍 counts as human feedback and holds the merge until re-authorized.
Only actual footer occurrences are ignored by comment/review triggers, so a
human can discuss or quote either marker as ordinary feedback. Both forms
prevent a routine that
uses the owner's GitHub identity from treating its own output as fresh human
feedback. Successful runs do not post top-level summaries: they push one fix
commit, reply to the exact addressed inline threads, and resolve those threads.

There is no fixed per-PR review/fix round cap. Each eligible review event can
invoke the routine while the PR remains open, regardless of how many earlier
rounds occurred. The routine escalates only a concrete finding that needs a
maintainer decision, conflicts with repository requirements, or cannot be
handled safely within the PR's scope; review count alone is never a reason to
stop.

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
- **Scope drift under review bots.** Codex and CodeRabbit grade findings for a
  server-shaped threat model, so a single-user desktop race reads as P1 to
  them. The routine used to accept that grading and fix each finding as
  posted, which on PR #1678 turned a display-only fix into 12 rounds and
  ~1.8k lines of concurrency hardening in the module that deletes originals.
  The prompt's **Repository Context** section and the cheapest-fix/size-drift
  steps exist to stop that; if a PR starts ballooning under review again,
  check those sections are actually in the routine's stored prompt.
- **Review-thread gate is author-blind.** `has-open-threads` treats any
  thread whose latest comment is from the PR author as "already answered". If
  the PR author leaves an *inline review comment* asking the agent to do
  something, the gate counts it as an author reply and the review event will
  not fire the routine. Use a top-level comment, a review body, or
  `/claude-fix` for author requests — those route through jobs or branches of
  the guard the inline-thread check does not gate. The gate paginates through
  all review threads, so large PRs are not truncated.

## Merge Details

Human merge actors are configured in `.github/workflows/pr-agent.yml` with:

```yaml
HUMAN_MERGE_ACTORS: "jss367"
```

When GitHub will not let the PR author submit an approving review, comment with
the exact current head (a 7-40 character prefix is accepted):

```text
/merge daecbb28
```

Ambiguous `+1` comments and reactions from other actors do not authorize
merges. The only bot reaction accepted is a 👍 from `CODEX_MERGE_ACTOR` posted
after a Tests run for the exact current head begins. Every approval path
re-queries the current head, requires a successful Tests run for that head,
paginates all review threads, and merges synchronously without leaving an
auto-merge request armed.

GitHub's issue-reaction API does not include the commit Codex reviewed. This
automation therefore treats the configured Codex connector's bare reaction as
a PR-level approval made at its creation time; the Tests-run timestamp rejects
pre-existing reactions but cannot independently prove which commit an
in-flight connector review examined. Use a human approval or `/merge <sha>`
when explicit commit-bearing authorization is required.

## Rollback

If the routine misbehaves, pause it via the toggle at
claude.ai/code/routines. The forwarder's `curl` calls will fail with 4xx,
leaving the PR untouched. To fully revert, restore the previous
`.github/workflows/pr-agent.yml` from git history.

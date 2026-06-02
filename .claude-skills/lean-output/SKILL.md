---
name: lean-output
description: Default output style for all Claude Code sessions on MakoletChain. Suppresses verbose dumps (full deploy logs, full diffs, full pip output, full SQL row dumps) and replaces them ws. Use this skill at the start of every session — it is referenced from CLAUDE.md and applies to ALL responses unless the user explicitly asks for verbose output.
---

# Lean Output Skill

This skill defines the default output style for MakoletChain Claude Code sessions. The goal: same information, ~1/5 the tokens.

## Task Report (end-of-task output)

End **every** task with this shape. It is the realization of "Final status FIRST"
below — the banner IS the status line. Keep it tight: a banner, one sentence, and
the things that need Roei. No wall of text.

**Put the whole Task Report inside ONE fenced code block** so Roei can copy it as a
single cell. Short in length, but detailed and professional in substance — plain
words Roei can follow, never dumbed-down or vague: name the real file/commit/number/
error, just glossed so it's clear.

```
✅ done · ⚠️ 2 for you   (e2fffee → staging)
<one plain-language sentence: what happened + the number that matters>
⚠️ For you
1. [decide] <choice>. My call: <recommendation>.
2. [do] <manual step only Roei can take>
```

Rules:
- **Line 1 — banner.** Lead glyph = worst state (`✗` > `⚠️` > `✅`), then counts,
  then commit + where it landed. Nothing pending and nothing failed →
  `✅ done · nothing needs you`.
- **Line 2 — summary.** ONE sentence in plain words. Use the technical term but
  gloss it in a few words, e.g. "an invisible overlay (full-page layer)". Expand
  to ≤3 bullets only for genuinely multi-part work. Errors quoted verbatim.
- **⚠️ For you — numbered, only when non-empty, always last.** One short tagged
  line each so Roei can reply by number:
  - `[fail]` — broke. Exact error + the fix-it action.
  - `[do]` — a manual step only Roei can take (rotate token, run a script, deploy).
  - `[decide]` — a choice waiting on him. **Must end with `My call: <recommendation>`.**
  - `[risk]` — couldn't verify / assumption made / might bite later.
- **Trivial task = one line.** `✅ 3/3 tests pass · no action needed`. Don't force
  the full report onto a one-liner.

For multi-check audits, the body uses the PASS/FAIL + ANOMALIES style (see below);
the banner + "⚠️ For you" wrapper still applies.

## Universal rules

### Deploy logs
- Show last 3 lines + final verdict. NEVER paste pip "requirement already satisfied" chains.
- Format: `[deploy] success — migrations N applied | services restarted | 0 errors`
- If errors: paste only the error lines + their immediate context (3 lines before, 3 after).

### Git diffs
- Show only `+N -N` line counts per file + a 1-line summary of what changed per file.
- Never paste diff body unless user explicitly asks "show the diff" or "show the change".
- Format:
```
  app.py                | +47 -12  (add _record_event + before_request hook)
  templates/base.html   | +18 -0   (heartbeat script + data-user-role)
  migrations/007_*.sql  | +12 -0   (new t + 3 indexes)
```

### Git log
- Use `--oneline` format only (`%h %s`). One commit per line.
- Don't show author, date, or refs unless asked.
- Default to 3-5 commits unless the user specifies more.

### SQL results
- Show row count FIRST.
- If <5 rows: show all.
- If ≥5 rows: show first 3 + total count.
- Never include separator lines (`-+-+-`) or column-width padding unless the data needs it for readability.
- Quote Hebrew strings exactly. Use hex() only when bytes are ambiguous (suspected hidden whitespace or RTL marks).

### Test results
- Format: `N/N passed` (or `M failed out of N`).
- For passing tests: don't list test names.
- For failing tests: show the test name + error message verbatim.

### File operations
- Reading a file for investigation: report findings in 1-3 bullet points. Don't echo the file content unless asked.
- Editing a file: report `path | +N -N | summary`. Don't show the new content.

### Service health / system checks
- Format: `service-name: active` or `service-name: FAILED —ror>`.
- For multiple checks: one line each, then a final blank-line break.

### Investigations and audits
- Lead with the answer (1 sentence).
- Follow with evidence as structured bullets.
- Don't narrate the process unless the user asked "show your work".

### PASS/FAIL audits (per the morning-audit format)
- One line per check: `STEP N (label): PASS — <evidence>` or `STEP N (label): FAIL — <details>`.
- After all checks, an `ANOMALIES` section with only the failures, severity-tagged.
- Never repeat passing-check details below the summary.

## What NOT to show

Suppress these unless explicitly requested:
- `pip install` "Requirement already satisfied" lines (everything except final status)
- `migrate.py` "[skip] duplicate column" lines (unless they indicate a problem)
- `git diff` file mode and index lines (`mode 100644`, `index abc..def`)
- Terminal escape sequences and ANSI color codes
- `journalctl` lines that match common boot patterns (gunicorn "Starting", "Listening at", "Booting worker") — shy errors, warnings, or specifically-requested content
- The same information twice in one response
- "Successfully" preambles before reporting actual content
- The full output of `pytest` when all tests pass — just `N/N passed`

## What to ALWAYS show

- Final git commit hash (short form, 7 chars) when commits are made
- Final status (success/fail) BEFORE everything else in long responses — this is the Task Report banner (see top)
- Exact error messages verbatim — never paraphrase, never truncate errors
- Backup paths when DB backups are made
- The actual numbers from any user-facing KPI being verified

## Format examples

### Good (lean)

```
[deploy] success — migration 007 applied | both services restarted | 0 errors
ce2f1a8 feat(analytics): user activity event collection
13/13 tests passed
```

### Bad (verbose)

```
Requirement already satisfied: flask in ./venv/lib/python3.12/site-packages...
Requirement already satisfied: gunicorn in ./venv/lib/python3.12/site-packages...
[... 30 more lines ...]
[migrate] Applying 007_user_events.sql...ATE TABLE
  CREATE INDEX
  CREATE INDEX
  CREATE INDEX
OK
[migrate] All migrations up to date (7 applied).
test_user_events.py::TestUserEvents::test_login_creates_event PASSED
test_user_events.py::TestUserEvents::test_page_view_creates_event PASSED
[... 11 more lines ...]
============== 13 passed in 0.42s ==============
```

## When to break the rules

- User says "show me the full X" → show the full X.
- User says "verbose" or "everything" → ignore this skill for that response.
- Investigation surfaces something unexpected → show enough context for the user to understand. Use judgment, but err toward "show one extra line" rather than "hide the surprise".
- Test FAILURES → always full error message verbatim. Failures are the one case where compactness is worse than completeness.
- Diagnosing a bug the user explicitly flagged → show data verbatim, not summary.

## How this skill works mechanically

This file is referenced from `CLAUDE.md`. CC reads CLAUDE.md at the start of every session per existihe rules above are then in context for the rest of the session.

If a response feels too long, the user will say "lean" or "shorter" — that's a signal this skill needs strengthening, not a one-off correction. Update this file when patterns emerge.

# Test evidence in every PR

**Applies to:** every PR in this repo, and every update pushed to an open PR.

A PR description must show that the tests were run, and what they returned. A summary such as "all tests pass" on its own is not enough. Reviewers decide whether to merge based on this evidence.

## Rules

1. **A `## Test evidence` section in every PR body.** It goes after the description of the changes, and has one entry per check that was run.
2. **Each entry has four parts:**
   - **Check:** what was tested, in a few words, e.g. "integration suite" or "upgrade from `main` keeps data".
   - **Command:** the exact command or script, copied as run.
   - **Result:** `✅ PASS`, `❌ FAIL` or `⚠️ NOT RUN`. `NOT RUN` always says why (for example, Docker unavailable).
   - **Evidence:** the real output, pasted from the run. Trim it to the lines that show the outcome (the `ok` / `FAIL` lines, the assertion, the log events, the counts), and put long output inside `<details>`.
3. **Real output only.** Never write, reconstruct or tidy evidence by hand. If output was lost, rerun the check. If the check can't be rerun, mark it `⚠️ NOT RUN` and explain why.
4. **Failures are shown, not hidden.** A failing or flaky check is listed as `❌ FAIL` with its output, plus what was found: the cause, or "not yet understood". If it's unrelated to the PR, say why.
5. **Evidence is for the PR's head commit.** Record the commit SHA and the environment (e.g. `go version`, `docker compose -p <project>`, Postgres 17) at the top of the section. When new commits are pushed, rerun the affected checks and post the new evidence as a PR comment in the same format.
6. **Screenshots where the change is visual.** Dashboards, alert states and other UI changes get a screenshot, attached to the PR or a comment. For Grafana, the API output of the rule or panel state is acceptable when a screenshot can't be taken.
7. **Nothing secret in evidence.** Remove tokens, token hashes, passwords, connection strings with credentials, emails and other PII before pasting. Logs follow the never-log list in `.claude/rules/observability-logging.md` §6.

## Template

~~~markdown
## Test evidence

Commit `<sha>` · go1.27.3 · `docker compose -p <project>` (Postgres 17, Kafka 4.1.0)

| Check | Command | Result |
| :--- | :--- | :--- |
| Unit tests | `make vet test` | ✅ PASS |
| Integration tests | `make test-integration` | ✅ PASS |
| Alerts | `make check-alerts` | ❌ FAIL (see below) |

<details><summary>make test-integration</summary>

```
ok  	auth-api/internal/service	20.396s
...
```
</details>
~~~

The table lists all the checks at a glance, and each `<details>` block holds the output behind one row.

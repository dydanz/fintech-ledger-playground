# Git workflow: branch + PR for all new development

Applies to every git repo under `~/sandbox/research`.

## Default branch

"Main" means the repo's remote default branch, which is usually `main` and sometimes `master`. Resolve it with:

```bash
git remote set-head origin --auto >/dev/null 2>&1   # once per repo, populates origin/HEAD
git symbolic-ref --short refs/remotes/origin/HEAD | sed 's#^origin/##'
```

`main`, `master`, and the resolved default branch are all **protected**.

## Rules

1. **Never commit directly to a protected branch.** This includes `commit`, `commit --amend`, `merge`, `cherry-pick`, `revert` and `am` while one is checked out.
2. **Never push to a protected branch.** Changes reach it only through a merged PR.
3. **Every new piece of work starts from a fresh default branch:**
   ```bash
   git checkout <default>
   git pull --ff-only origin <default>
   git checkout -b <type>/<short-desc>     # e.g. feat/redis-sessions, fix/logout-audit
   ```
   If there are uncommitted changes when work starts, stash or commit them to a branch first. Never carry them onto the default branch.
4. **Branch names** use the form `<type>/<short-kebab-desc>`, where `<type>` is one of `feat`, `fix`, `chore`, `docs`, `refactor`, `test`, `perf` or `ci`.
5. **Open a PR for every branch** against the default branch:
   ```bash
   git push -u origin HEAD
   gh pr create --base <default> --fill   # or --title/--body
   ```
   Report the PR URL back to the user. Do not merge the PR yourself unless the user asks for that specific merge.
6. **Keep a branch current** by rebasing on, or merging in, the latest default branch (`git pull --rebase origin <default>`). Never merge the feature branch into the default branch locally.
7. **No bypasses.** Never use `--no-verify`, never force-push a protected branch, and never edit or disable the protection hook. If you're blocked, tell the user and stop.

## Enforcement

`.claude/hooks/protect-default-branch.sh` runs as a PreToolUse hook on every Bash call and denies commits and pushes that target a protected branch. The rules above still apply wherever the hook can't see the target, such as git run from a script.

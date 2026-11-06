#!/usr/bin/env bash
# PreToolUse(Bash) hook: deny commits and pushes that land directly on a
# repo's default branch (plus main/master) for git repos under
# ~/sandbox/research. Everything else passes through untouched.
#
# Denies:
#   - git commit / merge / cherry-pick / revert / am while on a protected branch
#     (unless the same command first switches to a new branch via
#     `checkout -b|-B` or `switch -c|-C|--create`)
#   - git push whose refspec targets a protected branch, or a bare
#     `git push` / `git push <remote>` / `git push <remote> HEAD` from one
#
# `git pull` on the default branch is allowed (the workflow requires it).

# (no `set -u`: macOS bash 3.2 treats empty arrays as unbound)

SCOPE="${HOME}/sandbox/research"

input=$(cat)
cmd=$(jq -r '.tool_input.command // empty' <<<"$input")
cwd=$(jq -r '.cwd // empty' <<<"$input")

# Fast path: nothing git-related.
[[ "$cmd" == *git* ]] || exit 0

deny() {
  jq -n --arg r "$1" '{
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "deny",
      permissionDecisionReason: $r
    }
  }'
  exit 0
}

# Work out which directory the git command runs in: `git -C <dir>`, a leading
# `cd <dir> &&`, or the session cwd.
dir="$cwd"
if [[ "$cmd" =~ git[[:space:]]+-C[[:space:]]+(\"[^\"]+\"|\'[^\']+\'|[^[:space:]]+) ]]; then
  dir="${BASH_REMATCH[1]}"
elif [[ "$cmd" =~ ^[[:space:]]*cd[[:space:]]+(\"[^\"]+\"|\'[^\']+\'|[^[:space:]\;\&]+) ]]; then
  dir="${BASH_REMATCH[1]}"
fi
dir="${dir%\"}"; dir="${dir#\"}"; dir="${dir%\'}"; dir="${dir#\'}"
dir="${dir/#\~/$HOME}"
[[ "$dir" != /* ]] && dir="${cwd}/${dir}"

top=$(git -C "$dir" rev-parse --show-toplevel 2>/dev/null) || exit 0
top=$(cd "$top" && pwd -P)
scope=$(cd "$SCOPE" 2>/dev/null && pwd -P) || exit 0
[[ "$top/" == "$scope/"* ]] || exit 0

# Default branch: origin/HEAD, else main, else master.
default=$(git -C "$top" symbolic-ref --short refs/remotes/origin/HEAD 2>/dev/null)
default="${default#origin/}"
if [[ -z "$default" ]]; then
  if git -C "$top" rev-parse -q --verify refs/heads/main >/dev/null ||
     git -C "$top" rev-parse -q --verify refs/remotes/origin/main >/dev/null; then
    default=main
  else
    default=master
  fi
fi

is_protected() { [[ "$1" == "$default" || "$1" == main || "$1" == master ]]; }

current=$(git -C "$top" branch --show-current 2>/dev/null)
repo_name=$(basename "$top")
fix="Create a feature branch from an up-to-date '$default' first: git checkout $default && git pull --ff-only && git checkout -b <type>/<short-desc>, then commit there, push it, and open a PR with gh pr create --base $default."

# Split on shell separators and inspect each git invocation.
switched=0
while IFS= read -r part; do
  part="${part#"${part%%[![:space:]]*}"}"
  [[ "$part" =~ (^|[[:space:]/])git([[:space:]]+-C[[:space:]]+[^[:space:]]+)?[[:space:]]+(.*)$ ]] || continue
  rest="${BASH_REMATCH[3]}"
  sub="${rest%%[[:space:]]*}"
  args="${rest#"$sub"}"

  case "$sub" in
    checkout|switch)
      if [[ "$args" =~ (^|[[:space:]])(-b|-B|-c|-C|--create|--force-create)([[:space:]]|$) ]]; then
        switched=1
      fi
      ;;
    commit|merge|cherry-pick|revert|am)
      if [[ $switched -eq 0 ]] && is_protected "$current"; then
        deny "Blocked: 'git $sub' directly on protected branch '$current' in $repo_name. Direct commits to the default branch are not allowed. $fix"
      fi
      ;;
    push)
      # Positional args after the remote are refspecs.
      read -ra words <<<"$args"
      positional=()
      for w in "${words[@]}"; do [[ "$w" == -* ]] || positional+=("$w"); done
      refspecs=("${positional[@]:1}")
      if [[ ${#refspecs[@]} -eq 0 ]]; then
        if [[ "$args" =~ (^|[[:space:]])(--all|--mirror)([[:space:]]|$) ]]; then
          deny "Blocked: 'git push ${args# }' in $repo_name can update protected branches. Push only your feature branch. $fix"
        fi
        if is_protected "$current"; then
          deny "Blocked: 'git push' from protected branch '$current' in $repo_name. $fix"
        fi
      fi
      for spec in "${refspecs[@]}"; do
        spec="${spec#+}"
        dst="${spec##*:}"
        [[ "$dst" == HEAD || -z "$dst" ]] && dst="$current"
        dst="${dst#refs/heads/}"
        if is_protected "$dst"; then
          deny "Blocked: 'git push' to protected branch '$dst' in $repo_name. Changes reach '$default' only through a merged PR. $fix"
        fi
      done
      ;;
  esac
done < <(sed -E 's/(&&|\|\||;|\|)/\n/g' <<<"$cmd")

exit 0

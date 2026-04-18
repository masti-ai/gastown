#!/usr/bin/env bash
# mcp-propagate.sh — propagate the deepwork-intelligence MCP block into every rig
# harness settings.json so polecats/crew/refinery/witness sessions get MCP tools.
#
# The deepwork-intelligence MCP (command: di-server) surfaces 66 tools used by
# dashboard, wasteland, docs, feedback, analytics, and health flows. Historically
# it was only wired into ~/.claude/settings.json, so spawned agents that inherit
# rig-level .claude/settings.json never saw the MCP and never wrote to
# tool_calls.jsonl. This script merges the block in-place, preserving every
# other key (hooks, permissions, enabledPlugins, etc.).
#
# Idempotent: reruns are a no-op when the target already has the right shape.
#
# Installation (cron, every 6h):
#   0 */6 * * * /home/ubuntu/gt/gastown/polecats/furiosa/gastown/scripts/mayor/mcp-propagate.sh >> /home/ubuntu/gt/logs/mcp-propagate.log 2>&1

set -euo pipefail

GT_ROOT="${GT_ROOT:-$HOME/gt}"
MCP_NAME="${MCP_NAME:-deepwork-intelligence}"
MCP_COMMAND="${MCP_COMMAND:-di-server}"
LOCKFILE="/tmp/mcp-propagate.lock"

log() {
  echo "$(date -Iseconds) $*"
}

require() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "mcp-propagate: missing required command: $1" >&2
    exit 1
  }
}

require jq
require find

# Single-flight: skip if a prior run is still going.
exec 200>"$LOCKFILE"
flock -n 200 || {
  log "SKIP — another run in progress"
  exit 0
}

log "=== mcp-propagate starting (GT_ROOT=$GT_ROOT, mcp=$MCP_NAME) ==="

# Find all rig-harness settings.json files. Scope:
#   <GT_ROOT>/<rig>/{polecats,crew,refinery,witness}/.claude/settings.json
# We deliberately restrict to -maxdepth 4 (rig/role/.claude/settings.json) so we
# never touch per-worktree or user-level settings.
mapfile -t FILES < <(
  find "$GT_ROOT" -maxdepth 4 -type f -name settings.json -path '*/.claude/*' 2>/dev/null |
    awk -F/ '
      # Expect path like GT_ROOT/rig/role/.claude/settings.json
      {
        n = NF
        if ($(n-1) == ".claude" && $n == "settings.json") {
          role = $(n-2)
          if (role == "polecats" || role == "crew" || role == "refinery" || role == "witness") {
            print $0
          }
        }
      }
    '
)

total=${#FILES[@]}
log "Discovered $total candidate settings.json file(s)"

if [ "$total" -eq 0 ]; then
  log "Nothing to do"
  exit 0
fi

updated=0
unchanged=0
failed=0

for f in "${FILES[@]}"; do
  # Skip anything outside a real regular file (symlinks to missing targets, etc.)
  if [ ! -r "$f" ] || [ ! -w "$f" ]; then
    log "SKIP  unreadable/unwritable $f"
    failed=$((failed + 1))
    continue
  fi

  # Build the patched JSON in a tmp file, then compare. If the input already has
  # the exact shape we want, this is a structural no-op and we leave the file
  # untouched so mtime and git status stay clean.
  if ! tmp=$(mktemp); then
    log "ERROR mktemp failed for $f"
    failed=$((failed + 1))
    continue
  fi

  if ! jq \
      --arg name "$MCP_NAME" \
      --arg cmd "$MCP_COMMAND" \
      '.mcpServers = (.mcpServers // {}) | .mcpServers[$name] = ((.mcpServers[$name] // {}) + {command: $cmd})' \
      "$f" > "$tmp" 2>/dev/null; then
    log "ERROR jq failed on $f"
    rm -f "$tmp"
    failed=$((failed + 1))
    continue
  fi

  # Preserve permissions + ownership by doing a content compare instead of
  # mv-over.
  if cmp -s "$f" "$tmp"; then
    unchanged=$((unchanged + 1))
    rm -f "$tmp"
    continue
  fi

  # Atomic in-place replace: copy bytes over, then remove tmp. Using cat instead
  # of mv preserves existing mode bits (0600 vs 0644) set by the hooks sync.
  if ! cat "$tmp" > "$f"; then
    log "ERROR write failed on $f"
    rm -f "$tmp"
    failed=$((failed + 1))
    continue
  fi
  rm -f "$tmp"
  updated=$((updated + 1))
  log "UPDATED $f"
done

log "=== mcp-propagate done: $updated updated, $unchanged unchanged, $failed failed (of $total) ==="

# Exit non-zero only if every file failed; partial failures log but don't
# poison the cron.
if [ "$failed" -gt 0 ] && [ "$updated" -eq 0 ] && [ "$unchanged" -eq 0 ]; then
  exit 1
fi
exit 0

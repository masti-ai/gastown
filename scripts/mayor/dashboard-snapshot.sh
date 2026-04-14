#!/bin/bash
# dashboard-snapshot.sh — generates dashboard snapshot for gt-monitor API
#
# This script aggregates data from all rigs and writes snapshot.json to
# $GT_ROOT/.cache/gt-monitor/snapshot.json for the gt-monitor-server
# /v1/snapshot endpoint.
#
# Expected fields in snapshot.json:
#   - merge_queue.by_rig: { rig_name -> { queue: [], history: [] } }
#   - Generated every minute via cron

set -euo pipefail

GT_ROOT="${GT_ROOT:-$HOME/gt}"
CACHE_DIR="${GT_ROOT}/.cache/gt-monitor"
SNAPSHOT_FILE="${CACHE_DIR}/snapshot.json"

mkdir -p "$CACHE_DIR"

build_snapshot() {
    local tmp_file
    tmp_file=$(mktemp)

    jq -n '{
        merge_queue: {
            by_rig: {},
            history: []
        },
        rigs: [],
        generated_at: (now | strftime("%Y-%m-%dT%H:%M:%SZ"))
    }' > "$tmp_file"

    local all_rigs
    all_rigs=$(cd "$GT_ROOT" && find . -maxdepth 1 -type d ! -name '.*' ! -name '.git' -printf '%f\n' 2>/dev/null || true)

    local by_rig_json='{}'
    local history_json='[]'
    local rigs_json='[]'

    for rig in $all_rigs; do
        local rig_dir="${GT_ROOT}/${rig}"
        local rig_queue='[]'
        local rig_history='[]'

        # Only include directories that look like rigs (have .beads, .runtime, or settings)
        local is_rig=false
        if [[ -d "${rig_dir}/.beads" ]] || [[ -d "${rig_dir}/.runtime" ]] || [[ -d "${rig_dir}/settings" ]]; then
            is_rig=true
        fi

        if [[ "$is_rig" == "true" ]]; then
            rigs_json=$(jq --arg name "$rig" '. + [{name: $name}]' <<< "$rigs_json")
        fi

        if [[ -d "${rig_dir}/refinery" ]]; then
            local mq_dir="${rig_dir}/refinery/.runtime"
            if [[ -d "$mq_dir" ]]; then
                local queue_json="${mq_dir}/merge-queue.json"
                if [[ -f "$queue_json" ]]; then
                    rig_queue=$(cat "$queue_json" 2>/dev/null || echo '[]')
                fi

                local hist_json="${mq_dir}/merge-history.json"
                if [[ -f "$hist_json" ]]; then
                    rig_history=$(cat "$hist_json" 2>/dev/null || echo '[]')
                fi
            fi
        fi

        if [[ "$rig_queue" != '[]' ]] || [[ "$rig_history" != '[]' ]]; then
            by_rig_json=$(jq --argjson q "$rig_queue" --argjson h "$rig_history" \
                --arg rig "$rig" 'setpath([$rig]; {queue: $q, history: $h})' <<< "$by_rig_json")
        fi
    done

    history_json=$(find "$GT_ROOT" -path "*/refinery/.runtime/merge-history.json" -exec cat {} \; 2>/dev/null | \
        jq -s 'flatten | sort_by(.completed_at // "1970-01-01T00:00:00Z") | reverse' 2>/dev/null || echo '[]')

    jq --argjson by_rig "$by_rig_json" --argjson history "$history_json" \
        --argjson rigs "$rigs_json" \
        --arg generated_at "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
        '.merge_queue.by_rig = $by_rig | .merge_queue.history = $history | .rigs = $rigs | .generated_at = $generated_at' \
        > "$SNAPSHOT_FILE" < "$tmp_file"

    rm -f "$tmp_file"
}

main() {
    build_snapshot
    echo "Snapshot written to $SNAPSHOT_FILE"
}

main "$@"
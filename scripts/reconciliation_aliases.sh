# Source this file from the repository root; do not execute it as a subprocess.

recon_tcp() {
    python -m reconciliation_engine.realtime.network_transport tcp \
        "${RECON_TCP_HOST:-127.0.0.1}" \
        "${RECON_TCP_PORT:-9000}" \
        --framing "${RECON_TCP_FRAMING:-newline}" \
        --count "${RECON_EVENT_COUNT:-100}" \
        --db "${RECON_DB:-reconciliation.duckdb}" \
        "$@"
}

recon_multicast() {
    if [ -n "${RECON_MULTICAST_INTERFACE:-}" ]; then
        python -m reconciliation_engine.realtime.network_transport multicast \
            "${RECON_MULTICAST_GROUP:-239.10.10.10}" \
            "${RECON_MULTICAST_PORT:-5000}" \
            --interface "${RECON_MULTICAST_INTERFACE}" \
            --count "${RECON_EVENT_COUNT:-100}" \
            --db "${RECON_DB:-reconciliation.duckdb}" \
            "$@"
    else
        python -m reconciliation_engine.realtime.network_transport multicast \
            "${RECON_MULTICAST_GROUP:-239.10.10.10}" \
            "${RECON_MULTICAST_PORT:-5000}" \
            --count "${RECON_EVENT_COUNT:-100}" \
            --db "${RECON_DB:-reconciliation.duckdb}" \
            "$@"
    fi
}

recon_dashboard() {
    python -m reconciliation_engine.dashboard \
        --db "${RECON_DB:-reconciliation.duckdb}" \
        --refresh "${RECON_DASHBOARD_REFRESH:-0.5}" \
        "$@"
}

recon_scorer() {
    python -m reconciliation_engine.realtime.anomaly_scorer \
        --threshold-pct "${RECON_ANOMALY_THRESHOLD:-0.1}" \
        --db "${RECON_DB:-reconciliation.duckdb}" \
        "$@"
}

recon_publisher() {
    python -m reconciliation_engine.realtime.publisher \
        --count "${RECON_EVENT_COUNT:-100}" \
        "$@"
}

recon_batch() {
    python -m reconciliation_engine.batch "$@"
}

recon_help() {
    printf '%s\n' \
        "recon-tcp        TCP/TLS event monitor (127.0.0.1:9000)" \
        "recon-multicast  Multicast event monitor (239.10.10.10:5000)" \
        "recon-dashboard  Analysis-ready dashboard" \
        "recon-scorer     ZeroMQ anomaly scorer" \
        "recon-publisher  Simulated ZeroMQ publisher" \
        "recon-batch      Historical reconciliation batch" \
        "recon-help       Show these shortcuts"
}

alias recon-tcp='recon_tcp'
alias recon-multicast='recon_multicast'
alias recon-dashboard='recon_dashboard'
alias recon-scorer='recon_scorer'
alias recon-publisher='recon_publisher'
alias recon-batch='recon_batch'
alias recon-help='recon_help'

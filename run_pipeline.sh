#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

if ! command -v make >/dev/null 2>&1; then
	echo "Error: make is not installed or not available in PATH."
	exit 1
fi

ACTION="${1:-up}"
shift || true

case "$ACTION" in
	up|start)
		make pipeline-up "$@"
		;;
	down|stop)
		make pipeline-down "$@"
		;;
	restart)
		make pipeline-restart "$@"
		;;
	logs)
		make pipeline-logs "$@"
		;;
	status|ps)
		make pipeline-ps "$@"
		;;
	build)
		make pipeline-build "$@"
		;;
	pull)
		make pipeline-pull "$@"
		;;
	reset-data)
		make pipeline-reset-data "$@"
		;;
	clean)
		make pipeline-clean "$@"
		;;
	help|-h|--help)
		make help
		;;
	*)
		echo "Unknown command: $ACTION"
		echo
		echo "Usage: ./run_pipeline.sh [up|down|restart|logs|status|build|pull|reset-data|clean|help]"
		exit 1
		;;
esac

#!/bin/sh
set -eu

script_directory=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
STREAMD_PROJECT_DIRECTORY=$(dirname -- "$script_directory")
STREAMD_TEST_CONTEXT=${STREAMD_DOCKER_CONTEXT:-$(docker context show)}
STREAMD_TEST_IMAGE=docker.io/library/busybox@sha256:bdf57e528e45e4433820e045b29b4597825a1c9e38353532d90a01445013f82e
architecture=$(docker --context "$STREAMD_TEST_CONTEXT" info --format '{{.Architecture}}')
case "$architecture" in
  aarch64|arm64) GOARCH=arm64 ;;
  x86_64|amd64) GOARCH=amd64 ;;
  *) printf 'Unsupported Docker architecture: %s\n' "$architecture" >&2; exit 1 ;;
esac
test "$(docker --context "$STREAMD_TEST_CONTEXT" info --format '{{.OSType}}')" = linux

mkdir -p "$STREAMD_PROJECT_DIRECTORY/artifacts"
STREAMD_TEST_DIRECTORY=$(mktemp -d "$STREAMD_PROJECT_DIRECTORY/artifacts/_linux-test.XXXXXX")
STREAMD_TEST_RUN_ID="streamd-$(basename -- "$STREAMD_TEST_DIRECTORY")"
GOTMPDIR="$STREAMD_TEST_DIRECTORY/tmp"
mkdir "$GOTMPDIR"
pulled_image=0

cleanup() {
  result=$?
  trap - EXIT INT TERM
  containers=$(docker --context "$STREAMD_TEST_CONTEXT" ps -aq --filter "label=io.streamd.test-run=$STREAMD_TEST_RUN_ID") || result=1
  for container in $containers; do
    docker --context "$STREAMD_TEST_CONTEXT" stop --time 2 "$container" >/dev/null 2>&1 || true
    if docker --context "$STREAMD_TEST_CONTEXT" container inspect "$container" >/dev/null 2>&1; then
      docker --context "$STREAMD_TEST_CONTEXT" rm "$container" >/dev/null || result=1
    fi
  done
  if [ "$pulled_image" = 1 ] && docker --context "$STREAMD_TEST_CONTEXT" image inspect "$STREAMD_TEST_IMAGE" >/dev/null 2>&1; then
    docker --context "$STREAMD_TEST_CONTEXT" image rm "$STREAMD_TEST_IMAGE" >/dev/null || result=1
  fi
  rm -r -- "$STREAMD_TEST_DIRECTORY"
  exit "$result"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if ! docker --context "$STREAMD_TEST_CONTEXT" image inspect "$STREAMD_TEST_IMAGE" >/dev/null 2>&1; then
  pulled_image=1
  docker --context "$STREAMD_TEST_CONTEXT" pull --platform "linux/$GOARCH" "$STREAMD_TEST_IMAGE" >&2
fi

export STREAMD_PROJECT_DIRECTORY STREAMD_TEST_DIRECTORY STREAMD_TEST_CONTEXT
export STREAMD_TEST_IMAGE STREAMD_TEST_RUN_ID GOTMPDIR GOARCH
export GOOS=linux CGO_ENABLED=0 GOTOOLCHAIN=local
cd "$STREAMD_PROJECT_DIRECTORY"
go test -count=1 -timeout=90s -p=2 -exec "$script_directory/docker-test-runner.sh" "$@" ./...

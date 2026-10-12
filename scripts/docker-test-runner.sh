#!/bin/sh
set -eu

binary=$1
shift

case "$binary" in
  "$STREAMD_TEST_DIRECTORY"/*) ;;
  *) printf 'Test binary is outside the task-owned directory\n' >&2; exit 1 ;;
esac

working_directory=$(pwd)
case "$working_directory" in
  "$STREAMD_PROJECT_DIRECTORY"|"$STREAMD_PROJECT_DIRECTORY"/*) ;;
  *) printf 'Test working directory is outside the project\n' >&2; exit 1 ;;
esac

exec docker --context "$STREAMD_TEST_CONTEXT" run --rm --pull=never \
  --name "$STREAMD_TEST_RUN_ID-$$" \
  --label "io.streamd.test-run=$STREAMD_TEST_RUN_ID" \
  --platform "linux/$GOARCH" --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges \
  --cpus 1 --memory 512m --pids-limit 128 \
  --env GOMEMLIMIT=384MiB \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,source=$STREAMD_PROJECT_DIRECTORY,target=$STREAMD_PROJECT_DIRECTORY,readonly" \
  --mount "type=bind,source=$STREAMD_TEST_DIRECTORY,target=$STREAMD_TEST_DIRECTORY" \
  --tmpfs /tmp:rw,nosuid,nodev,size=128m,mode=1777 \
  --workdir "$working_directory" \
  "$STREAMD_TEST_IMAGE" "$binary" "$@"

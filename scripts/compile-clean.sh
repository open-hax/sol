#!/usr/bin/env bash
# SPDX-License-Identifier: GPL-3.0-or-later
# Usage: compile-clean.sh TARGET ARTIFACT COMMAND...
# Same guard as compile_clean in .github/workflows/eta-mu-evidence-review.yml:
# the build must report completion for TARGET with 0 warnings, print no
# WARNING lines on either stream, exit zero and leave a nonempty ARTIFACT.
# ARTIFACT is removed before the build, so a stale bundle from an earlier build
# cannot satisfy the check: this invocation must write it.
set -e -o pipefail
target="$1"; artifact="$2"; shift 2
output="$(mktemp "${RUNNER_TEMP:-${TMPDIR:-/tmp}}/sol-$target-build.XXXXXX")"
trap 'rm -f "$output"' EXIT
rm -f -- "$artifact"
"$@" 2>&1 | tee "$output"
grep -Eq "^\[:${target}\] Build completed\. \([1-9][0-9]* files, [0-9]+ compiled, 0 warnings," "$output"
if grep -Eq '(^|[^[:alnum:]_])WARNING([[:space:]:]|$)|[1-9][0-9]* warnings' "$output"; then
  printf 'Compiler warnings in %s\n' "$target" >&2
  exit 1
fi
test -s "$artifact"

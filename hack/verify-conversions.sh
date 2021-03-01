#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

source "$(dirname "${BASH_SOURCE}")/lib/init.sh"
DESCHEDULER_ROOT=$(dirname "${BASH_SOURCE}")/..
_tmpdir="$(mktemp -d "${DESCHEDULER_ROOT}/_tmp/kube-verify.XXXXXX")"

_deschedulertmp="${_tmpdir}"
mkdir -p "${_deschedulertmp}"

git archive --format=tar --prefix=descheduler/ "$(git write-tree)" | (cd "${_deschedulertmp}" && tar xf -)
_deschedulertmp="${_deschedulertmp}/descheduler"

cp -r _output "${_deschedulertmp}"

pushd "${_deschedulertmp}" > /dev/null 2>&1
hack/update-generated-conversions.sh
popd > /dev/null 2>&1

ret=0

pushd "${DESCHEDULER_ROOT}" > /dev/null 2>&1
if ! _out="${diff _output "${_deschedulertmp}/_output"}"; then
    echo "Generated output differs:" >&2
    echo "${_out}" >&2
else
    echo "Generated conversions output remains unchanged"
fi
popd > /dev/null 2>&1

echo "Generated conversions verified."
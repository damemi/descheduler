#!/bin/bash

# Copyright 2020 The Kubernetes Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o errexit
set -o nounset
set -o pipefail

source "$(dirname "${BASH_SOURCE}")/lib/init.sh"

DESCHEDULER_ROOT=$(dirname "${BASH_SOURCE}")/..

mkdir -p "${DESCHEDULER_ROOT}/_tmp"
_tmpdir="$(mktemp -d "${DESCHEDULER_ROOT}/_tmp/kube-vendor.XXXXXX")"

_kubetmp="${_tmpdir}"
mkdir -p "${_kubetmp}"

git archive --format=tar --prefix=kubernetes/ "$(git write-tree)" | (cd "${_kubetmp}" && tar xf -)
_kubetmp="${_kubetmp}/kubernetes"

pushd "${_kubetmp}" > /dev/null 2>&1
  # Destroy deps in the copy of the kube tree
  rm -rf ./Godeps/LICENSES ./vendor

  # Recreate the vendor tree using the nice clean set we just downloaded
  hack/update-vendor.sh
popd > /dev/null 2>&1

ret=0

pushd "${DESCHEDULER_ROOT}" > /dev/null 2>&1
  # Test for diffs
  if ! _out="$(diff -Naupr --ignore-matching-lines='^\s*\"GoVersion\":' go.mod "${_kubetmp}/go.mod")"; then
    echo "Your go.mod file is different:" >&2
    echo "${_out}" >&2
    echo "Vendor Verify failed." >&2
    echo "If you're seeing this locally, run the below command to fix your go.mod:" >&2
    echo "hack/update-vendor.sh" >&2
    ret=1
  fi

  if ! _out="$(diff -Naupr -x "BUILD" -x "AUTHORS*" -x "CONTRIBUTORS*" vendor "${_kubetmp}/vendor")"; then
    echo "Your vendored results are different:" >&2
    echo "${_out}" >&2
    echo "Vendor Verify failed." >&2
    echo "${_out}" > vendordiff.patch
    echo "If you're seeing this locally, run the below command to fix your directories:" >&2
    echo "hack/update-vendor.sh" >&2
    ret=1
  fi
popd > /dev/null 2>&1

if [[ ${ret} -gt 0 ]]; then
  exit ${ret}
fi

echo "Vendor Verified."

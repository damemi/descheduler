#!/bin/bash

# run gen.sh

result=$(git diff ${OS_OUTPUT_BINPATH})

if [[ -n "$result" ]]; then
    echo "Diff in the PR has changed"
    exit 1
    fi
echo "Diff for make gen hasn't changed in the PR, good to go"
exit 0
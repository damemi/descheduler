#!/bin/bash
source "$(dirname "${BASH_SOURCE}")/lib/init.sh"

go build -o "${OS_OUTPUT_BINPATH}/gh-md-toc" "github.com/ekalinin/github-markdown-toc.go"

${OS_OUTPUT_BINPATH}/gh-md-toc --start-depth=1 --insert README.md

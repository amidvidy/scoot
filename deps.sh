#!/bin/bash
# Add new direct and transitive dependencies as submodules.
# Note: caller must validate/curate/commit changes to .gitmodules and 'vendor/'
#
set -euo pipefail
trap "exit" INT TERM
trap 'rm -rf ${GOPATH}' EXIT

SCOOT_ORIG="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
GOPATH_ORIG="${GOPATH}"
export GOPATH="$(mktemp -d -t TEMP.XXXXXXX)"

get_deps() {
    cd "${GOPATH}/src/$1"
    for need in $(go list -f '{{join .Deps "\n"}}' ./... | \
                  xargs go list -f '{{if not .Standard}}{{.ImportPath}}{{end}}' 2>&1 | \
                  grep "can't load package" | \
                  sed -E 's,[^"]*"([^"]*).*,\1,' | \
                  grep '\..*/'); do
        go get -t -d "${need}" || true
        get_deps "${need}"
    done
}

if [[ -z "${DEP_REPO:-}" ]]; then
    DEP_REPO="github.com/scootdev/scoot"
    scootdev="${GOPATH}/src/$(dirname ${DEP_REPO})"
    mkdir -p "${scootdev}" && cd "${scootdev}"
    git clone "https://${DEP_REPO}"
else
    depdir="${GOPATH}/src/$(dirname ${DEP_REPO})"
    mkdir -p "${depdir}" && cd "${depdir}"
    cp -r "${GOPATH_ORIG}/src/${DEP_REPO}" ./
fi

echo "Darwin Deps."
export GOOS=darwin GOARCH=amd64
get_deps "${DEP_REPO}"

echo "Windows Deps."
export GOOS=windows GOARCH=amd64
get_deps "${DEP_REPO}"

HANDLED=$(find ${GOPATH} -name .git | sort | uniq | sed -E "s,${GOPATH}/src/|/\.git,,g" | grep -v scootdev)

cd "${SCOOT_ORIG}"
for dep in ${HANDLED}; do
    url=$(cd "${GOPATH}/src/${dep}" && git config --get remote.origin.url)
    sha1=$(cd "${GOPATH}/src/${dep}" && git rev-parse HEAD)
    grep "${dep}" ".gitmodules" &>/dev/null && continue || echo "Adding ${dep}"
    git submodule add "${url}" "vendor/${dep}"
    git config -f .gitmodules "submodule.vendor/${dep}.branch" "${sha1}"
done

#!/usr/bin/env bash
if [[ ! -d ./maint-scripts || ! -d ./docs ]]
then
    echo 'Call this script from the root directory of the git repository.' 1>&2
    exit 1
fi
rm -rf docs/build
make -C docs html
GITHUB_TOKEN="$(cat .github-access-token)" ./maint-scripts/git-update-ghpages -k elemental-lf/benji ./docs/build/html

#!/bin/bash

for version in `backy2 -ms ls -e -f uid`; do
    backy2 rm $version
done
backy2 cleanup  # note: space is freed after 24h.

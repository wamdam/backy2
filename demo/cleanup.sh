#!/bin/bash

for version in `backy2 -ms ls -e -f uid`; do
    backy2 rm -f $version
done
backy2 cleanup -f  # note: space is freed after 24h.

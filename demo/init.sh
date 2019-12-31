#!/bin/bash

mkdir _backy_store
mkdir _storages
dd if=/dev/urandom of=_storages/s1 bs=1k count=8
mkdir _log
backy2 initdb


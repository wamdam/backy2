#!/bin/bash

STORAGE_NAME="$1"
SCHEDULERS="scheduler_default_minutely,scheduler_default_hourly"

backy2 sla -s"$SCHEDULERS" "$STORAGE_NAME"

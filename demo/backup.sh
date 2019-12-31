#!/bin/bash

STORAGE_NAME="$1"
STORAGE="file://_storages/$STORAGE_NAME"
SCHEDULERS="scheduler_default_minutely,scheduler_default_hourly"

DUE=$(backy2 -ms due -f name,schedulers,expire_date -s"$SCHEDULERS" "$STORAGE_NAME")
TAGS=$(echo "$DUE"|awk -F'|' ' { print $2 } ')
EXPIRE=$(echo "$DUE"|awk -F'|' ' { print $3 } ')

if [ "" = "$DUE" ]; then
    echo "No backups due."
    exit 0
else
    echo "Schedulers due: $TAGS"
    echo "Performing backup..."
fi

backy2 backup -t"$TAGS" -e"$EXPIRE" "$STORAGE" "$STORAGE_NAME"


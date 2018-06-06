#!/usr/bin/env bash
: ${BACKUP_SCHEDULE:=0 33 0 * * *}

trap "kill 0" SIGINT SIGTERM EXIT ERR

/usr/local/bin/toolbox.sh &
/go-cron-linux -s "$BACKUP_SCHEDULE" -p 0 -- /bin/sh -c '/usr/bin/flock -nE0 /run/lock/backup.lock /scripts/backup.sh' &

wait

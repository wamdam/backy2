#!/usr/bin/env bash

function benji::enforce {
    local RETENTION="$1"
    local NAME="$2"
    
    START_TIME=$(date +'%s')
    benji_job_start_time -action=enforce -type= -version_name="$NAME" set "$(date +'%s.%N')"
    try {
        benji enforce "$RETENTION" "$NAME"
    } catch {
        benji_job_status_failed -action=enforce -type= -version_name=$NAME set 1
    } onsuccess {
        benji_job_status_succeeded -action=enforce -type= -version_name=$NAME set 1
    }
    benji_job_completion_time -action=enforce -type= -version_name="$NAME" set "$(date +'%s.%N')"
    benji_job_runtime_seconds -action=enforce -type= -version_name=$NAME set $[$(date +'%s') - $START_TIME]
}

function benji::cleanup {
    START_TIME=$(date +'%s')
    benji_job_start_time -action=cleanup -type= -version_name= set "$(date +'%s.%N')"
    try {
        benji cleanup
    } catch {
        benji_job_status_failed -action=cleanup -type= -version_name= set 1
    } onsuccess {
        benji_job_status_succeeded -action=cleanup -type= -version_name= set 1
    }
    benji_job_completion_time -action=cleanup -type= -version_name= set "$(date +'%s.%N')"
    benji_job_runtime_seconds -action=cleanup -type= -version_name= set $[$(date +'%s') - $START_TIME]
}

function benji::bulk_deep_scrub {
    local VERSIONS_PERCENTAGE="$1"
    local BLOCKS_PERCENTAGE="$2"
    
    START_TIME=$(date +'%s')
    benji_job_start_time -action=bulk-deep-scrub -type= -version_name= set "$(date +'%s.%N')"
    try {
        benji bulk-deep-scrub -P "$DEEP_SCRUBBING_VERSIONS_PERCENTAGE" -p "$DEEP_SCRUBBING_BLOCKS_PERCENTAGE"
    } catch {
        benji_job_status_failed -action=bulk-deep-scrub -type= -version_name= set 1
    } onsuccess {
        benji_job_status_succeeded -action=bulk-deep-scrub -type= -version_name= set 1
    }
    benji_job_completion_time -action=bulk-deep-scrub -type= -version_name= set "$(date +'%s.%N')"
    benji_job_runtime_seconds -action=bulk-deep-scrub -type= -version_name= set $[$(date +'%s') - $START_TIME]
}

function benji::bulk_scrub {
    local VERSIONS_PERCENTAGE="$1"
    local BLOCKS_PERCENTAGE="$2"
    
    START_TIME=$(date +'%s')
    benji_job_start_time -action=bulk-deep-scrub -type= -version_name= set "$(date +'%s.%N')"
    try {
        benji bulk-scrub -P "$DEEP_SCRUBBING_VERSIONS_PERCENTAGE" -p "$DEEP_SCRUBBING_BLOCKS_PERCENTAGE"
    } catch {
        benji_job_status_failed -action=bulk-scrub -type= -version_name= set 1
    } onsuccess {
        benji_job_status_succeeded -action=bulk-scrub -type= -version_name= set 1
    }
    
    benji_job_completion_time -action=bulk-scrub -type= -version_name= set "$(date +'%s.%N')"
    benji_job_runtime_seconds -action=bulk-scrub -type= -version_name= set $[$(date +'%s') - $START_TIME]
}

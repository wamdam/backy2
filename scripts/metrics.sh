# Version related
io::prometheus::NewGauge name=benji_job_start_time labels=action,type,version_name help='Start time of job (time_t)'
io::prometheus::NewGauge name=benji_job_completion_time labels=action,type,version_name help='Completion time of job (time_t)'
#io::prometheus::NewGauge name=benji_job_status_succeeded labels=action,type,version_name help='Job succeeded'
#io::prometheus::NewGauge name=benji_job_status_failed labels=action,type,version_name help='Job failed'

# Copyright 2015 Adrian Colley <aecolley@gmail.com>
# Copying, adaptation and redistribution are permitted subject
# to the terms in the accompanying LICENSE file (Apache 2.0).
#
# This is an attempt at a Prometheus Bash client library.
# This is broadly based on the Go client library, but simplified.
# All metrics are automatically registered at creation time.
# Metric options don't have Namespace or Subsystem because shell scripts
# can do the concatenation more clearly themselves. They don't have
# ConstLabels because those are hard to construct while quoting everything
# safely. Because all the metrics must have a distinct (fully-qualified) name,
# their structures are all named after that name.

# Notably missing from this script:
# + User documentation
# + Counter metrics
# + Summary metrics
# + A fallback if no "curl" is available (maybe /dev/tcp)
# + Global constant labels (needed if library code is going to export any
#   vars without getting information from its client).

# All variables defined in this file have names matching "io_prometheus_*".
# Similarly, all functions have names matching "io::prometheus::*".

# Example use:
# io::prometheus::NewGauge name=start_time help='time_t when cron job last started'
# start_time set $(date +'%s.%N')
# io::prometheus::PushAdd cronjob $HOSTNAME pushgateway0:9091

# An example with labels (which doesn't work yet):
# io::prometheus::NewGauge name=start_time help='time_t when cron job last started' \
#   labels=host,runmode
# start_time -host=spof0 -runmode=PRODUCTION set $(date +'%s.%N')
# io::prometheus::PushAdd cronjob $HOSTNAME pushgateway0:9091

# Note to self: metric names match ^[a-zA-Z_:][a-zA-Z0-9_:]*$ and
# label names match ^[a-zA-Z_][a-zA-Z0-9_]*$ according to
# http://prometheus.io/docs/concepts/data_model/

# A list of all registered collectors (i.e. a list of function names).
declare -a io_prometheus_registered_collectors

# An associative array of values, used by the collectors defined in this file.
# Each key is a metric name with its labels, in the client data exposition
# format (i.e. \n, \" and \\ are escape sequences. Each value is the
# corresponding metric value (a float in an unspecified format).
# Example:
# io_prometheus_value['start_time{host="spof0",runmode="PRODUCTION"}']=1.42342655e+09
declare -A io_prometheus_value

# An associative array of help strings, used by the collectors defined in this
# file. Each key is a metric name without labels. Each value is the
# corresponding metric help string in the client data exposition format (i.e.
# \n and \\ are escape sequences).
# Example:
# io_prometheus_help[start_time]='time_t when cron job last started'
declare -A io_prometheus_help

# An associative array of metric types, used by the collectors defined in this
# file. Each key is a metric name without labels. Each value is the
# corresponding type as used following the TYPE keyword in the client data
# exposition format.
# Example:
# io_prometheus_type[start_time]=gauge
declare -A io_prometheus_type

# An associative array of metric label sets associated with a metric name.
# The order of the labels in this list is the same as their order in the
# io_prometheus_value keys for the same metric name. This list is comma-
# separated. Each value for a metric must have exactly the same label names
# as in this list. This list includes all constant labels declared for
# the exporter and the metric (but not "job" or "instance").
# Example:
# io_prometheus_labelnames[start_time]=host,runmode
declare -A io_prometheus_labelnames

# Clear all the data saved in io_prometheus_* variables.
# This is used by the unit tests; I can't think when else you'd use it.
io::prometheus::DiscardAllMetrics() {
  local key
  unset io_prometheus_registered_collectors
  declare -a -g io_prometheus_registered_collectors
  unset io_prometheus_value
  declare -A -g io_prometheus_value
  unset io_prometheus_help
  declare -A -g io_prometheus_help
  unset io_prometheus_type
  declare -A -g io_prometheus_type
  unset io_prometheus_labelnames
  declare -A -g io_prometheus_labelnames
}

# This function outputs text compatible with version 0.0.4 of the
# Prometheus client data exposition format as described at:
# https://docs.google.com/document/d/1ZjyKiKxZV83VI9ZKAXRGKaUKK2BIWCT7oiGBKDBpjEY/view
# It works by polling all of the registered collectors.
# Args: none
# Output: the text of all collectors (possibly corrupted unless status 0)
# Return status: 0 if and only if every collector's collect method returns 0.
io::prometheus::ExportAsText() {
  local collector retval=0
  for collector in "${io_prometheus_registered_collectors[@]}"; do
    ${collector} collect || { retval=$?; }
  done
  return ${retval}
}

io::prometheus::ExportToFile() {
  local filename="$1"
  local tmpfilename="$1.tmp.$$"
  io::prometheus::ExportAsText > "${tmpfilename}" \
    && mv -f -- "${tmpfilename}" "${filename}"
}

# Create a new metric of type Gauge.
io::prometheus::NewGauge() {
  local name='' help='' labels=''
  io::prometheus::internal::ParseDdStyleArgs "${FUNCNAME[0]}" \
    'name' 'help' '~labels' -- "$@" || return

  # Check syntax of the metric name and label names (and canonicalize them).
  io::prometheus::internal::CheckValidMetricName "${name}" || return
  local -a labelnames
  local savedIFS="$IFS"
  IFS=','
  labelnames=(${labels})
  labels="${labelnames[*]}"
  IFS="${savedIFS}"
  local label
  for label in "${labelnames[@]}"; do
    io::prometheus::internal::CheckValidLabelName "${label}" || return
  done

  # Warn about duplicate metric names.
  if [[ -n "${io_prometheus_type["${name}"]:-}" ]]; then
    io::prometheus::internal::PrintfError \
      '"%s" is already a registered %s\n' \
      "${name}" "${io_prometheus_type["${name}"]}"
  fi

  # Initialize the new gauge.
  io_prometheus_type["${name}"]=gauge
  local REPLY
  io::prometheus::internal::escape_help_string "${help}"
  io_prometheus_help["${name}"]="$REPLY"
  io_prometheus_labelnames["${name}"]="${labels}"
  if [[ -z "${labels}" ]]; then
    io_prometheus_value["${name}"]=0
  fi
  local dollar_at='"$@"'
  eval "${name}() { io::prometheus::internal::DispatchGauge ${name} ${dollar_at}; }"

  # Register it.
  io_prometheus_registered_collectors+=("${name}")
}

# Push the current values of registered metrics to a Prometheus pushgateway.
# The newly-pushed metrics will replace any previously-pushed metrics with
# the same (job, instance) pair. That is, it uses the PUT method. See PushAdd.
# Args:
#   job=JOBVALUE - provides the mandatory "job" label name/value pair
#   instance=INSTANCEVALUE - provides the optional "instance" label pair
#   gateway=URL - address of the pushgateway
# Output: none (error messages may appear on standard error)
# Return status: 0 if and only if push successful.
io::prometheus::Push() {
  io::prometheus::internal::Push method=PUT "$@"
}

# Push the current values of registered metrics to a Prometheus pushgateway.
# The newly-pushed metrics will replace any previously-pushed metrics with
# the same (<metric name>, job, instance) pair. That is, it uses the POST
# method. See also Push.
# Args:
#   job=JOBVALUE - provides the mandatory "job" label name/value pair
#   instance=INSTANCEVALUE - provides the optional "instance" label pair
#   gateway=HOST:PORT - TCP address of the pushgateway
# Output: none (error messages may appear on standard error)
# Return status: 0 if and only if push successful.
io::prometheus::PushAdd() {
  io::prometheus::internal::Push method=POST "$@"
}

io::prometheus::gauge::add() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Now increase the current value of the named series by the supplied value.
  if [[ $# -ne 1 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s add" called with %s arguments (expected %s)\n' \
      "${metric_name}" $# 1
    return 1
  fi
  io::prometheus::internal::Addition \
    "${io_prometheus_value["${series_name}"]:-0}" "$1" || return
  io_prometheus_value["${series_name}"]="${REPLY}"
  return 0
}

io::prometheus::gauge::collect() {
  local metricname="$1"
  local -i num_label_args="$2"
  shift 2
  if [[ ${num_label_args} -ne 0 || $# -ne 0 ]]; then
    io::prometheus::internal::PrintfError \
      '%s collect called with extra arguments "%s"\n' \
      "${metricname}" "$*"
    return 1
  fi
  printf '# TYPE %s %s\n# HELP %s %s\n' \
    "${metricname}" "${io_prometheus_type["${metricname}"]}" \
    "${metricname}" "${io_prometheus_help["${metricname}"]}" || return
  local key
  for key in "${!io_prometheus_value[@]}"; do
    case "${key}" in
    "${metricname}"|"${metricname}{"*)
      printf '%s %s\n' "${key}" "${io_prometheus_value["${key}"]}" || return
    esac
  done
  return 0
}

io::prometheus::gauge::dec() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Now decrease the current value of the named series by 1.
  if [[ $# -ne 0 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s dec" called with %s arguments (expected %s)\n' \
      "${metric_name}" $# 0
    return 1
  fi
  io::prometheus::internal::Addition \
    "${io_prometheus_value["${series_name}"]:-0}" 1 || return
  io_prometheus_value["${series_name}"]="${REPLY}"
  return 0
}

io::prometheus::gauge::inc() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Now increase the current value of the named series by 1.
  if [[ $# -ne 0 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s inc" called with %s arguments (expected %s)\n' \
      "${metric_name}" $# 0
    return 1
  fi
  io::prometheus::internal::Addition \
    "${io_prometheus_value["${series_name}"]:-0}" 1 || return
  io_prometheus_value["${series_name}"]="${REPLY}"
  return 0
}

io::prometheus::gauge::set() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Now set the current value of the named series to the supplied value.
  if [[ $# -ne 1 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s set" called with %s arguments (expected %s)\n' \
      "${metric_name}" $# 1
    return 1
  fi
  # TODO(aecolley): Check that it's a parseable number assignable to float64.
  io_prometheus_value["${series_name}"]="$1"
  return 0
}

io::prometheus::gauge::setToElapsedTime() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Check that we actually have a command to run.
  if [[ $# -lt 1 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s setToElapsedTime" called with %s arguments (expected %s+)\n' \
      "${metric_name}" $# 1
    return 1
  fi
  local cmd="$1"
  shift

  local before after
  before="$( exec 2>&1; set -o posix; TIMEFORMAT='%3R'; time; )"

  local rc=0
  "${cmd}" "$@"
  rc=$?

  after="$( exec 2>&1; set -o posix; TIMEFORMAT='%3R'; time; )"
  
  local REPLY
  if io::prometheus::internal::Addition "${after}" -"${before}"; then
    if [[ "${REPLY}" =~ ^[0-9] ]]; then
      io_prometheus_value["${series_name}"]="${REPLY}"
    fi
  fi

  return ${rc}
}

io::prometheus::gauge::sub() {
  local metric_name="$1"
  local num_labels="$2"
  shift 2

  # Combine the metric name and the label/value pairs into the series name.
  local REPLY
  io::prometheus::internal::assemble_series_name \
    "${metric_name}" "${num_labels}" "$@" || return
  local series_name="${REPLY}"
  shift ${num_labels}

  # Now decrease the current value of the named series by the supplied value.
  if [[ $# -ne 1 ]]; then
    io::prometheus::internal::PrintfError \
      '"%s sub" called with %s arguments (expected %s)\n' \
      "${metric_name}" $# 1
    return 1
  fi
  case "$1" in
  -*)
    io::prometheus::internal::Addition \
      "${io_prometheus_value["${series_name}"]:-0}" "${1#'-'}" || return
    ;;
  *)
    io::prometheus::internal::Addition \
      "${io_prometheus_value["${series_name}"]:-0}" "-$1" || return
  esac
  io_prometheus_value["${series_name}"]="${REPLY}"
  return 0
}

io::prometheus::internal::Addition() {
  local a="$1"
  local b="$2"
  if [[ "${a}${b}" =~ ^[-+0-9]*$ ]]; then
    # They're both integers; use builtin shell arithmetic.
    REPLY="$(( ${a} + ${b} ))"
  else
    # Too complex for bash, so use awk.
    local newvalue
    newvalue="$(awk -v a="${a}" -v b="${b}" 'BEGIN {print(a + b); exit(0)}')" \
    || {
      io::prometheus::internal::PrintfError \
        'failed to compute (%s + %s) using awk\n' \
        "${a}" "${b}"
      return 1
    }
    REPLY="${newvalue}"
  fi
  return 0
}

io::prometheus::internal::CheckValidLabelName() {
  local name="$1"
  if [[ "${name}" =~ ^[a-zA-Z_][a-zA-Z0-9_]*$ ]]; then
    return 0
  else
    io::prometheus::internal::PrintfError \
     'Malformed label name "%s" /%s/\n' \
      "${name}" '^[a-zA-Z_][a-zA-Z0-9_]*$'
    return 1
  fi
}

io::prometheus::internal::CheckValidMetricName() {
  local name="$1"
  if [[ "${name}" =~ ^[a-zA-Z_:][a-zA-Z0-9_:]*$ ]]; then
    return 0
  else
    io::prometheus::internal::PrintfError \
     'Malformed metric name "%s" /%s/\n' \
      "${name}" '^[a-zA-Z_:][a-zA-Z0-9_:]*$'
    return 1
  fi
}

io::prometheus::internal::DispatchGauge() {
  local metricname="$1"
  shift

  local -a label_args
  local methodname=''
  while [[ $# -gt 0 ]]; do
    case "$1" in
    -*=*)
      label_args+=("$1")
      shift
      ;;
    *)
      methodname="$1"
      shift
      break
    esac
  done

  case "${methodname}" in
  add|collect|dec|inc|set|setToElapsedTime|sub)
    io::prometheus::gauge::${methodname} \
      "${metricname}" "${#label_args[@]}" "${label_args[@]}" "$@"
    ;;
  '')
    io::prometheus::internal::PrintfError 'Called %s without a method name\n' \
      "${metricname}"
    return 1
    ;;
  *)
    io::prometheus::internal::PrintfError 'Gauge %s has no "%s" method\n' \
      "${metricname}" "${methodname}"
    return 1
  esac
}

# For debugging.
io::prometheus::internal::DumpInternalState() {
  local key
  printf 'io::prometheus::internal::DumpInternalState => {\n'
  printf '  io_prometheus_registered_collectors=%s\n' \
    "${io_prometheus_registered_collectors[*]}"
  printf '  io_prometheus_value={\n'
  for key in "${!io_prometheus_value[@]}"; do
    printf '    [%s]=%s\n' "${key}" "${io_prometheus_value[${key}]}"
  done
  printf '  }\n'
  printf '  io_prometheus_help={\n'
  for key in "${!io_prometheus_help[@]}"; do
    printf '    [%s]=%s\n' "${key}" "${io_prometheus_help[${key}]}"
  done
  printf '  }\n'
  printf '  io_prometheus_type={\n'
  for key in "${!io_prometheus_type[@]}"; do
    printf '    [%s]=%s\n' "${key}" "${io_prometheus_type[${key}]}"
  done
  printf '  }\n'
  printf '  io_prometheus_labelnames={\n'
  for key in "${!io_prometheus_labelnames[@]}"; do
    printf '    [%s]=%s\n' "${key}" "${io_prometheus_labelnames[${key}]}"
  done
  printf '  }\n'
  printf '}\n'
}

io::prometheus::internal::DuplicateArg() {
  local funcname=$1
  local param=$2
  io::prometheus::internal::PrintfError \
    'Duplicate %s arg: %s\n' \
    "${funcname}" "${param}"
  return 1
}

io::prometheus::internal::MissingArg() {
  local funcname=$1
  local param=$2
  io::prometheus::internal::PrintfError \
    'Missing %s arg: %s\n' \
    "${funcname}" "${param}"
  return 1
}

# Example: io::prometheus::internal::ParseDdStyleArgs "${FUNCNAME[0]}" foo ~bar -- "$@"
# The 0 is the number of stack frames to skip when generating error messages.
# The ~ denotes an optional argument; all others are mandatory.
# Returns 0 if parse successful and params assigned; 1 otherwise.
io::prometheus::internal::ParseDdStyleArgs() {
  # All our local variables begin with parser_ to avoid accidental capture.
  local parser_funcname="$1"; shift
  local -A parser_params
  # parser_params' keys are parameter names; the values are:
  # mandatory - mandatory parameter not seen yet
  # optional - optional parameter not seen yet
  # already-seen - parameter seen already
  # Collect the params.
  while [[ $# -gt 0 ]]; do
    if [[ "$1" = "--" ]]; then
      shift; break
    elif [[ "$1" =~ ^[~] ]]; then
      parser_params["${1#'~'}"]=optional
    else
      parser_params["$1"]=mandatory
    fi
    shift
  done
  # Process the arguments and assign them.
  local parser_arg parser_val
  while [[ $# -gt 0 ]]; do
    if [[ "$1" =~ = ]]; then
      parser_arg="${1%%'='*}"
      parser_val="${1#*'='}"
      case "${parser_params["${parser_arg}"]:-unset}" in
      mandatory|optional)
        eval "${parser_arg}=\"\${parser_val}\""
        parser_params["${parser_arg}"]=already-seen
        ;;
      already-seen)
        io::prometheus::internal::DuplicateArg "${parser_funcname}" "${parser_arg}"
        return 1
        ;;
      *)
        io::prometheus::internal::UnrecognizedArg "${parser_funcname}" "${parser_arg}"
        return 1
      esac
    else
      io::prometheus::internal::UnrecognizedArg "${parser_funcname}" "$1"
      return 1
    fi
    shift
  done
  # Complain about any mandatory arguments which weren't specified.
  for parser_arg in "${!parser_params[@]}"; do
    if [[ "${parser_params["${parser_arg}"]}" = "mandatory" ]]; then
      io::prometheus::internal::MissingArg "${parser_funcname}" "${parser_arg}"
      return 1
    fi
  done
  return 0
}

# Like printf 1>&2 except it prefixes the format with "ERROR: [file:line] "
# where "file" and "line" are the address of the line that called into this
# file of functions in the first place.
io::prometheus::internal::PrintfError() {
  local format="$1"
  shift

  # Find the index in BASH_SOURCE of the innermost caller not in this file.
  local i=1
  while [[ $i -lt ${#BASH_SOURCE[@]} ]]; do
    if [[ "${BASH_SOURCE[$i]}" != "${BASH_SOURCE[0]}" ]]; then
      break
    fi
    i=$(( $i + 1 ))
  done
  if [[ $i -ge ${#BASH_SOURCE[@]} ]]; then
    i=1  # Didn't find one, here's a not-too-insane fallback value.
  fi

  local sourcefile="${BASH_SOURCE[${i}]}"
  i=$(( $i - 1 ))
  local sourceline="${BASH_LINENO[${i}]}"
  printf 1>&2 "ERROR: [%s:%s] ${format}" "${sourcefile}" "${sourceline}" "$@"
}

io::prometheus::internal::Push() {
  local method='' job='' instance='' gateway=''
  io::prometheus::internal::ParseDdStyleArgs "${FUNCNAME[1]}" \
    'method' 'job' '~instance' 'gateway' -- "$@" || return

  # Construct the URL to push to.
  local url
  case "${gateway}" in
  :*)  url="http://localhost${gateway}/metrics/job/${job}";;
  *:*) url="http://${gateway}/metrics/job/${job}";;
  *)   url="http://${gateway}:9091/metrics/job/${job}"
  esac
  if [[ -n "${instance}" ]]; then
    url="${url}/instance/${instance}"
  fi
  # Compose and transmit the metrics.
  io::prometheus::ExportAsText | curl -q \
    --request "${method}" \
    --data-binary '@-' \
    --user-agent 'Prometheus-client_bash/prerelease' \
    --header 'Content-Type: text/plain; version=0.0.4' \
    --fail \
    --silent \
    --connect-timeout 5 \
    --max-time 10 \
    "${url}" > /dev/null

  [[ "${PIPESTATUS[0]}" -eq 0 && "${PIPESTATUS[1]}" -eq 0 ]]
}

io::prometheus::internal::UnrecognizedArg() {
  local funcname=$1
  local arg=$2
  io::prometheus::internal::PrintfError \
    'Unrecognized %s arg: %s\n' \
    "${funcname}" "${arg}"
  return 1
}

# Sets the variable REPLY to the variable's key in io_prometheus_value.
# Argument 1 is the name of the metric.
# Argument 2 is the number of flag arguments (num_flags).
# Arguments 3..(3+num_flags-1) are the flag arguments
# Arguments (3+num_flags)..($#) are ignored.
# If an error occurs, REPLY is set to the error message and this returns 1.
#
# Example:
# Given:
#   io_prometheus_labelnames[horses]=name,number
# the call:
#   io::prometheus::internal::assemble_series_name \
#     'horses' 2 -number=1 -name="Zeinab Badawi's Twenty Hotels" to look for
# will set REPLY to the string:
#   horses{name="Zeinab Badawi's Twenty Hotels",number="1"}
io::prometheus::internal::assemble_series_name() {
  local metric_name="$1"
  local num_flags="$2"
  shift 2

  # Put the first $num_flags arguments into an associative array.
  local -A labelmap
  local key_equals_value key value
  local num_labels_shifted=0
  while [[ ${num_labels_shifted} -lt ${num_flags} ]]; do
    key_equals_value="${1#'-'}"
    shift
    num_labels_shifted=$((num_labels_shifted + 1))
    key="${key_equals_value%%'='*}"
    value="${key_equals_value#*'='}"
    if [[ -n "${labelmap["${key}"]+set}" ]]; then
      REPLY="Label ${key} is assigned twice"
      return 1
    fi
    labelmap["${key}"]="${value}"
  done

  # Now take them out again in canonical order, building up series_name.
  local series_name='' num_labels_encoded=0
  local csv="${io_prometheus_labelnames["${metric_name}"]-'???'}"
  if [[ "${csv}" = '???' ]]; then
    REPLY="No such metric: ${metric_name}"
    return 1
  fi
  while [[ -n "${csv}" ]]; do
    # Shift the first comma-separated item off the list.
    case "${csv}" in
    *','*)
      key="${csv%%','*}"
      csv="${csv#*','}"
      ;;
    *)
      key="${csv}"
      csv=''
    esac
    if [[ -z "${labelmap["${key}"]+set}" ]]; then
      REPLY="No value supplied for ${metric_name} label ${key}"
      return 1
    fi
    value="${labelmap["${key}"]}"
    io::prometheus::internal::escape_label_value "${value}"  # sets REPLY
    series_name="${series_name}${series_name:+,}${key}=\"${REPLY}\""
    num_labels_encoded=$(( $num_labels_encoded + 1 ))
  done
  if [[ "${num_labels_encoded}" != "${num_labels_shifted}" ]]; then
    REPLY="Incorrect number of labels for ${metric_name} (expected ${num_labels_encoded}, actual ${num_labels_shifted})"
    return 1
  fi

  # Assemble the whole series name.
  if [[ -n "${series_name}" ]]; then
    REPLY="${metric_name}{${series_name}}"
  else
    REPLY="${metric_name}"
  fi
  return 0
}

# Assigns to REPLY the value of $1, with escaping as follows:
# each newline is replaced with '\n';
# each backslash is replaced with two backslashes.
io::prometheus::internal::escape_help_string() {
  local input="$1"
  local left backslash='\' newline='
'
  REPLY=''
  while true; do
    local previnput="${input}"
    case "${input}" in
    "${newline}"*)
      REPLY="${REPLY}${backslash}"n
      input="${input#"${newline}"}"
      ;;
    "${backslash}"*)
      REPLY="${REPLY}${backslash}${backslash}"
      input="${input#"${backslash}"}"
      ;;
    *["${newline}${backslash}"]*)
      left="${input%%["${newline}${backslash}"]*}"
      REPLY="${REPLY}${left}"
      input="${input#"${left}"}"
      ;;
    *)
      REPLY="${REPLY}${input}"
      return 0
    esac
    if [[ "${previnput}" = "${input}" ]]; then
      printf 1>&2 'escape_help_string failed to reduce "%s"\n' "${input}"
      exit 1
    fi
  done
}

# Assigns to REPLY the value of $1, with escaping as follows:
# each newline is replaced with '\n';
# each double-quote is replaced with a backslash-double-quote pair; and
# each backslash is replaced with two backslashes.
io::prometheus::internal::escape_label_value() {
  local input="$1"
  local left backslash='\' doublequote='"' newline='
'
  REPLY=''
  while true; do
    case "${input}" in
    "${newline}"*)
      REPLY="${REPLY}${backslash}"n
      input="${input#"${newline}"}"
      ;;
    "${doublequote}"*)
      REPLY="${REPLY}${backslash}${doublequote}"
      input="${input#"${doublequote}"}"
      ;;
    "${backslash}"*)
      REPLY="${REPLY}${backslash}${backslash}"
      input="${input#"${backslash}"}"
      ;;
    *["${newline}${doublequote}${backslash}"]*)
      left="${input%%["${newline}${doublequote}${backslash}"]*}"
      REPLY="${REPLY}${left}"
      input="${input#"${left}"}"
      ;;
    *)
      REPLY="${REPLY}${input}"
      return 0
    esac
  done
}


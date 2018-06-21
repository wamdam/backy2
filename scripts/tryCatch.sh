#!/usr/bin/env bash
# Based on https://github.com/niieani/bash-oo-framework/blob/master/lib/util/tryCatch.sh
#      and https://github.com/niieani/bash-oo-framework/blob/master/lib/util/exception.sh
#
# Copyright (c) 2015 Bazyli Brzóska @ https://invent.life/
# Modified work: 2018 Lars Fenneberg
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

# From: http://wiki.bash-hackers.org/scripting/debuggingtips
export PS4='+(${BASH_SOURCE##*/}:${LINENO}): ${FUNCNAME[0]:+${FUNCNAME[0]}(): }'
set -o pipefail
shopt -s expand_aliases

declare -ig __oo__insideTryCatch=0
declare -g __oo__presetShellOpts="$-"

# in case try-catch is nested, we set +e before so the parent handler doesn't catch us instead
alias try='__EXCEPTION__=(); [[ $__oo__insideTryCatch -eq 0 ]] || set +e; __oo__presetShellOpts="$-"; __oo__insideTryCatch+=1; ( set -e; true; '
alias catch='); declare __oo__tryResult=$?; __oo__insideTryCatch+=-1; [[ $__oo__insideTryCatch -lt 1 ]] || set -${__oo__presetShellOpts:-e} && Exception::Extract $__oo__tryResult || '
alias onsuccess='; [[ ${#__EXCEPTION__} -gt 0 ]] || '

Exception::Extract() {
  local retVal=$1
  unset __oo__tryResult

  if [[ $retVal -gt 0 ]]
  then
    __EXCEPTION__=("${BASH_SOURCE[1]#./}" "${BASH_LINENO[0]}")
    return 1 # so that we may continue with a "catch"
  fi
  return 0
}

Exception::DumpBacktrace() {
  local -i startFrom="${1:-1}"
  # inspired by: http://stackoverflow.com/questions/64786/error-handling-in-bash

  # USE DEFAULT IFS IN CASE IT WAS CHANGED
  local IFS=$' \t\n'

  local -i i=0

  while caller $i > /dev/null
  do
    if (( i + 1 >= startFrom ))
    then
      local -a trace=( $(caller $i) )

      echo " Backtrace: ${trace[*]:2}:${trace[0]}, function ${trace[1]}."
    fi
    i+=1
  done
}

Exception::Print() {
    echo "Exception occured at ${__EXCEPTION__[0]}:${__EXCEPTION__[1]}."
    Exception::DumpBacktrace 2
}

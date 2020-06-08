#/usr/bin/env bash

_backy2_versions() {
    suggestions=($(compgen -W "$(backy2 -ms ls -f uid)" -- "${COMP_WORDS[2]}"))
}

_backy2_names() {
    suggestions=($(compgen -W "$(backy2 -ms ls -f name|sort|uniq)" -- "${COMP_WORDS[2]}"))
}

_backy2_commands() {
    suggestions=($(compgen -W "$(backy2|grep -oP -e "{.*}"|sed 's/[{}]//g'|sed 's/,/ /g')" -- "${COMP_WORDS[1]}"))
}

_backy2_completions() {
    if [[ "${COMP_WORDS[1]}" == "ls" ]]; then
        _backy2_names
    elif [[ "${COMP_WORDS[1]}" == "restore" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "protect" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "unprotect" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "rm" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "scrub" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "export" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "du" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "migrate-encryption" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "add-tag" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "remove-tag" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "expire" ]]; then
        _backy2_versions
    elif [[ "${COMP_WORDS[1]}" == "due" ]]; then
        _backy2_names
    elif [[ "${COMP_WORDS[1]}" == "sla" ]]; then
        _backy2_names
    else
        _backy2_commands
    fi

    #local suggestions=($(compgen -W "$(backy2 -ms ls -f uid)" -- "${COMP_WORDS[2]}"))
    COMPREPLY=("${suggestions[@]}")
}
complete -F _backy2_completions backy2

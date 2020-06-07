#/usr/bin/env bash

_backy2_versions() {
    suggestions=($(compgen -W "$(backy2 -ms ls -f uid)" -- "${COMP_WORDS[2]}"))
}

_backy2_names() {
    suggestions=($(compgen -W "$(backy2 -ms ls -f name|sort|uniq)" -- "${COMP_WORDS[2]}"))
}

_backy2_completions() {
    if [[ "${COMP_WORDS[1]}" == "ls" ]]; then
        _backy2_names
    elif [[ "${COMP_WORDS[1]}" == "restore" ]]; then
        _backy2_versions
    fi

    #local suggestions=($(compgen -W "$(backy2 -ms ls -f uid)" -- "${COMP_WORDS[2]}"))
    COMPREPLY=("${suggestions[@]}")
}
complete -F _backy2_completions backy2

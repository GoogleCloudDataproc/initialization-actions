#!/usr/bin/env bash

function_exists () {
    declare -f -F $1 > /dev/null
    return $?
}

throw () {
    echo "$*" >&2
    echo
    function_exists usage && usage
    exit 1
}


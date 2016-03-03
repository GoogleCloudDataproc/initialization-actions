#!/usr/bin/env bash

trim_ws() {
    local var="$*"
    var="${var#"${var%%[![:space:]]*}"}"   # remove leading whitespace characters
    var="${var%"${var##*[![:space:]]}"}"   # remove trailing whitespace characters
    echo -n "$var"
}

trim_qt() {
    local var="$*"
    var="${var#"${var%%[!\'\"]*}"}"   # remove leading whitespace characters
    var="${var%"${var##*[!\'\"]}"}"   # remove trailing whitespace characters
    echo -n "$var"
}


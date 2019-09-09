#!/usr/bin/env bash

args=""

if { [ -z "$QUERY" ] && [ ! -z "$Q" ]; } || { [ ! -z "$QUERY" ] && [ -z "$Q" ]; }; then
    args="$args|--query|$QUERY$Q"
fi

if { [ -z "$COMPRESSION" ] && [ ! -z "$C" ]; } || { [ ! -z "$COMPRESSION" ] && [ -z "$C" ]; }; then
    args="$args|--compression|$COMPRESSION$C"
fi

if { [ -z "$FORMAT" ] && [ ! -z "$F" ]; } || { [ ! -z "$FORMAT" ] && [ -z "$F" ]; }; then
    args="$args|--format|$FORMAT$F"
fi

if { [ -z "$SOURCE" ] && [ ! -z "$S" ]; } || { [ ! -z "$SOURCE" ] && [ -z "$S" ]; }; then
    args="$args|--source|$SOURCE$S"
fi

if { [ -z "$DEST" ] && [ ! -z "$D" ]; } || { [ ! -z "$DEST" ] && [ -z "$D" ]; }; then
    args="$args|--destination|$DEST$D"
fi

mvn scala:run -Dlauncher="downloadconverter" -DaddArgs="${args#?}"

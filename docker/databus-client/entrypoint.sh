#!/usr/bin/env bash

DEST="/data/dumps"

args="--destination|$DEST"

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

mvn scala:run -Dlauncher="downloadconverter" -DaddArgs="$args"

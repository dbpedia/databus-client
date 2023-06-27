#!/usr/bin/env bash

TARGET="/var/repo"

args="--target $TARGET"

if { [ -z "$SOURCE" ] && [ ! -z "$S" ]; } || { [ ! -z "$SOURCE" ] && [ -z "$S" ]; }; then
    args="$args --source $SOURCE$S"
fi

if { [ -z "$ENDPOINT" ] && [ ! -z "$E" ]; } || { [ ! -z "$ENDPOINT" ] && [ -z "$E" ]; }; then
    args="$args --endpoint $ENDPOINT$E"
fi

if { [ -z "$COMPRESSION" ] && [ ! -z "$C" ]; } || { [ ! -z "$COMPRESSION" ] && [ -z "$C" ]; }; then
    args="$args --compression $COMPRESSION$C"
fi

if { [ -z "$FORMAT" ] && [ ! -z "$F" ]; } || { [ ! -z "$FORMAT" ] && [ -z "$F" ]; }; then
    args="$args --format $FORMAT$F"
fi

echo $args

java -jar databus-client.jar $args

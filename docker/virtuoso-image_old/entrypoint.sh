#!/usr/bin/env bash

TARGET="/data/dumps"
LOAD="/data/toLoad"

mkdir -p $TARGET && mkdir -p $LOAD

args="--target|$TARGET|--compression|gz"

if { [ -z "$SOURCE" ] && [ -z "$S" ]; }; then exit 1; fi

if { [ -z "$SOURCE" ] && [ ! -z "$S" ]; } || { [ ! -z "$SOURCE" ] && [ -z "$S" ]; }; then
    args="$args|--source|$SOURCE$S"
fi

if { [ -z "$FORMAT" ] && [ ! -z "$F" ]; } || { [ ! -z "$FORMAT" ] && [ -z "$F" ]; }; then
    args="$args|--format|$FORMAT$F"
fi

mvn -q scala:run -Dlauncher="databusclient" -DaddArgs="$args"

mv -t $LOAD $(find $TARGET -name "*.gz")

bash /virtuoso.sh

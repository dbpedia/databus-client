#!/usr/bin/env bash

DEST="/data/dumps"
LOAD="/data/toLoad"

mkdir -p $DEST && mkdir -p $LOAD

args="--target|$DEST|--compression|gz"

if { [ -z "$SOURCE" ] && [ -z "$S" ]; }; then exit 1; fi

if { [ -z "$SOURCE" ] && [ ! -z "$S" ]; } || { [ ! -z "$SOURCE" ] && [ -z "$S" ]; }; then
    args="$args|--source|$SOURCE$S"
fi

mvn -q scala:run -Dlauncher="databusclient" -DaddArgs="$args"

mv -t $LOAD $(find $DEST -name "*.gz")

bash /virtuoso.sh

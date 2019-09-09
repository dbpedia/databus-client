#!/usr/bin/env bash

DEST="/data/dumps"
LOAD="/data/toLoad"

mkdir -p $DEST && mkdir -p $LOAD

if [ -z "$QUERY" ]; then exit 1; fi

args="--query|$QUERY|-d|$DEST|--compression|gz"

mvn -q scala:run -Dlauncher="downloadconverter" -DaddArgs="${args#?}"

mv -t $LOAD $(find $DEST -name "*.gz")

bash /virtuoso.sh
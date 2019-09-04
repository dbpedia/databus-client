#!/bin/sh

ENV_VARIABLES=""

if { [ -z "$QUERY" ] && [ ! -z "$Q" ]; } || { [ ! -z "$QUERY" ] && [ -z "$Q" ]; };
then
    NEW="--query|$QUERY$Q|"
    ENV_VARIABLES="$ENV_VARIABLES$NEW"
fi

if { [ -z "$COMPRESSION" ] && [ ! -z "$C" ]; } || { [ ! -z "$COMPRESSION" ] && [ -z "$C" ]; };
then
    NEW="--compression|$COMPRESSION$C|"
    ENV_VARIABLES="$ENV_VARIABLES$NEW"
fi

if { [ -z "$FORMAT" ] && [ ! -z "$F" ]; } || { [ ! -z "$FORMAT" ] && [ -z "$F" ]; };
then
    NEW="--format|$FORMAT$F|"
    ENV_VARIABLES="$ENV_VARIABLES$NEW"
fi

if { [ -z "$SOURCE" ] && [ ! -z "$S" ]; } || { [ ! -z "$SOURCE" ] && [ -z "$S" ]; };
then
    NEW="--source|$SOURCE$S|"
    ENV_VARIABLES="$ENV_VARIABLES$NEW"
fi

DEST="files/"


if { [ -z "$DESTINATION" ] && [ ! -z "$D" ]; } || { [ ! -z "$DESTINATION" ] && [ -z "$D" ]; };
then
    DEST="--destination|$DESTINATION$D|"
    ENV_VARIABLES="$ENV_VARIABLES$DEST"
fi

mkdir /root/databus-client/$DEST
    

mvn scala:run -Dlauncher=$LAUNCHER$L -DaddArgs=$ENV_VARIABLES


mkdir /data/toLoad
cd /root/databus-client/$DEST
#find . -mindepth 2 -type f -print -exec mv --backup=numbered {} /data/toLoad \;

bash /virtuoso.sh

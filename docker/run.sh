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

TARGET="files/"
if { [ -z "$TARGETREPO" ] && [ ! -z "$T" ]; } || { [ ! -z "$TARGETREPO" ] && [ -z "$T" ]; };
then
    TARGET="--targetrepo|$TARGETREPO$T|"
    ENV_VARIABLES="$ENV_VARIABLES$TARGET"
fi
    
echo $ENV_VARIABLES  
ls -A
echo "mvn scala:run -Dlauncher=$LAUNCHER$L -DaddArgs=$ENV_VARIABLES"
mvn scala:run -Dlauncher=$LAUNCHER$L -DaddArgs=$ENV_VARIABLES

mkdir /data/toLoad
cd /root/databus-client/$TARGET
find . -mindepth 2 -type f -print -exec mv --backup=numbered {} /data/toLoad \;

bash /virtuoso.sh

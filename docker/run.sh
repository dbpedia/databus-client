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

if { [ -z "$TARGETREPO" ] && [ ! -z "$T" ]; } || { [ ! -z "$TARGETREPO" ] && [ -z "$T" ]; };
then
    NEW="--targetrepo|$TARGETREPO$T|"
    ENV_VARIABLES="$ENV_VARIABLES$NEW"
fi
    
echo $ENV_VARIABLES  
cd ..
mvn scala:run -Dlauncher=execute -DaddArgs=$ENV_VARIABLES

mkdir /data/toLoad
cd /root/databus-client/converted_files/
find . -mindepth 2 -type f -print -exec mv --backup=numbered {} /data/toLoad \;

bash /virtuoso.sh

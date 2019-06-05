#!/bin/sh

#mvn scala:run -Dlauncher=execute -DaddArgs="-q|$*|--outCompression|gz" /root/dbpediaclient/src/query/query2
mvn scala:run -Dlauncher=execute -DaddArgs="-q|$QUERY|--compression|$COMPRESSION|--format|$FORMAT|--repo|$REPO"

mkdir /data/toLoad
mv -t /data/toLoad $(find . -name "*.ttl.gz") 

bash /virtuoso.sh

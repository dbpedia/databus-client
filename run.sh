#!/bin/sh

#mvn scala:run -Dlauncher=execute -DaddArgs="-q|$*|--outCompression|gz"
mvn scala:run -Dlauncher=execute -DaddArgs="-q|/root/dbpediaclient/src/query/query|--compression|gz"

mkdir /data/toLoad
mv -t /data/toLoad $(find . -name "*.ttl.gz") 

bash /virtuoso.sh

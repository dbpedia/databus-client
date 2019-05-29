#!/bin/sh

#mvn scala:run -Dlauncher=execute -DaddArgs="-q|$*|--outCompression|gz"
mvn scala:run -Dlauncher=execute -DaddArgs="-q|/root/dbpediaclient/src/query/query|--outCompression|gz"

mv -t /toLoad $(find . -name "*.ttl.gz") 

/virtuoso.sh

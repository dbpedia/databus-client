# Docker

You can pass all the variables as Environment Variables (**-e**), that are shown in [CLI options](../usage/quickstart.md) (except `target`), but you have to write the Environment Variables in Capital Letters.

## Example
```
docker run --name databus-client \
    -v $(pwd)/yourQuery.sparql:/opt/databus-client/query.sparql \
    -v $(pwd)/repo:/var/repo \
    -e SOURCE="/opt/databus-client/query.sparql" \
    -e FORMAT="ttl" \
    -e COMPRESSION="bz2" \
    databus-client

docker rm databus-client```
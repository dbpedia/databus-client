# Docker Images

## Dockerized Databus-Client

build

```
git clone https://github.com/dbpedia/databus-client.git
cd docker
docker build -t databus-client -f databus-client/Dockerfile ./ 
```

run 

```
docker run --name databus-client -v /home/marvin/workspace/dbpedia/databus-client/docker/clienttest:/var/repo -e QUERY="/opt/databus-client/src/query/query" databus-client
```

## Preloaded Virtuoso

### with docker-compose

### as plain Dockerfile





Run a docker container.

```
docker run -p 8890:8890 --name client -e LAUNCHER=downloadconverter -e QUERY=/root/dbpediaclient/src/query/query dbpedia-client
```

You have to specify the launcher you want the container to execute. Therefore you need to pass the name of the launcher as environment variable (`LAUNCHER` or `L`)
```
-e LAUNCHER=downloader
```

Additionally you can pass all the variables that are shown in the list above as Environment Variables (**-e**).  
You have to write the Environment Variables in Capital Letters, if you use docker to execute.  

```
docker run -p 8890:8890 --name client -e L=downloadconverter -e Q=<path> -e F=nt -e C=gz dbpedia-client
```

To stop the image *client* in the container *dbpedia-client* use `docker stop client`

> **Important:** If you use docker to execute, you can't change the "_TARGETREPO_" yet.


## EXAMPLES

```
docker build -t databus-client -f docker/databus-client.dockerfile ./
```

```
docker run --name dbtcl -v /home/marvin/workspace/dbpedia/databus-client/testrepo:/var/repo -e QUERY="/opt/databus-client/src/query/query" databus-client
```

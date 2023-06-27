FROM openjdk:11

ENV SOURCE /databus-client/query.sparql

RUN mkdir "/databus-client"
WORKDIR /databus-client

#ARG JAR_DOWNLOAD_URI=$(curl -s https://api.github.com/repos/dbpedia/databus-client/releases/latest | grep browser_download_url | cut -d '"' -f 4)
COPY  /home/runner/.m2/repository/org/dbpedia/databus-client/v2.1-beta/databus-client-v2.1-beta.jar /databus-client/databus-client.jar
#RUN export jarDownloadUri=$(curl -s https://api.github.com/repos/dbpedia/databus-client/releases/latest | grep browser_download_url | cut -d '"' -f 4); wget -O databus-client.jar $jarDownloadUri;
#RUN curl -s https://api.github.com/repos/dbpedia/databus-client/releases/latest | grep browser_download_url | cut -d '"' -f 4 | xargs -I{} curl -L {} > /databus-client/databus-client.jar

COPY ./entrypoint.sh ./

RUN mkdir -p /var/repo && mkdir -p /var/toLoad
#ENTRYPOINT ["/bin/bash"]
ENTRYPOINT ["/bin/bash","/databus-client/entrypoint.sh"]

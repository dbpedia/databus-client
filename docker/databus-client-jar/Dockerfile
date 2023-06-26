FROM openjdk:11

ENV SOURCE /opt/databus-client/query.sparql

RUN mkdir "/opt/databus-client"
WORKDIR /opt/databus-client

#RUN
#ARG JAR_DOWNLOAD_URI=$(curl -s https://api.github.com/repos/dbpedia/databus-client/releases/latest | grep browser_download_url | cut -d '"' -f 4)

RUN export jarDownloadUri=$(curl -s https://api.github.com/repos/dbpedia/databus-client/releases/latest | grep browser_download_url | cut -d '"' -f 4); wget -O databus-client.jar $jarDownloadUri;

COPY ./entrypoint.sh /opt/databus-client/

WORKDIR /opt/databus-client

#RUN git checkout a3772ef1e3bd835ac503f38807b94b7fa005028a  \
# && mvn install
#RUN mvn install

RUN mkdir -p /var/repo && mkdir -p /var/toLoad

ENTRYPOINT ["/bin/bash","/opt/databus-client/entrypoint.sh"]

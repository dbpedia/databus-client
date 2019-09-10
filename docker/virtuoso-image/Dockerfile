FROM tenforce/virtuoso

RUN apt-get update \
    && apt-get -y install git openjdk-8-jdk maven

RUN echo 2 | update-alternatives --config java

RUN cd /opt \
    && git clone https://github.com/dbpedia/databus-client.git

WORKDIR /opt/databus-client

RUN git checkout a3772ef1e3bd835ac503f38807b94b7fa005028a  \
    && mvn install

#RUN mvn install

COPY ./entrypoint.sh /opt/databus-client/entrypoint.sh

ENTRYPOINT ["/bin/bash","/opt/databus-client/entrypoint.sh"]

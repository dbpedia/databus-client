FROM maven:3.6.1-jdk-8

ENV SOURCE /opt/databus-client/query.query

RUN cd /opt \
    && git clone https://github.com/dbpedia/databus-client.git

COPY ./entrypoint.sh /opt/databus-client/

WORKDIR /opt/databus-client

#RUN git checkout a3772ef1e3bd835ac503f38807b94b7fa005028a  \
# && mvn install
RUN mvn install

RUN mkdir -p /var/repo && mkdir -p /var/toLoad

ENTRYPOINT ["/bin/bash","/opt/databus-client/entrypoint.sh"]

FROM tenforce/virtuoso

RUN apt-get update

RUN apt-get -y install git openjdk-8-jdk  maven

WORKDIR /root/

#RUN git clone https://github.com/dbpedia/databus-client.git
COPY . ./databus-client
#CMD git pull

RUN echo 2 | update-alternatives --config java

RUN cd databus-client && mvn install

WORKDIR /root/databus-client

RUN ls


ENTRYPOINT ["/bin/bash","/root/databus-client/docker/run.sh"] 


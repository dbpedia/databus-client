FROM tenforce/virtuoso

RUN apt-get update

RUN apt-get -y install git openjdk-8-jdk  maven

WORKDIR /root/

RUN git clone https://git.informatik.uni-leipzig.de/fg60hyfy/dbpediaclient.git
#COPY . ./dbpediaclient
#CMD git pull

RUN echo 2 | update-alternatives --config java

RUN cd dbpediaclient && mvn install

WORKDIR /root/dbpediaclient

RUN ls


ENTRYPOINT ["/bin/bash","/root/dbpediaclient/run.sh"] 


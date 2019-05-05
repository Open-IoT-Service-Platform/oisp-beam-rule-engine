# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#

#Build beam application and embedd in Spark container
FROM maven:3.6.1-jdk-8-alpine

RUN apk update && apk add build-base

ADD pom.xml /app/pom.xml
RUN mkdir /app/checkstyle
ADD checkstyle/checkstyle.xml /app/checkstyle/checkstyle.xml
ADD src /app/src

WORKDIR /app

RUN mvn checkstyle:check pmd:check clean package -Pflink-runner  -DskipTests

FROM flink:1.5.0-scala_2.11-alpine
EXPOSE 6123 8081



RUN mkdir -p /app/target
COPY --from=0 /app/target/rule-engine-bundled-0.1.jar /app/target

RUN apk update
RUN apk add python py-pip wget bash openjdk8-jre libc6-compat gcompat
RUN pip install poster
RUN pip install requests
RUN pip install kafka-python
ADD deployer /app/deployer
ADD bootstrap.sh /app
ADD local-deploy.sh /app
ADD wait-for-it.sh /app

RUN chmod +x /app/bootstrap.sh
RUN (cd /app/deployer; pip install -r requirements.txt)

WORKDIR /app

CMD /bin/bash -c /app/bootstrap.sh



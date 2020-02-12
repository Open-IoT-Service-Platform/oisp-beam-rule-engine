# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#

#Build beam application and embedd in Spark container
FROM maven:3.6.1-jdk-8-alpine AS rule-engine-builder

RUN apk update && apk add build-base

ADD pom.xml /app/pom.xml
RUN mkdir /app/checkstyle
ADD checkstyle/checkstyle.xml /app/checkstyle/checkstyle.xml
ADD src /app/src

WORKDIR /app

RUN mvn checkstyle:check pmd:check clean package -Pflink-runner  -DskipTests

FROM python:3.8-alpine

ADD deployer/requirements.txt requirements.txt
ADD deployer/app.py app.py
RUN pip install -r requirements.txt

COPY --from=rule-engine-builder /app/target/rule-engine-bundled-0.1.jar rule-engine-bundled-0.1.jar
CMD python app.py rule-engine-bundled-0.1.jar

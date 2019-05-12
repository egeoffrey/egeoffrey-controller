### MYHOUSE ###

### define base image
ARG MYHOUSE_SDK_VERSION
ARG ARCHITECTURE
FROM myhouseproject/myhouse-sdk-python:${ARCHITECTURE}-${MYHOUSE_SDK_VERSION}

### disable local logging since running logger service
ENV MYHOUSE_LOGGING_LOCAL=0

### install module's dependencies
RUN pip install fuzzywuzzy pyyaml apscheduler redis==2.10.6 rq==0.12.0

### copy files into the image
COPY . $WORKDIR

### define the modules provided which needs to be started
ENV MYHOUSE_MODULES="controller/logger, controller/config, controller/db, controller/alerter, controller/chatbot, controller/hub"


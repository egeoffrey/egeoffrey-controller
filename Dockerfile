### MYHOUSE ###

### define base image
ARG SDK_VERSION
ARG ARCHITECTURE
FROM myhouseproject/myhouse-sdk-alpine:${ARCHITECTURE}-${SDK_VERSION}

### disable local logging since running logger service
ENV MYHOUSE_LOGGING_LOCAL=0

### install module's dependencies
RUN pip install fuzzywuzzy apscheduler redis==2.10.6 rq==0.12.0

### copy files into the image
COPY . $WORKDIR

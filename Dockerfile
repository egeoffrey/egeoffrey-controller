### EGEOFFREY ###

### define base image
ARG SDK_VERSION
ARG ARCHITECTURE
FROM egeoffrey/egeoffrey-sdk-alpine:${SDK_VERSION}-${ARCHITECTURE}

### disable local logging since running logger service
ENV EGEOFFREY_LOGGING_LOCAL=0

### install module's dependencies
RUN pip install fuzzywuzzy apscheduler redis==2.10.6 rq==0.12.0

### copy files into the image
COPY . $WORKDIR

### EGEOFFREY ###

### define base image
ARG SDK_VERSION
ARG ARCHITECTURE
FROM egeoffrey/egeoffrey-sdk-alpine:${SDK_VERSION}-${ARCHITECTURE}

### install module's dependencies
RUN pip install fuzzywuzzy apscheduler redis==2.10.6 rq==0.12.0

### copy files into the image
COPY . $WORKDIR

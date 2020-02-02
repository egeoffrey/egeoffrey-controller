### EGEOFFREY ###

### define base image
ARG SDK_VERSION
ARG ARCHITECTURE
FROM egeoffrey/egeoffrey-sdk-alpine:${SDK_VERSION}-${ARCHITECTURE}

### install module's dependencies
RUN pip install fuzzywuzzy redis==2.10.6 rq==0.12.0 pymongo

### copy files into the image
COPY . $WORKDIR

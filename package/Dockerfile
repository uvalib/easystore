#
# build the target application
#
FROM public.ecr.aws/docker/library/golang:1.25.4-alpine3.22 AS builder

# update the packages
RUN apk update && apk upgrade && apk add --no-cache make

WORKDIR /build
COPY tools ./tools
COPY uvaeasystore ./uvaeasystore
RUN cd /build/tools/easystore-blob; make linux
RUN cd /build/tools/easystore-delete; make linux
RUN cd /build/tools/easystore-export; make linux
RUN cd /build/tools/easystore-fields; make linux
RUN cd /build/tools/easystore-import; make linux
RUN cd /build/tools/easystore-query; make linux
RUN cd /build/tools/easystore-s3-check; make linux
RUN cd /build/tools/easystore-s3-rebuild; make linux
RUN cd /build/tools/easystore-stressor; make linux

#
# build the target container
#
FROM public.ecr.aws/docker/library/alpine:3.22

# update the packages
RUN apk update && apk upgrade && apk add bash

# Create the run user and group
RUN addgroup --gid 18570 sse && adduser --uid 1984 docker -G sse -D

# set the timezone appropriatly
#ENV TZ=UTC
#RUN cp /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Specify home 
ENV APP_HOME=/easystore-tools
WORKDIR ${APP_HOME}

# Create necessary directories
RUN mkdir -p ${APP_HOME}/bin
RUN chown -R docker ${APP_HOME} && chgrp -R sse ${APP_HOME}

# Move in necessary assets
COPY package/data/container_bash_profile /home/docker/.profile
COPY --from=builder /build/tools/easystore-blob/bin/easystore-blob.linux ${APP_HOME}/bin/easystore-blob
COPY --from=builder /build/tools/easystore-delete/bin/easystore-delete.linux ${APP_HOME}/bin/easystore-delete
COPY --from=builder /build/tools/easystore-export/bin/easystore-export.linux ${APP_HOME}/bin/easystore-export
COPY --from=builder /build/tools/easystore-fields/bin/easystore-fields.linux ${APP_HOME}/bin/easystore-fields
COPY --from=builder /build/tools/easystore-import/bin/easystore-import.linux ${APP_HOME}/bin/easystore-import
COPY --from=builder /build/tools/easystore-query/bin/easystore-query.linux ${APP_HOME}/bin/easystore-query
COPY --from=builder /build/tools/easystore-s3-check/bin/easystore-s3-check.linux ${APP_HOME}/bin/easystore-s3-check
COPY --from=builder /build/tools/easystore-s3-rebuild/bin/easystore-s3-rebuild.linux ${APP_HOME}/bin/easystore-s3-rebuild
COPY --from=builder /build/tools/easystore-stressor/bin/easystore-stressor.linux ${APP_HOME}/bin/easystore-stressor

# Ensure permissions are correct
RUN chown docker:sse /home/docker/.profile ${APP_HOME}/bin/* && chmod 755 /home/docker/.profile ${APP_HOME}/bin/*

# Add the build tag
ARG BUILD_TAG
RUN test -n "$BUILD_TAG" && touch ${APP_HOME}/buildtag.build-$BUILD_TAG || touch ${APP_HOME}/buildtag.build-0

# Specify the user
USER docker

#
# end of file
#

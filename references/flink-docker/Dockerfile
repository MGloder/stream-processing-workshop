###############################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
###############################################################################

  FROM openjdk:8-jre-alpine

  # Install dependencies, bash and su-exec for easy step-down from root
  RUN apk add --no-cache bash libc6-compat snappy 'su-exec>=0.2'

  # Configure Flink version
  ENV FLINK_VERSION=1.8.0 \
  HADOOP_SCALA_VARIANT=scala_2.12 \
  GPG_KEY=F2A67A8047499BBB3908D17AA8F4FD97121D7293

  # Prepare environment
  ENV FLINK_HOME=/opt/flink
  ENV PATH=$FLINK_HOME/bin:$PATH
  RUN addgroup -S -g 9999 flink && \
  adduser -D -S -H -u 9999 -G flink -h $FLINK_HOME flink
  WORKDIR $FLINK_HOME

  ENV FLINK_URL_FILE_PATH=flink/flink-${FLINK_VERSION}/flink-${FLINK_VERSION}-bin-${HADOOP_SCALA_VARIANT}.tgz
  # Not all mirrors have the .asc files
  ENV FLINK_TGZ_URL=https://www.apache.org/dyn/closer.cgi?action=download&filename=${FLINK_URL_FILE_PATH} \
  FLINK_ASC_URL=https://www.apache.org/dist/${FLINK_URL_FILE_PATH}.asc

  # Install Flink
  RUN set -ex; \
  apk add --no-cache --virtual .build-deps \
  ca-certificates \
  gnupg \
  openssl \
  tar \
  ; \
  \
  wget -nv -O flink.tgz "$FLINK_TGZ_URL"; \
  wget -nv -O flink.tgz.asc "$FLINK_ASC_URL"; \
  \
  export GNUPGHOME="$(mktemp -d)"; \
  for server in ha.pool.sks-keyservers.net $(shuf -e \
  hkp://p80.pool.sks-keyservers.net:80 \
  keyserver.ubuntu.com \
  hkp://keyserver.ubuntu.com:80 \
  pgp.mit.edu) ; do \
gpg --batch --keyserver "$server" --recv-keys "$GPG_KEY" && break || : ; \
  done && \
  gpg --batch --verify flink.tgz.asc flink.tgz; \
  gpgconf --kill all; \
  rm -rf "$GNUPGHOME" flink.tgz.asc; \
  \
  tar -xf flink.tgz --strip-components=1; \
  rm flink.tgz; \
  \
  apk del .build-deps; \
  \
  chown -R flink:flink .;

  # Configure container
  COPY docker-entrypoint.sh /
  ENTRYPOINT ["/docker-entrypoint.sh"]
  EXPOSE 6123 8081
  CMD ["help"]
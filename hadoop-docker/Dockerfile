# Copyright 2022 Jiaqi Liu. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
FROM ubuntu:latest

LABEL maintainer="Jiaqi (Jack) Liu"
LABEL maintainer-email="jack20220723@gmail.com"

ARG HADOOP_VERSION=2.8.4

# Ubuntu updates
RUN apt-get update
RUN apt-get upgrade -y
RUN apt update
RUN apt upgrade -y

# Java
RUN apt-get install openjdk-8-jdk -y
ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/jre/

# Auxiliary packages
RUN apt-get install wget -y
RUN apt-get install ssh -y
RUN apt-get install rsync -y
RUN apt-get install nano -y

# Download Hadoop
RUN wget https://archive.apache.org/dist/hadoop/core/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
RUN tar -xvzf hadoop-$HADOOP_VERSION.tar.gz
RUN rm hadoop-$HADOOP_VERSION.tar.gz

# Rename "hadoop-<version>" to "hadoop" as HADOOP_HOME
RUN mv hadoop-$HADOOP_VERSION hadoop

# Configure Hadoop env variables
ENV HADOOP_HOME /hadoop
ENV HADOOP_PREFIX /hadoop
ENV HADOOP_CONF_DIR $HADOOP_HOME/etc/hadoop/
ENV PATH $PATH:/hadoop/bin/
ENV HADOOP_MAPRED_HOME $HADOOP_HOME
ENV HADOOP_COMMON_HOME $HADOOP_HOME
ENV HADOOP_HDFS_HOME $HADOOP_HOME
ENV YARN_HOME $HADOOP_HOME
ENV HADOOP_CONFDIR ${HADOOP_HOME}/etc/hadoop
ENV HADOOP_COMMON_LIB_NATIVE_DIR ${HADOOP_PREFIX}/lib/native
ENV HADOOP_OPTS "-Djava.library.path=${HADOOP_PREFIX}/lib/native"
ENV JAVA_LIBRARY_PATH $HADOOP_HOME/lib/native:$JAVA_LIBRARY_PATH
ENV PATH $PATH:$HADOOP_HOME/
ENV PATH $PATH:$HADOOP_HOME/bin

# Prepare Hadoop config files
ADD core-site.xml $HADOOP_CONF_DIR/core-site.xml
ADD hdfs-site.xml $HADOOP_CONF_DIR/hdfs-site.xml
ADD mapred-site.xml $HADOOP_CONF_DIR/mapred-site.xml
# hadoop-env.sh
RUN sed -i '/export JAVA_HOME/d' $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export JAVA_HOME=${JAVA_HOME}" >> $HADOOP_CONF_DIR/hadoop-env.sh
RUN sed -i '/export HADOOP_CONF_DIR/d' $HADOOP_CONF_DIR/hadoop-env.sh
RUN echo "export HADOOP_CONF_DIR=${HADOOP_CONF_DIR}" >> $HADOOP_CONF_DIR/hadoop-env.sh

# Enable passwordless SSH
RUN ssh-keygen -q -N "" -t dsa -f /etc/ssh/ssh_host_dsa_key
RUN sh -c '/bin/echo -e "y\n" | ssh-keygen -q -N "" -t rsa -f /etc/ssh/ssh_host_rsa_key'
RUN sh -c '/bin/echo -e "y\n" | ssh-keygen -q -N "" -t rsa -f /root/.ssh/id_rsa'
RUN cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

# Prevent interactive question on first-time-SSH: "Are you sure you want to continue connecting (yes/no)?"
# Change config from "StrictHostKeyChecking ask" to "StrictHostKeyChecking no"
RUN sed -i s/ask/no/ /etc/ssh/ssh_config
# Uncomment "StrictHostKeyChecking no" by removing the "#" in the beginning of that config line
RUN sed -i '/^#.* StrictHostKeyChecking /s/^#//' /etc/ssh/ssh_config
# Add "UserKnownHostsFile=/dev/null" config with indentation
RUN echo "    UserKnownHostsFile=/dev/null" >> /etc/ssh/ssh_config

# Add script for starting HDFS
ADD init.sh /etc/init.sh
RUN chown root:root /etc/init.sh
RUN chmod 700 /etc/init.sh
ENV INIT /etc/init.sh

FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org|http://archive.debian.org|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    apt-get update -qq && apt-get install -y -qq wget && rm -rf /var/lib/apt/lists/*

# Install Apache Flume 1.11.0
RUN wget -q "https://archive.apache.org/dist/flume/1.11.0/apache-flume-1.11.0-bin.tar.gz" \
    -O /tmp/flume.tar.gz && \
    tar -xzf /tmp/flume.tar.gz -C /opt/ && \
    mv /opt/apache-flume-1.11.0-bin /opt/flume && \
    rm /tmp/flume.tar.gz

# Copy Hadoop HDFS libraries into Flume so the HDFS sink works
RUN cp /opt/hadoop-3.2.1/share/hadoop/hdfs/hadoop-hdfs-*.jar /opt/flume/lib/ 2>/dev/null || true && \
    cp /opt/hadoop-3.2.1/share/hadoop/hdfs/hadoop-hdfs-client-*.jar /opt/flume/lib/ 2>/dev/null || true && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/hadoop-common-*.jar /opt/flume/lib/ 2>/dev/null || true && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/lib/commons-configuration2-*.jar /opt/flume/lib/ 2>/dev/null || true && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/lib/htrace-core4-*.jar /opt/flume/lib/ 2>/dev/null || true && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/lib/hadoop-auth-*.jar /opt/flume/lib/ 2>/dev/null || true

# Replace Flume's old Guava 11 with Hadoop's Guava 27 (required for Hadoop 3.x HDFS sink)
RUN rm -f /opt/flume/lib/guava-11.0.2.jar && \
    cp /opt/hadoop-3.2.1/share/hadoop/common/lib/guava-27.0-jre.jar /opt/flume/lib/

# Pre-create Hadoop core-site.xml so HDFS sink resolves namenode
RUN mkdir -p /etc/hadoop && \
    printf '<configuration>\n  <property>\n    <name>fs.defaultFS</name>\n    <value>hdfs://namenode:9000</value>\n  </property>\n</configuration>' \
    > /etc/hadoop/core-site.xml

ENV FLUME_HOME=/opt/flume
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV PATH="${PATH}:${FLUME_HOME}/bin"

COPY flume.conf /opt/flume/conf/flume.conf

CMD ["tail", "-f", "/dev/null"]

FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i 's|http://security.debian.org|http://archive.debian.org|g' /etc/apt/sources.list && \
    sed -i '/stretch-updates/d' /etc/apt/sources.list && \
    apt-get update -qq && apt-get install -y -qq wget && rm -rf /var/lib/apt/lists/*

# Install Sqoop 1.4.7
RUN wget -q "https://archive.apache.org/dist/sqoop/1.4.7/sqoop-1.4.7.bin__hadoop-2.6.0.tar.gz" \
    -O /tmp/sqoop.tar.gz && \
    tar -xzf /tmp/sqoop.tar.gz -C /opt/ && \
    mv /opt/sqoop-1.4.7.bin__hadoop-2.6.0 /opt/sqoop && \
    rm /tmp/sqoop.tar.gz

# MySQL 8 JDBC connector
RUN wget -q "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar" \
    -O /opt/sqoop/lib/mysql-connector-java.jar

# Copy commons-lang3 from Hadoop 3.x (needed for Sqoop 1.x + Hadoop 3.x compat)
RUN cp /opt/hadoop-3.2.1/share/hadoop/common/lib/commons-lang3-*.jar /opt/sqoop/lib/ 2>/dev/null || true

# commons-lang 2.6 required by Sqoop 1.4.7 (Hadoop 3.x dropped v2)
RUN wget -q "https://repo1.maven.org/maven2/commons-lang/commons-lang/2.6/commons-lang-2.6.jar" \
    -O /opt/sqoop/lib/commons-lang-2.6.jar

ENV SQOOP_HOME=/opt/sqoop
ENV HADOOP_HOME=/opt/hadoop-3.2.1
ENV PATH="${PATH}:${SQOOP_HOME}/bin"

CMD ["tail", "-f", "/dev/null"]

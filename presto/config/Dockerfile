FROM prestodb/presto:latest

USER root

ADD delta.properties /opt/presto-server/etc/catalog/
ADD hive.properties /opt/presto-server/etc/catalog/
COPY config.properties /opt/presto-server/etc/config.properties
COPY jvm.config /opt/presto-server/etc/jvm.config
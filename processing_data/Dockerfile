FROM apache/spark:v3.2.1

COPY target/scala-2.12/processing_2.12-0.1.jar /opt/target/scala-2.12/

COPY src/main/resources/log4j.properties /app/src/main/resources/

# ADD run.sh /opt/run.sh

WORKDIR /opt

USER root

# RUN chmod 777 /opt/run.sh

# ENTRYPOINT "./run.sh"

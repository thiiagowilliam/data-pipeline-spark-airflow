FROM apache/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y openjdk-17-jdk wget curl netcat && \
    rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master"]
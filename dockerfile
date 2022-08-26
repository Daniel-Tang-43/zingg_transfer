FROM docker.io/bitnami/spark:3.1.2
ENV SPARK_MASTER local[*]
ENV ZINGG_HOME /zingg-0.3.4-SNAPSHOT
ENV PATH $ZINGG_HOME/scripts:$PATH
ENV LANG C.UTF-8
WORKDIR /
USER root
WORKDIR /zingg-0.3.4-SNAPSHOT
RUN curl --location https://www.dropbox.com/s/2mj1bq8qfzpm9r8/zingg-0.3.4-SNAPSHOT.tar.gz?dl=0 | \
tar --extract --gzip --strip=1 
RUN pip install -r python/requirements.txt
RUN pip install zingg
RUN chmod -R a+rwx /zingg-0.3.4-SNAPSHOT/models
RUN chown -R 1001 /zingg-0.3.4-SNAPSHOT/models
USER 1001

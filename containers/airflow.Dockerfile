# Airflow's dependencies stay separate from the pinned ML/Spark environment.
ARG ML_IMAGE=opsyntra-spark:local
FROM ${ML_IMAGE} AS ml
FROM apache/airflow:3.3.2-python3.12
USER root
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=ml /opt/opsyntra-ml /opt/opsyntra-ml
COPY --from=ml /opt/java/openjdk /opt/java/openjdk
COPY --from=ml --chown=1000:0 /app/backend /app/backend
ENV JAVA_HOME=/opt/java/openjdk PYTHONPATH=/app/backend/src \
    OPSYNTRA_CONFIG=/tmp/opsyntra-config.yaml SPARK_LOCAL_IP=127.0.0.1 \
    PATH=/opt/java/openjdk/bin:$PATH
WORKDIR /app/backend
USER 1000:0
# Preserve the official UID/group setup and signal forwarding.
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["bash", "-c", "/opt/opsyntra-ml/bin/python -c 'from container_runtime import prepare_config; prepare_config()' && exec airflow standalone"]

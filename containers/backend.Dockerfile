# syntax=docker/dockerfile:1
FROM python:3.12-slim-bookworm AS core
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/backend/src OPSYNTRA_CONFIG=/tmp/opsyntra-config.yaml \
    OPSYNTRA_KAFKA_BOOTSTRAP_SERVERS=kafka:29092 OMP_NUM_THREADS=2
WORKDIR /app/backend
ENV PATH=/opt/opsyntra-ml/bin:$PATH
RUN python -m venv /opt/opsyntra-ml
RUN apt-get update && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*
COPY backend/requirements/ci-py312-linux.lock /tmp/requirements.lock
RUN --mount=type=cache,target=/root/.cache/pip pip install --require-hashes -r /tmp/requirements.lock
RUN groupadd -g 1000 opsyntra && useradd -u 1000 -g 1000 -m opsyntra
COPY backend/src ./src
COPY backend/pipelines ./pipelines
COPY backend/tools ./tools
COPY backend/migrations ./migrations
COPY backend/examples ./examples
COPY backend/dags ./dags
COPY backend/config.yaml backend/producer.py backend/consumer.py backend/analytics_consumer.py ./
COPY containers/runtime.py ./container_runtime.py
COPY containers/checks.py ./container_checks.py
RUN mkdir -p data artifacts/models reports tracking mlartifacts \
    && chown -R opsyntra:opsyntra /app/backend
USER opsyntra
ENTRYPOINT ["python", "container_runtime.py"]
CMD ["api"]

FROM eclipse-temurin:21-jre-jammy AS java
FROM core AS spark
USER root
COPY --from=java /opt/java/openjdk /opt/java/openjdk
ENV JAVA_HOME=/opt/java/openjdk PATH=/opt/java/openjdk/bin:$PATH SPARK_LOCAL_IP=127.0.0.1
COPY backend/requirements/ci-spark-py312-linux.lock /tmp/spark-requirements.lock
RUN --mount=type=cache,target=/root/.cache/pip pip install --require-hashes -r /tmp/spark-requirements.lock
USER opsyntra
CMD ["spark-train"]

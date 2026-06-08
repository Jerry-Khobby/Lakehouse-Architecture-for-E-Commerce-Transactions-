# Local test runner — kept in lockstep with .github/workflows/ci.yml so that
# "green locally" means "green in CI".
#   Python 3.10  +  Java 11 (temurin via bullseye)  +  pyspark 3.3.2
# matches AWS Glue 4.0's Spark 3.3.x runtime. delta-spark is NOT installed:
# unit tests mock DeltaTable, so no Delta JAR is needed (see requirements-dev.txt).
FROM python:3.10-slim-bullseye

# procps provides `ps`, which Spark's launch scripts call; the JRE runs the JVM.
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install pinned dev/test deps first so the layer caches across source changes.
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

COPY . .

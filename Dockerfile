FROM apache/spark:3.5.0-python3

USER root

WORKDIR /app

# Install test tooling only.
# pyspark is NOT pip-installed — it ships with the base image at $SPARK_HOME/python,
# which avoids downloading the ~300 MB wheel on every build.
RUN ln -sf /usr/bin/python3 /usr/bin/python && \
    pip install --no-cache-dir \
    boto3 \
    pytest==8.3.5 \
    pytest-cov==5.0.0 \
    black==24.8.0 \
    flake8==7.1.1

COPY . .

# Expose the bundled pyspark + py4j from the Spark installation so that
# a plain `python` process (not spark-submit) can import pyspark normally.
# py4j-0.10.9.7 is the version shipped with Spark 3.5.0.
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"

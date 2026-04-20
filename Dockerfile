FROM apache/spark:3.5.1

USER root

# ----------------------------
# System dependencies
# ----------------------------
RUN apt-get update && apt-get install -y \
    curl \
    git \
    ca-certificates \
 && update-ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# ----------------------------
# Python dependencies
# ----------------------------
RUN python3 -m pip install --upgrade pip setuptools wheel

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ----------------------------
# App setup
# ----------------------------
WORKDIR /app
COPY . /app

# Set write permissions
RUN mkdir -p /app/logs /app/control  \ 
    && chown -R spark:spark /app/logs /app/control
#/app/data-lake /app/dbt /app/data-lake/raw /app/data-lake/landing /app/data-lake/bronze /app/data-lake/silver /app/data-lake/gold
# ----------------------------
# Delta Lake JARs
# ----------------------------
RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
    -o /opt/spark/jars/delta-spark_2.12-3.0.0.jar && \
    curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar \
    -o /opt/spark/jars/delta-storage-3.0.0.jar

# ----------------------------
# Switch to spark user
# ----------------------------
# USER spark
# # Set write permissions
# RUN mkdir -p /app/logs /app/control /app/data-lake /app/dbt \ 
#     && chown -R spark:spark /app/logs /app/control /app/data-lake /app/dbt

ARG UID
ARG GID

RUN groupadd -g ${GID} appgroup \
 && useradd -m -u ${UID} -g ${GID} appuser

USER appuser

WORKDIR /app

CMD ["bash"]
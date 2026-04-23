# FROM apache/spark:3.5.1

# USER root

# # ----------------------------
# # System dependencies
# # ----------------------------
# RUN apt-get update && apt-get install -y \
#     curl \
#     git \
#     ca-certificates \
#  && update-ca-certificates \
#  && rm -rf /var/lib/apt/lists/*

# # ----------------------------
# # Python dependencies
# # ----------------------------
# RUN python3 -m pip install --upgrade pip setuptools wheel

# COPY requirements.txt /tmp/requirements.txt
# RUN pip install --no-cache-dir -r /tmp/requirements.txt

# # ----------------------------
# # App setup
# # ----------------------------
# WORKDIR /app
# COPY . /app

# # Set write permissions
# RUN mkdir -p /app/logs /app/control  \ 
#     && chown -R spark:spark /app/logs /app/control
# #/app/data-lake /app/dbt /app/data-lake/raw /app/data-lake/landing /app/data-lake/bronze /app/data-lake/silver /app/data-lake/gold
# # ----------------------------
# # Delta Lake JARs
# # ----------------------------
# RUN curl -L https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.0.0/delta-spark_2.12-3.0.0.jar \
#     -o /opt/spark/jars/delta-spark_2.12-3.0.0.jar && \
#     curl -L https://repo1.maven.org/maven2/io/delta/delta-storage/3.0.0/delta-storage-3.0.0.jar \
#     -o /opt/spark/jars/delta-storage-3.0.0.jar

# # ----------------------------
# # Switch to spark user
# # ----------------------------
# # USER spark
# # # Set write permissions
# # RUN mkdir -p /app/logs /app/control /app/data-lake /app/dbt \ 
# #     && chown -R spark:spark /app/logs /app/control /app/data-lake /app/dbt

# ARG UID
# ARG GID

# RUN groupadd -g ${GID} appgroup \
#  && useradd -m -u ${UID} -g ${GID} appuser

# USER appuser

# WORKDIR /app

# CMD ["bash"]




FROM python:3.11-slim

USER root

WORKDIR /app

# ------------------------
# System dependencies
# ------------------------
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    ca-certificates \
    && update-ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN curl -L https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip -o duckdb.zip && \
    apt-get update && apt-get install -y unzip && \
    unzip duckdb.zip && \
    mv duckdb /usr/local/bin/duckdb && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb.zip

# ------------------------
# Install Python deps first (cache optimization)
# ------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ------------------------
# Create user/group
# ------------------------
ARG UID=1000
ARG GID=1000

RUN groupadd -g ${GID} appgroup \
 && useradd -m -u ${UID} -g ${GID} appuser

# ------------------------
# Copy project 
# ------------------------
COPY . /app

# ------------------------
# Fix permissions 
# ------------------------
RUN mkdir -p /app/logs /app/control /app/data-lake \
 && chown -R appuser:appgroup /app

# ------------------------
# Switch to non-root user
# ------------------------
USER appuser

WORKDIR /app

CMD ["bash"]
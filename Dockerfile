FROM python:3.11-slim
FROM apache/airflow:2.3.4

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

ENV PYTHONPATH=/app
EXPOSE 8000
CMD ["uvicorn", "engine.api.main:app", "--host", "0.0.0.0", "--port", "8000"]

# Install Java (OpenJDK 8 or 11 depending on your preference)
USER root
RUN echo "deb http://archive.debian.org/debian bullseye main" > /etc/apt/sources.list && \
    apt-get update -o Acquire::Check-Valid-Until=false && \
    apt-get install -y openjdk-11-jdk && \
    apt-get clean;

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER airflow
COPY requirements.txt .
RUN pip install -r requirements.txt
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DAGSTER_HOME=/opt/dagster/dagster_home \
    DATA_DIR=/opt/dagster/data

WORKDIR /opt/dagster/app

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    sqlite3 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

RUN mkdir -p /opt/dagster/dagster_home /opt/dagster/data

COPY . /opt/dagster/app

VOLUME ["/opt/dagster/dagster_home", "/opt/dagster/data"]

EXPOSE 4000

CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "4000"]

FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY migrations /app/migrations
COPY alembic.ini /app/alembic.ini

RUN pip install --no-cache-dir .

ENV PYTHONUNBUFFERED=1
ENV DML_LOG_JSON=true

EXPOSE 8000 9100

CMD ["python", "-m", "dml.api.main"]

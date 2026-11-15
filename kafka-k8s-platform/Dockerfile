# syntax=docker/dockerfile:1
FROM python:3.12-slim AS base
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*
COPY packages/dap_common /app/packages/dap_common
RUN pip install --no-cache-dir /app/packages/dap_common

FROM base AS api
RUN pip install --no-cache-dir "fastapi>=0.110" "uvicorn[standard]>=0.27" "python-multipart>=0.0.9" "PyJWT>=2.8.0"
COPY services/api /app/services/api
ENV PYTHONPATH=/app/services/api/src
ENV DAP_SERVICE_NAME=api
CMD ["python", "-m", "dap_api.main"]

FROM base AS ingest
COPY services/ingest /app/services/ingest
ENV PYTHONPATH=/app/services/ingest/src
ENV DAP_SERVICE_NAME=ingest
CMD ["python", "-m", "dap_ingest.main"]

FROM base AS etl
COPY services/etl /app/services/etl
ENV PYTHONPATH=/app/services/etl/src
ENV DAP_SERVICE_NAME=etl
CMD ["python", "-m", "dap_etl.main"]

FROM base AS embed
# sentence-transformers is optional: only needed when DAP_EMBEDDING_BACKEND=huggingface
COPY services/embed /app/services/embed
ENV PYTHONPATH=/app/services/embed/src
ENV DAP_SERVICE_NAME=embed
CMD ["python", "-m", "dap_embed.main"]

FROM base AS embed-hf
RUN pip install --no-cache-dir sentence-transformers
COPY services/embed /app/services/embed
ENV PYTHONPATH=/app/services/embed/src
ENV DAP_SERVICE_NAME=embed
ENV DAP_EMBEDDING_BACKEND=huggingface
CMD ["python", "-m", "dap_embed.main"]

FROM base AS index
COPY services/index /app/services/index
ENV PYTHONPATH=/app/services/index/src
ENV DAP_SERVICE_NAME=index
CMD ["python", "-m", "dap_index.main"]

FROM base AS iot-edge
COPY services/iot_edge /app/services/iot_edge
ENV PYTHONPATH=/app/services/iot_edge/src
ENV DAP_SERVICE_NAME=iot-edge
CMD ["python", "-m", "dap_iot_edge.main"]

FROM base AS mqtt-bridge
RUN pip install --no-cache-dir "aiomqtt>=2.0.0"
COPY services/mqtt_bridge /app/services/mqtt_bridge
ENV PYTHONPATH=/app/services/mqtt_bridge/src
ENV DAP_SERVICE_NAME=mqtt-bridge
CMD ["python", "-m", "dap_mqtt_bridge.main"]

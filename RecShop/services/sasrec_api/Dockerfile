# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM python:3.10-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        build-essential \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY LICENSE.md /app/LICENSE.md

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
    && pip install -r /app/requirements.txt

COPY shared/ /app/shared/
COPY services/sasrec_api/ /app/services/sasrec_api/

WORKDIR /app/services/sasrec_api

EXPOSE 8200

CMD ["python", "api_server.py"]

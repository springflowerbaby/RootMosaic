# Unified Python dependencies for the business services, including CPU PyTorch.
# Build from the repository root; model/cache assets are mounted at runtime.
FROM python:3.10-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY LICENSE.md /app/LICENSE.md
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip \
    && pip install torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu \
    && pip install -r /app/requirements.txt
CMD ["python", "--version"]

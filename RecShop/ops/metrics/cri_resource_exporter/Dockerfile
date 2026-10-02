# Build context: this directory. Pin the chosen base image for a reproducible release.
# Containerd socket access is privileged observation access, even with a read-only mount.
FROM python:3.10-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY requirements.txt /tmp/cri-exporter-requirements.txt
RUN pip install --no-cache-dir -r /tmp/cri-exporter-requirements.txt

COPY *.py /app/cri_resource_exporter/

EXPOSE 9738

ENTRYPOINT ["python", "-m", "cri_resource_exporter.exporter"]
CMD ["--cri-endpoint", "unix:///run/containerd/containerd.sock", \
     "--listen", "0.0.0.0:9738"]

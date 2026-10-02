# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/backend_api/ /app/services/backend_api/

WORKDIR /app/services/backend_api

EXPOSE 5000

CMD ["python", "app.py"]

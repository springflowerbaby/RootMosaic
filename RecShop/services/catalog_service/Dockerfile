# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/catalog_service/ /app/services/catalog_service/

WORKDIR /app/services/catalog_service

EXPOSE 5005

CMD ["python", "app.py"]

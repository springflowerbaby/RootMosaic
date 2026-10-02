# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/inventory_service/ /app/services/inventory_service/

WORKDIR /app/services/inventory_service

EXPOSE 5013

CMD ["python", "app.py"]

# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/pricing_service/ /app/services/pricing_service/

WORKDIR /app/services/pricing_service

EXPOSE 5014

CMD ["python", "app.py"]

# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/promotion_service/ /app/services/promotion_service/

WORKDIR /app/services/promotion_service

EXPOSE 5015

CMD ["python", "app.py"]

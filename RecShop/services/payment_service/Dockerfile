# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/payment_service/ /app/services/payment_service/

WORKDIR /app/services/payment_service

EXPOSE 5012

CMD ["python", "app.py"]

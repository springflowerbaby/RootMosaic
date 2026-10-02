# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/checkout_service/ /app/services/checkout_service/

WORKDIR /app/services/checkout_service

EXPOSE 5011

CMD ["python", "app.py"]

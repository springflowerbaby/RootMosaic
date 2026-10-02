# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/order_service/ /app/services/order_service/

WORKDIR /app/services/order_service

EXPOSE 5010

CMD ["python", "app.py"]

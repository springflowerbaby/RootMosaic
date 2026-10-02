# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/cart_service/ /app/services/cart_service/

WORKDIR /app/services/cart_service

EXPOSE 5006

CMD ["python", "app.py"]

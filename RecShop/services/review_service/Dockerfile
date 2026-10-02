# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/review_service/ /app/services/review_service/

WORKDIR /app/services/review_service

EXPOSE 5003

CMD ["python", "app.py"]

# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/notification_service/ /app/services/notification_service/

WORKDIR /app/services/notification_service

EXPOSE 5021

CMD ["python", "app.py"]

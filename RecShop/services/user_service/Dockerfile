# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/user_service/ /app/services/user_service/

WORKDIR /app/services/user_service

EXPOSE 5004

CMD ["python", "app.py"]

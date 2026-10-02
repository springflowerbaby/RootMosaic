# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/interaction_service/ /app/services/interaction_service/

WORKDIR /app/services/interaction_service

EXPOSE 5020

CMD ["python", "app.py"]

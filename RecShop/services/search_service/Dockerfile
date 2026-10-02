# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/search_service/ /app/services/search_service/

WORKDIR /app/services/search_service

EXPOSE 5017

CMD ["python", "app.py"]

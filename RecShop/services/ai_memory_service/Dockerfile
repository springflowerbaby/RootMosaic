# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/ai_memory_service/ /app/services/ai_memory_service/

WORKDIR /app/services/ai_memory_service

EXPOSE 5008

CMD ["python", "app.py"]

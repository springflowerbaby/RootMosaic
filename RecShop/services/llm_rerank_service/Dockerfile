# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/llm_rerank_service/ /app/services/llm_rerank_service/

WORKDIR /app/services/llm_rerank_service

EXPOSE 5002

CMD ["python", "app.py"]

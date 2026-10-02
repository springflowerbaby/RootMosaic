# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/recommendation_agent/ /app/services/recommendation_agent/

WORKDIR /app/services/recommendation_agent

EXPOSE 5001

CMD ["python", "app.py"]

# Build context: repository root. Preserve service templates, static assets and shared code.
# Secrets and model/data blobs are excluded from the image; provide them at runtime.
FROM recweb-base:latest

WORKDIR /app

COPY shared/ /app/shared/
COPY services/shop_web/ /app/services/shop_web/

WORKDIR /app/services/shop_web

EXPOSE 3000

CMD ["python", "run.py"]

FROM python:3.11-alpine
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn \
    && addgroup -S appgroup && adduser -S appuser -G appgroup
COPY . .
RUN mkdir -p /data && chown appuser:appgroup /data
ENV DATA_DIR=/data
USER appuser
EXPOSE 5000
CMD ["gunicorn", "-w", "2", "-b", "0.0.0.0:5000", "server:app"]

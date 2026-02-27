FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY app/ app/
COPY static/ static/

# Create data directory
RUN mkdir -p /data

# Expose port
EXPOSE 8299

# Run the app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8299"]

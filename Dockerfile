FROM python:3.11-slim

# Optional: set timezone (helps with logging)
ENV TZ=America/Toronto

# Workdir in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the script
COPY mqtt2exist.py .

# Default command
CMD ["python", "-u", "mqtt2exist.py"]


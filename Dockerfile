FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend code
COPY Backend/ ./Backend/

# Copy frontend code
COPY Frontend/ ./Frontend/

# Create notebooks directory
RUN mkdir -p /app/Backend/notebooks

# Set working directory to Backend
WORKDIR /app/Backend

# Environment variables
ENV FRONTEND_DIR=/app/Frontend
ENV NOTEBOOK_FILE=notebooks/notebook.py

# Expose port
EXPOSE 8000

# Run the application
# Exclude notebooks dir from reload watcher to prevent infinite reload loop
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload", "--reload-exclude", "notebooks/*"]

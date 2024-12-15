FROM apache/superset:latest

# Switch to root to install dependencies
USER root

# Install system dependencies and clean up to reduce the size of the image
RUN apt-get update && apt-get install -y \
    python3-dev \
    libsasl2-dev \
    libldap2-dev \
    libssl-dev \
  && rm -rf /var/lib/apt/lists/*  # Cleanup apt cache to reduce layer size

# Create directories with proper permissions
RUN mkdir -p /app/superset_home/db && \
    chown -R superset:superset /app/superset_home

# Install Python dependencies
RUN pip install --no-cache-dir psycopg2-binary cassandra-driver

# Switch back to superset user for running the app
USER superset

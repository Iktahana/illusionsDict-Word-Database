FROM python:3.12-slim

# Install Node.js 20, git
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        git \
        ca-certificates \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install gemini CLI (pin to same version as local)
RUN npm install -g @google/gemini-cli@0.34.0

# ~/.gemini will be provided by volume mount at runtime (host credentials)
# Create a placeholder so the directory exists if mount is not used
RUN mkdir -p /root/.gemini

# Copy script only (data is volume-mounted)
WORKDIR /app
COPY script/generate_examples.py /app/script/generate_examples.py

# Git config for auto-commit (can be overridden via env)
ARG GIT_USER_NAME="illusionsDict Bot"
ARG GIT_USER_EMAIL="bot@illusions-dict.local"
RUN git config --global user.name "${GIT_USER_NAME}" \
 && git config --global user.email "${GIT_USER_EMAIL}" \
 && git config --global --add safe.directory /app

ENTRYPOINT ["python", "/app/script/generate_examples.py"]
CMD []

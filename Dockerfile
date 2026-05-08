FROM node:20-slim

# System deps:
#  - ffmpeg + ffprobe for video assembly
#  - python3 + pip for yt-dlp (more reliable than the apt yt-dlp which lags upstream)
#  - fonts-liberation for the burned-in subtitles (close-enough Georgia substitute)
#  - ca-certificates for HTTPS to Google Photos / LRCLIB / YouTube
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      ffmpeg \
      python3 python3-pip \
      ca-certificates \
      fonts-liberation fonts-dejavu \
      aubio-tools \
    && pip3 install --break-system-packages --no-cache-dir -U yt-dlp \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install JS deps in a separate layer for caching
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund

# Application code
COPY server.js ./
COPY public ./public/

# Persist generated jobs across container recreation
VOLUME /app/jobs

ENV PORT=3737
EXPOSE 3737

CMD ["node", "server.js"]

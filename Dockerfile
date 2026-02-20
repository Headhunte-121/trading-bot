FROM python:3.12-slim
WORKDIR /app

# 1. System Tools (Cached)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    && rm -rf /var/lib/apt/lists/*

# 2. THE "HEAVY HITTER" LAYER
# I removed the version pin on torch so it grabs the one compatible with Python 3.12
RUN pip install --no-cache-dir \
    torch \
    transformers \
    pandas \
    pandas-ta \
    numpy \
    scipy \
    bitsandbytes \
    accelerate \
    yfinance \
    alpaca-trade-api \
    finnhub-python \
    --default-timeout=2000

# 3. The "Delta" Layer
COPY requirements.txt .

# 4. Install only what's missing
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "shared/schema.py"]

FROM python:3.12-slim
WORKDIR /app

# 1. System Tools (Cached)
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    git \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# 2. THE "HEAVY HITTER" LAYER
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
    git+https://github.com/amazon-science/chronos-forecasting.git

# 3. The "Delta" Layer
COPY requirements.txt .

# 4. Install only what's missing
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "shared/schema.py"]

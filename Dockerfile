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
# We install ALL the big libraries here manually. 
# As long as you don't edit THIS specific block, Docker will never download these again.
# Even if you change requirements.txt later, this layer stays "Frozen".
RUN pip install --no-cache-dir \
    torch==2.1.0 \
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
# Now we copy the file. If you add a new library (like 'requests' or 'beautifulsoup'),
# Docker detects the change here.
COPY requirements.txt .

# 4. Install only what's missing
# Pip is smart. It will look at requirements.txt, see that Torch/Pandas/etc 
# are already installed from Step 2, and say "Requirement already satisfied."
# It will ONLY download the new stuff you added.
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
ENV PYTHONUNBUFFERED=1
CMD ["python", "shared/schema.py"]

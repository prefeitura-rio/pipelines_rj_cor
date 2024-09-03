# Build arguments
ARG PYTHON_VERSION=3.10-slim

# Start Python image
FROM python:${PYTHON_VERSION}

# Install git
# hadolint ignore=DL3008
RUN apt-get update && \
    apt-get install -y --no-install-recommends git ffmpeg libsm6 libxext6 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install GDAL dependencies
FROM curlimages/curl:7.81.0 as curl-step
ARG GDAL_WHEELS_URL=https://prefeitura-rio.github.io/storage/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl
RUN curl -sSLo /tmp/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl $GDAL_WHEELS_URL

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION $PREFECT_VERSION

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
WORKDIR /app
COPY . .
COPY --from=curl-step /tmp/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl /tmp/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl
RUN python3 -m pip install --prefer-binary --no-cache-dir -U . \
    python3 -m pip install --no-cache-dir /tmp/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl && \
    rm /tmp/GDAL-3.4.1-cp39-cp39-manylinux_2_5_x86_64.manylinux1_x86_64.whl
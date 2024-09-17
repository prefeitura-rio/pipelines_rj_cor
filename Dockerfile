# Build arguments
ARG PYTHON_VERSION=3.9-slim-buster

# Start Python image
FROM python:${PYTHON_VERSION} as base

# Install git and other dependencies
# hadolint ignore=DL3008
# Install git and other dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    ffmpeg=7:4.1.11-0+deb10u1 \
    git=1:2.20.1-2+deb10u9 \
    libsm6=2:1.2.3-1 \
    libxext6=2:1.3.3-1+b2 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Setting environment variables for libgdal
ENV CPLUS_INCLUDE_PATH /usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Setting environment with prefect version
ARG PREFECT_VERSION=1.4.1
ENV PREFECT_VERSION $PREFECT_VERSION

# Setup virtual environment and prefect
ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:$PATH"
RUN python3 -m pip install --no-cache-dir -U "pip>=21.2.4" "prefect==$PREFECT_VERSION"

# Install requirements
FROM base as final
WORKDIR /app
COPY . .
RUN python3 -m pip install --prefer-binary --no-cache-dir -U .

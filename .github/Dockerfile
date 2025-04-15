# Copyright 2024 CS Group
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

FROM ghcr.io/rs-python/python:3.11.7-slim-bookworm AS builder

# Install dependencies
RUN apt update && apt upgrade -y && apt install -y libpq-dev gcc

# Update pip version
RUN pip install --no-cache-dir --upgrade pip

RUN useradd -m user
USER user
WORKDIR /home/user

ENTRYPOINT [ \
    "python", "-m", "uvicorn", "rs_dpr_service.main:app", \
    "--host", "0.0.0.0", "--port", "8000", "[DEBUG_MODE_RELOAD]" ]

LABEL org.opencontainers.image.source="https://github.com/RS-PYTHON/rs-dpr-service"
LABEL org.opencontainers.image.ref.name="ghcr.io/rs-python/rs-dpr-service"

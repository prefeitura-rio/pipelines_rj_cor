# -*- coding: utf-8 -*-
from enum import Enum


class constants(Enum):
    ######################################
    # Automatically managed,
    # please do not change these values
    ######################################
    # Docker image
    DOCKER_TAG = "AUTO_REPLACE_DOCKER_TAG"
    DOCKER_IMAGE_NAME = "AUTO_REPLACE_DOCKER_IMAGE"
    DOCKER_IMAGE = f"{DOCKER_IMAGE_NAME}:{DOCKER_TAG}"
    GCS_FLOWS_BUCKET = "datario-public"

    ######################################
    # Agent labels
    ######################################
    # EXAMPLE_AGENT_LABEL = "example_agent"

    RJ_COR_AGENT_LABEL = "cor"

    ######################################
    # Other constants
    ######################################
    # Prefect
    PREFECT_DEFAULT_PROJECT = "main"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds

    ######################################
    # Discord code owners constants
    ######################################
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"

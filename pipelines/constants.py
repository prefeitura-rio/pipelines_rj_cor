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

    RJ_ESCRITORIO_AGENT_LABEL = "rj-escritorio"
    RJ_ESCRITORIO_DEV_AGENT_LABEL = "rj-escritorio-dev"

    ######################################
    # Other constants
    ######################################
    # Prefect
    PREFECT_DEFAULT_PROJECT = "main"
    # Prefect tasks retry policy
    TASK_MAX_RETRIES = 5
    TASK_RETRY_DELAY = 10  # seconds
    DEFAULT_CODE_OWNERS = ["paty"]
    OWNERS_DISCORD_MENTIONS = {
        # Register all code owners, users_id and type
        #     - possible types: https://docs.discord.club/embedg/reference/mentions
        #     - how to discover user_id: https://www.remote.tools/remote-work/how-to-find-discord-id
        #     - types: user, user_nickname, channel, role
        "pipeliners": {
            "user_id": "962067746651275304",
            "type": "role",
        },
        # "gabriel": {
        #     "user_id": "218800040137719809",
        #     "type": "user_nickname",
        # },
        "diego": {
            "user_id": "272581753829326849",
            "type": "user_nickname",
        },
        "joao": {
            "user_id": "692742616416256019",
            "type": "user_nickname",
        },
        "fernanda": {
            "user_id": "692709168221650954",
            "type": "user_nickname",
        },
        "paty": {
            "user_id": "821121576455634955",
            "type": "user_nickname",
        },
        "bruno": {
            "user_id": "183691546942636033",
            "type": "user_nickname",
        },
        "caio": {
            "user_id": "276427674002522112",
            "type": "user_nickname",
        },
        "anderson": {
            "user_id": "553786261677015040",
            "type": "user_nickname",
        },
        "rodrigo": {
            "user_id": "1031636163804545094",
            "type": "user_nickname",
        },
        "boris": {
            "user_id": "1109195532884262934",
            "type": "user_nickname",
        },
        "thiago": {
            "user_id": "404716070088343552",
            "type": "user_nickname",
        },
        "andre": {
            "user_id": "369657115012366336",
            "type": "user_nickname",
        },
        "rafaelpinheiro": {
            "user_id": "1131538976101109772",
            "type": "user_nickname",
        },
        "carolinagomes": {
            "user_id": "620000269392019469",
            "type": "user_nickname",
        },
        "karinappassos": {
            "user_id": "222842688117014528",
            "type": "user_nickname",
        },
        "danilo": {
            "user_id": "1147152438487416873",
            "type": "user_nickname",
        },
        "dados_smtr": {
            "user_id": "1056928259700445245",
            "type": "role",
        },
    }

    # Infisical
    INFISICAL_URL = "URL"
    INFISICAL_USERNAME = "USERNAME"
    INFISICAL_PASSWORD = "PASSWORD"

    ######################################
    # Discord code owners constants
    ######################################
    EMD_DISCORD_WEBHOOK_SECRET_PATH = "prefect-discord-webhook"

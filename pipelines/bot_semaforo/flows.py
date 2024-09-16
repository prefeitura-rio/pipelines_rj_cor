# -*- coding: utf-8 -*-
"""
Flows for cor
"""

from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

from pipelines.bot_semaforo.schedules import bot_schedule
from pipelines.bot_semaforo.tasks import (
    format_message,
    get_data,
    get_token_and_group_id,
    send_messages,
)
from pipelines.constants import constants
from pipelines.utils.decorators import Flow

with Flow(
    name="COR: CET semáforos - Telegram Bot",
) as cet_telegram_flow:
    secret_path = Parameter("secret_path")

    # Get credentials for Telegram
    token, group_id = get_token_and_group_id(secret_path=secret_path)

    # Get data and generate messages
    dataframe = get_data()
    messages = format_message(dataframe=dataframe)

    # Send messages
    send_messages(
        token=token,
        group_id=group_id,
        messages=messages,
    )

cet_telegram_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cet_telegram_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cet_telegram_flow.schedule = bot_schedule

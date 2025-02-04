# -*- coding: utf-8 -*-
"""
Flows for setting rain data in Redis.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow  # pylint: disable=E0611, E0401

# pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)

from pipelines.constants import constants
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)

# from pipelines.rj_escritorio.rain_dashboard.schedules import every_fifteen_minutes
from pipelines.rj_escritorio.rain_dashboard.tasks import (
    dataframe_to_dict,
    get_data,
    set_redis_key,
)

with Flow(
    name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
    state_handlers=[
        handler_initialize_sentry,
        handler_inject_bd_credentials,
    ],
    # skip_if_running=True,
) as rj_escritorio_rain_dashboard_flow:
    # Parameters
    query_data = Parameter("query_data")
    query_update = Parameter("query_update")
    redis_data_key = Parameter("redis_data_key", default="data_last_15min_rain")
    redis_update_key = Parameter("redis_update_key", default="data_last_15min_rain_update")

    # Tasks
    dataframe = get_data(query=query_data)
    dataframe_update = get_data(query=query_update)
    dictionary = dataframe_to_dict(dataframe=dataframe)
    dictionary_update = dataframe_to_dict(dataframe=dataframe_update)
    set_redis_key(
        key=redis_data_key,
        value=dictionary,
    )
    set_redis_key(
        key=redis_update_key,
        value=dictionary_update,
    )


rj_escritorio_rain_dashboard_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
rj_escritorio_rain_dashboard_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
# rj_escritorio_rain_dashboard_flow.schedule = every_fifteen_minutes

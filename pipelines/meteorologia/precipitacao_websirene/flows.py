# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for precipitacao_alertario
"""
from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.precipitacao_websirene.schedules import MINUTE_SCHEDULE
from pipelines.meteorologia.precipitacao_websirene.tasks import (
    download_dados,
    salvar_dados,
    tratar_dados,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    "COR: Meteorologia - Precipitacao WEBSIRENE",
    state_handlers=[handler_inject_bd_credentials],
) as cor_meteorologia_precipitacao_websirene:
    DATASET_ID = "clima_pluviometro"
    TABLE_ID = "taxa_precipitacao_websirene"
    DUMP_MODE = "append"

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=False, required=False)
    MATERIALIZE_TO_DATARIO = Parameter("materialize_to_datario", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    dataframe = download_dados()
    dataframe, empty_data = tratar_dados(
        dfr=dataframe,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        mode=MATERIALIZATION_MODE,
    )

    with case(empty_data, False):
        PATH = salvar_dados(dfr=dataframe)

        # Create table in BigQuery
        UPLOAD_TABLE = create_table_and_upload_to_gcs(
            data_path=PATH,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            dump_mode=DUMP_MODE,
            wait=PATH,
        )

        # Trigger DBT flow run
        with case(MATERIALIZE_AFTER_DUMP, True):
            run_dbt = task_run_dbt_model_task(
                dataset_id=DATASET_ID,
                table_id=TABLE_ID,
                # mode=materialization_mode,
                # materialize_to_datario=materialize_to_datario,
            )

# para rodar na cloud
cor_meteorologia_precipitacao_websirene.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_websirene.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_precipitacao_websirene.schedule = MINUTE_SCHEDULE

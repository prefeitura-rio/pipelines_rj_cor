# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1120
"""
Flows for meteorologia_inmet
"""

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.meteorologia_inmet.schedules import hour_schedule
from pipelines.meteorologia.meteorologia_inmet.tasks import (  # slice_data,
    download,
    get_dates,
    salvar_dados,
    tratar_dados,
)
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import create_table_and_upload_to_gcs

with Flow(
    name="COR: Meteorologia - Meteorologia INMET",
    state_handlers=[handler_inject_bd_credentials],
) as cor_meteorologia_meteorologia_inmet:
    DATASET_ID = "clima_estacao_meteorologica"
    TABLE_ID = "meteorologia_inmet"
    DUMP_MODE = "append"

    # data_inicio e data_fim devem ser strings no formato "YYYY-MM-DD"
    data_inicio = Parameter("data_inicio", default="", required=False)
    data_fim = Parameter("data_fim", default="", required=False)

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    data_inicio_, data_fim_, backfill = get_dates(data_inicio, data_fim)
    # data = slice_data(current_time=CURRENT_TIME)
    dados = download(data_inicio_, data_fim_)
    dados = tratar_dados(dados, backfill)
    PATH = salvar_dados(dados=dados)

    # Create table in BigQuery
    UPLOAD_TABLE = create_table_and_upload_to_gcs(
        data_path=PATH,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        wait=PATH,
    )

    with case(MATERIALIZE_AFTER_DUMP, True):
        run_dbt = task_run_dbt_model_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            mode=MATERIALIZATION_MODE,
        )

# para rodar na cloud
cor_meteorologia_meteorologia_inmet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_inmet.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_inmet.schedule = hour_schedule

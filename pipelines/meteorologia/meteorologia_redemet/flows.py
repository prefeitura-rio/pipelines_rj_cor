# -*- coding: utf-8 -*-
# pylint: disable=C0103, E1120
"""
Flows for meteorologia_redemet
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.meteorologia_redemet.schedules import (
    hour_schedule,
    month_schedule,
)
from pipelines.meteorologia.meteorologia_redemet.tasks import (
    check_for_new_stations,
    download_data,
    download_stations_data,
    get_dates,
    save_data,
    treat_data,
    treat_stations_data,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import (
    create_table_and_upload_to_gcs,
    get_current_flow_labels,
)

with Flow(
    name="COR: Meteorologia - Meteorologia REDEMET",
    state_handlers=[handler_inject_bd_credentials],
) as cor_meteorologia_meteorologia_redemet:
    DUMP_MODE = Parameter("dump_mode", default="append", required=True)
    DATASET_ID = Parameter("dataset_id", default="clima_estacao_meteorologica", required=True)
    TABLE_ID = Parameter("table_id", default="meteorologia_redemet", required=True)

    # first_date and last_date must be strings as "YYYY-MM-DD"
    first_date = Parameter("first_date", default=None, required=False)
    last_date = Parameter("last_date", default=None, required=False)

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=True, required=False)
    MATERIALIZE_TO_DATARIO = Parameter("materialize_to_datario", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="prod", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    first_date_, last_date_, backfill = get_dates(first_date, last_date)
    # data = slice_data(current_time=CURRENT_TIME)
    dataframe = download_data(first_date_, last_date_)
    dataframe = treat_data(dataframe, backfill)
    PATH = save_data(dataframe=dataframe)

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

        # with case(DUMP_TO_GCS, True):
        #     # Trigger Dump to GCS flow run with project id as datario
        #     dump_to_gcs_flow = create_flow_run(
        #         flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
        #         project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        #         parameters={
        #             "project_id": "datario",
        #             "dataset_id": DATASET_ID,
        #             "table_id": TABLE_ID,
        #             "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
        #         },
        #         labels=[
        #             "datario",
        #         ],
        #         run_name=f"Dump to GCS {DATASET_ID}.{TABLE_ID}",
        #     )
        #     dump_to_gcs_flow.set_upstream(wait_for_materialization)
        #     wait_for_dump_to_gcs = wait_for_flow_run(
        #         dump_to_gcs_flow,
        #         stream_states=True,
        #         stream_logs=True,
        #         raise_final_state=True,
        #     )


cor_meteorologia_meteorologia_redemet.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_redemet.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_redemet.schedule = hour_schedule


with Flow(
    name="COR: Meteorologia - Meteorologia REDEMET - Atualização das estações",
    code_owners=[
        "karinappassos",
        "paty",
    ],
) as cor_meteorologia_meteorologia_redemet_estacoes:
    DUMP_MODE = Parameter("dump_mode", default="overwrite", required=True)
    DATASET_ID = Parameter("dataset_id", default="clima_estacao_meteorologica", required=True)
    TABLE_ID = Parameter("table_id", default="estacoes_redemet", required=True)

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=False, required=False)
    MATERIALIZE_TO_DATARIO = Parameter("materialize_to_datario", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    dataframe = download_stations_data()
    dataframe = treat_stations_data(dataframe)
    path = save_data(dataframe=dataframe, partition_column="data_atualizacao")

    # Create table in BigQuery
    UPLOAD_TABLE = create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=DATASET_ID,
        table_id=TABLE_ID,
        dump_mode=DUMP_MODE,
        wait=path,
    )

    # Trigger DBT flow run
    with case(MATERIALIZE_AFTER_DUMP, True):
        run_dbt = task_run_dbt_model_task(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            # mode=materialization_mode,
            # materialize_to_datario=materialize_to_datario,
        )

        # with case(DUMP_TO_GCS, True):
        #     # Trigger Dump to GCS flow run with project id as datario
        #     dump_to_gcs_flow = create_flow_run(
        #         flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
        #         project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        #         parameters={
        #             "project_id": "datario",
        #             "dataset_id": DATASET_ID,
        #             "table_id": TABLE_ID,
        #             "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
        #         },
        #         labels=[
        #             "datario",
        #         ],
        #         run_name=f"Dump to GCS {DATASET_ID}.{TABLE_ID}",
        #     )
        #     dump_to_gcs_flow.set_upstream(wait_for_materialization)
        #     wait_for_dump_to_gcs = wait_for_flow_run(
        #         dump_to_gcs_flow,
        #         stream_states=True,
        #         stream_logs=True,
        #         raise_final_state=True,
        #     )

    check_for_new_stations(dataframe, wait=UPLOAD_TABLE)

# para rodar na cloud
cor_meteorologia_meteorologia_redemet_estacoes.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_meteorologia_redemet_estacoes.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_meteorologia_redemet_estacoes.schedule = month_schedule

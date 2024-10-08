# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for precipitacao_inea.
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.precipitacao_inea.schedules import minute_schedule
from pipelines.meteorologia.precipitacao_inea.tasks import (
    check_for_new_stations,
    check_new_data,
    download_data,
    save_data,
    treat_data,
    wait_task,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.custom import wait_for_flow_run_with_timeout
from pipelines.utils.decorators import Flow
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants
from pipelines.utils.tasks import create_table_and_upload_to_gcs

wait_for_flow_run_with_2min_timeout = wait_for_flow_run_with_timeout(timeout=timedelta(minutes=2))

with Flow(
    name="COR: Meteorologia - Precipitacao e Fluviometria INEA",
    state_handlers=[handler_inject_bd_credentials],
    # skip_if_running=True,
) as cor_meteorologia_precipitacao_inea:
    DUMP_MODE = Parameter("dump_mode", default="append", required=True)
    DATASET_ID_PLUVIOMETRIC = Parameter(
        "dataset_id_pluviometric", default="clima_pluviometro", required=True
    )
    TABLE_ID_PLUVIOMETRIC = Parameter(
        "table_id_pluviometric", default="taxa_precipitacao_inea", required=True
    )
    DATASET_ID_FLUVIOMETRIC = Parameter(
        "dataset_id_fluviometric", default="clima_fluviometro", required=True
    )
    TABLE_ID_FLUVIOMETRIC = Parameter(
        "table_id_fluviometric", default="lamina_agua_inea", required=True
    )

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

    dataframe = download_data()
    dfr_pluviometric, dfr_fluviometric = treat_data(
        dataframe=dataframe,
        dataset_id=DATASET_ID_PLUVIOMETRIC,
        table_id=TABLE_ID_PLUVIOMETRIC,
        mode=MATERIALIZATION_MODE,
    )
    new_pluviometric_data, new_fluviometric_data = check_new_data(
        dfr_pluviometric, dfr_fluviometric
    )

    with case(new_pluviometric_data, True):
        path_pluviometric = save_data(dataframe=dfr_pluviometric, folder_name="pluviometer")

        # Create pluviometric table in BigQuery
        UPLOAD_TABLE_PLUVIOMETRIC = create_table_and_upload_to_gcs(
            data_path=path_pluviometric,
            dataset_id=DATASET_ID_PLUVIOMETRIC,
            table_id=TABLE_ID_PLUVIOMETRIC,
            dump_mode=DUMP_MODE,
            wait=path_pluviometric,
        )

        # Trigger pluviometric DBT flow run
        with case(MATERIALIZE_AFTER_DUMP, True):
            run_dbt = task_run_dbt_model_task(
                dataset_id=DATASET_ID_PLUVIOMETRIC,
                table_id=TABLE_ID_PLUVIOMETRIC,
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
            #             "dataset_id": DATASET_ID_PLUVIOMETRIC,
            #             "table_id": TABLE_ID_PLUVIOMETRIC,
            #             "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
            #         },
            #         labels=[
            #             "datario",
            #         ],
            #         run_name=f"Dump to GCS {DATASET_ID_PLUVIOMETRIC}.{TABLE_ID_PLUVIOMETRIC}",
            #     )
            #     dump_to_gcs_flow.set_upstream(wait_for_materialization)
            #     wait_for_dump_to_gcs = wait_for_flow_run_with_2min_timeout(
            #         flow_run_id=dump_to_gcs_flow,
            #         stream_states=True,
            #         stream_logs=True,
            #         raise_final_state=True,
            #     )

    status = wait_task()
    status.set_upstream(UPLOAD_TABLE_PLUVIOMETRIC)
    with case(new_fluviometric_data, True):
        path_fluviometric = save_data(dataframe=dfr_fluviometric, folder_name="fluviometer")
        path_fluviometric.set_upstream(status)

        # Create fluviometric table in BigQuery
        UPLOAD_TABLE_FLUVIOMETRIC = create_table_and_upload_to_gcs(
            data_path=path_fluviometric,
            dataset_id=DATASET_ID_FLUVIOMETRIC,
            table_id=TABLE_ID_FLUVIOMETRIC,
            dump_mode=DUMP_MODE,
            wait=path_fluviometric,
        )

        # Trigger DBT flow run
        with case(MATERIALIZE_AFTER_DUMP, True):
            run_dbt = task_run_dbt_model_task(
                dataset_id=DATASET_ID_PLUVIOMETRIC,
                table_id=TABLE_ID_PLUVIOMETRIC,
                # mode=materialization_mode,
                # materialize_to_datario=materialize_to_datario,
            )

            # with case(DUMP_TO_GCS, True):
            #    # Trigger Dump to GCS flow run with project id as datario
            #    dump_to_gcs_flow = create_flow_run(
            #        flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
            #        project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            #        parameters={
            #            "project_id": "datario",
            #            "dataset_id": DATASET_ID_FLUVIOMETRIC,
            #            "table_id": TABLE_ID_FLUVIOMETRIC,
            #            "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
            #        },
            #        labels=[
            #            "datario",
            #        ],
            #        run_name=f"Dump to GCS {DATASET_ID_FLUVIOMETRIC}.{TABLE_ID_FLUVIOMETRIC}",
            #    )
            #    dump_to_gcs_flow.set_upstream(wait_for_materialization)
    #
    #    wait_for_dump_to_gcs = wait_for_flow_run_with_2min_timeout(
    #        flow_run_id=dump_to_gcs_flow,
    #        stream_states=True,
    #        stream_logs=True,
    #        raise_final_state=True,
    #    )
    #
    check_for_new_stations(dataframe, wait=UPLOAD_TABLE_PLUVIOMETRIC)

# para rodar na cloud
cor_meteorologia_precipitacao_inea.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_inea.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_precipitacao_inea.schedule = minute_schedule

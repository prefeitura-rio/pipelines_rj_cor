# -*- coding: utf-8 -*-
# pylint: disable=C0103
"""
Flows for precipitacao_alertario.
"""
from datetime import timedelta

from prefect import Parameter, case
from prefect.executors import LocalDaskExecutor
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow  # pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    create_table_and_upload_to_gcs,
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.precipitacao_alertario.constants import (
    constants as alertario_constants,
)
from pipelines.meteorologia.precipitacao_alertario.schedules import minute_schedule
from pipelines.meteorologia.precipitacao_alertario.tasks import (
    check_to_run_dbt,
    download_data,
    save_data,
    save_last_dbt_update,
    treat_pluviometer_and_meteorological_data,
)
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)
from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.custom import wait_for_flow_run_with_timeout
from pipelines.utils.dump_db.constants import constants as dump_db_constants
from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants

wait_for_flow_run_with_5min_timeout = wait_for_flow_run_with_timeout(timeout=timedelta(minutes=5))

with Flow(
    name="COR: Meteorologia - Precipitacao e Meteorologia ALERTARIO",
    state_handlers=[handler_inject_bd_credentials],
) as cor_meteorologia_precipitacao_alertario:
    DATASET_ID_PLUVIOMETRIC = alertario_constants.DATASET_ID_PLUVIOMETRIC.value
    TABLE_ID_PLUVIOMETRIC = alertario_constants.TABLE_ID_PLUVIOMETRIC.value
    TABLE_ID_PLUVIOMETRIC_OLD_API = alertario_constants.TABLE_ID_PLUVIOMETRIC_OLD_API.value
    DATASET_ID_METEOROLOGICAL = alertario_constants.DATASET_ID_METEOROLOGICAL.value
    TABLE_ID_METEOROLOGICAL = alertario_constants.TABLE_ID_METEOROLOGICAL.value
    DUMP_MODE = "append"

    # Materialization parameters
    MATERIALIZE_AFTER_DUMP_OLD_API = Parameter(
        "materialize_after_dump_old_api", default=False, required=False
    )
    MATERIALIZE_TO_DATARIO_OLD_API = Parameter(
        "materialize_to_datario_old_api", default=False, required=False
    )
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=False, required=False)
    MATERIALIZE_TO_DATARIO = Parameter("materialize_to_datario", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=False, required=False
    )
    PREFECT_PROJECT = Parameter("Prefect_project", default="staging", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    MAXIMUM_BYTES_PROCESSED = Parameter(
        "maximum_bytes_processed",
        required=False,
        default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    )

    dfr_pluviometric, dfr_meteorological = download_data()
    (dfr_pluviometric, empty_data_pluviometric,) = treat_pluviometer_and_meteorological_data(
        dfr=dfr_pluviometric,
        dataset_id=DATASET_ID_PLUVIOMETRIC,
        table_id=TABLE_ID_PLUVIOMETRIC,
        mode=MATERIALIZATION_MODE,
    )
    (dfr_meteorological, empty_data_meteorological,) = treat_pluviometer_and_meteorological_data(
        dfr=dfr_meteorological,
        dataset_id=DATASET_ID_METEOROLOGICAL,
        table_id=TABLE_ID_METEOROLOGICAL,
        mode=MATERIALIZATION_MODE,
    )

    with case(empty_data_pluviometric, False):
        path_pluviometric = save_data(
            dfr_pluviometric, "pluviometric", wait=empty_data_pluviometric
        )
        # Create table in BigQuery
        UPLOAD_TABLE = create_table_and_upload_to_gcs(
            data_path=path_pluviometric,
            dataset_id=DATASET_ID_PLUVIOMETRIC,
            table_id=TABLE_ID_PLUVIOMETRIC,
            dump_mode=DUMP_MODE,
        )

        with case(TRIGGER_RAIN_DASHBOARD_UPDATE, True):
            # Trigger rain dashboard update flow run
            rain_dashboard_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario flow)",  # noqa
            )
            rain_dashboard_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 30min flow run
            rain_dashboard_last_30min_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_30MIN_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 30min flow)",  # noqa
            )
            rain_dashboard_last_30min_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_30min_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_30min_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 60min flow run
            rain_dashboard_last_60min_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_60MIN_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 60min flow)",  # noqa
            )
            rain_dashboard_last_60min_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_60min_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_60min_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 2h flow run
            rain_dashboard_last_2h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_2H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 2h flow)",  # noqa
            )
            rain_dashboard_last_2h_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_2h_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_2h_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 3h flow run
            rain_dashboard_last_3h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_3H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 3h flow)",  # noqa
            )
            rain_dashboard_last_3h_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_3h_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_3h_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 6h flow run
            rain_dashboard_last_6h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_6H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 6h flow)",  # noqa
            )
            rain_dashboard_last_6h_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_6h_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_6h_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 12h flow run
            rain_dashboard_last_12h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_12H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 12h flow)",  # noqa
            )
            rain_dashboard_last_12h_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_12h_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_12h_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 24h flow run
            rain_dashboard_last_24h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_24H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 24h flow)",  # noqa
            )
            rain_dashboard_last_24h_update_flow.set_upstream(UPLOAD_TABLE)

            wait_for_rain_dashboard_last_24h_update = wait_for_flow_run(
                flow_run_id=rain_dashboard_last_24h_update_flow,
                stream_states=True,
                stream_logs=True,
                raise_final_state=False,
            )

            # Trigger rain dashboard update last 96h flow run
            rain_dashboard_last_96h_update_flow = create_flow_run(
                flow_name=rain_dashboard_constants.RAIN_DASHBOARD_FLOW_NAME.value,
                project_name=PREFECT_PROJECT,
                parameters=alertario_constants.RAIN_DASHBOARD_LAST_96H_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    "rj-escritorio-dev",
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 96h flow)",  # noqa
            )
            rain_dashboard_last_96h_update_flow.set_upstream(UPLOAD_TABLE)

        wait_for_rain_dashboard_last_96h_update = wait_for_flow_run(
            flow_run_id=rain_dashboard_last_96h_update_flow,
            stream_states=True,
            stream_logs=True,
            raise_final_state=False,
        )

    # Trigger DBT for new API
    check_2_run_dbt = check_to_run_dbt(
        dataset_id=DATASET_ID_PLUVIOMETRIC,
        table_id=TABLE_ID_PLUVIOMETRIC,
        mode=MATERIALIZATION_MODE,
    )
    check_2_run_dbt.set_upstream(UPLOAD_TABLE)

    with case(check_2_run_dbt, True):
        # Trigger DBT flow run
        with case(MATERIALIZE_AFTER_DUMP, True):
            run_dbt = task_run_dbt_model_task(
                dataset_id=DATASET_ID_PLUVIOMETRIC,
                table_id=TABLE_ID_PLUVIOMETRIC,
                # mode=materialization_mode,
                # materialize_to_datario=materialize_to_datario,
            )
            run_dbt.set_upstream(check_2_run_dbt)
            last_dbt_update = save_last_dbt_update(
                dataset_id=DATASET_ID_PLUVIOMETRIC,
                table_id=TABLE_ID_PLUVIOMETRIC,
                mode=MATERIALIZATION_MODE,
                wait=run_dbt,
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

            #     wait_for_dump_to_gcs = wait_for_flow_run_with_5min_timeout(
            #         flow_run_id=dump_to_gcs_flow,
            #         stream_states=True,
            #         stream_logs=True,
            #         raise_final_state=True,
            #     )

    # Save and materialize meteorological data
    with case(empty_data_meteorological, False):
        path_meteorological = save_data(
            dfr_meteorological, "meteorological", wait=empty_data_meteorological
        )
        # Create table in BigQuery
        UPLOAD_TABLE_METEOROLOGICAL = create_table_and_upload_to_gcs(
            data_path=path_meteorological,
            dataset_id=DATASET_ID_METEOROLOGICAL,
            table_id=TABLE_ID_METEOROLOGICAL,
            dump_mode=DUMP_MODE,
        )

        with case(MATERIALIZE_AFTER_DUMP, True):
            run_dbt_meterological = task_run_dbt_model_task(
                dataset_id=DATASET_ID_METEOROLOGICAL,
                table_id=TABLE_ID_METEOROLOGICAL,
                # mode=materialization_mode,
                # materialize_to_datario=materialize_to_datario,
            )
            run_dbt.set_upstream(UPLOAD_TABLE_METEOROLOGICAL)

            # with case(DUMP_TO_GCS, True):
            #     # Trigger Dump to GCS flow run with project id as datario
            #     dump_to_gcs_flow = create_flow_run(
            #         flow_name=utils_constants.FLOW_DUMP_TO_GCS_NAME.value,
            #         project_name=constants.PREFECT_DEFAULT_PROJECT.value,
            #         parameters={
            #             "project_id": "datario",
            #             "dataset_id": DATASET_ID_METEOROLOGICAL,
            #             "table_id": TABLE_ID_METEOROLOGICAL,
            #             "maximum_bytes_processed": MAXIMUM_BYTES_PROCESSED,
            #         },
            #         labels=[
            #             "datario",
            #         ],
            #         run_name=f"Dump to GCS {DATASET_ID_METEOROLOGICAL}.{TABLE_ID_METEOROLOGICAL}",
            #     )
            #     dump_to_gcs_flow.set_upstream(wait_for_materialization)

            #     wait_for_dump_to_gcs = wait_for_flow_run_with_5min_timeout(
            #         flow_run_id=dump_to_gcs_flow,
            #         stream_states=True,
            #         stream_logs=True,
            #         raise_final_state=True,
            #     )

# para rodar na cloud
cor_meteorologia_precipitacao_alertario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_alertario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
# cor_meteorologia_precipitacao_alertario.executor = LocalDaskExecutor(num_workers=10)
cor_meteorologia_precipitacao_alertario.schedule = minute_schedule

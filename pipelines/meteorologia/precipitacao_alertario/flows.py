# -*- coding: utf-8 -*-
# pylint: disable=C0103, line-too-long
"""
Flows for precipitacao_alertario.
"""
from datetime import timedelta
from threading import Thread

from prefect import Parameter, case  # pylint: disable=E0611, E0401
from prefect.run_configs import KubernetesRun  # pylint: disable=E0611, E0401
from prefect.storage import GCS  # pylint: disable=E0611, E0401
from prefect.tasks.prefect import (  # pylint: disable=E0611,E0401
    create_flow_run,
    wait_for_flow_run,
)
from prefeitura_rio.pipelines_utils.custom import Flow  # pylint: disable=E0611, E0401

# pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    create_table_and_upload_to_gcs,
    get_now_datetime,
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.precipitacao_alertario.constants import (
    constants as alertario_constants,
)
from pipelines.meteorologia.precipitacao_alertario.schedules import minute_schedule
from pipelines.meteorologia.precipitacao_alertario.tasks import (
    check_to_run_dbt,
    convert_sp_timezone_to_utc,
    download_data,
    save_data,
    save_last_dbt_update,
    treat_pluviometer_and_meteorological_data,
)
from pipelines.rj_escritorio.rain_dashboard.constants import (
    constants as rain_dashboard_constants,
)
from pipelines.tasks import task_create_partitions  # pylint: disable=E0611, E0401

# from pipelines.utils.constants import constants as utils_constants
from pipelines.utils.custom import wait_for_flow_run_with_timeout

# preprocessing imports
from pipelines.utils.gypscie.tasks import (  # pylint: disable=E0611, E0401
    access_api,
    add_caracterization_columns_on_dfr,
    convert_columns_type,
    download_datasets_from_gypscie,
    execute_dataflow_on_gypscie,
    get_dataflow_alertario_params,
    get_dataset_info,
    get_dataset_name_on_gypscie,
    get_dataset_processor_info,
    path_to_dfr,
    register_dataset_on_gypscie,
    rename_files,
    unzip_files,
)

# from pipelines.utils.dump_db.constants import constants as dump_db_constants
# from pipelines.utils.dump_to_gcs.constants import constants as dump_to_gcs_constants


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
    # MATERIALIZE_AFTER_DUMP_OLD_API = Parameter(
    #     "materialize_after_dump_old_api", default=False, required=False
    # )
    # MATERIALIZE_TO_DATARIO_OLD_API = Parameter(
    #     "materialize_to_datario_old_api", default=False, required=False
    # )
    MATERIALIZE_AFTER_DUMP = Parameter("materialize_after_dump", default=False, required=False)
    # MATERIALIZE_TO_DATARIO = Parameter("materialize_to_datario", default=False, required=False)
    MATERIALIZATION_MODE = Parameter("mode", default="dev", required=False)
    TRIGGER_RAIN_DASHBOARD_UPDATE = Parameter(
        "trigger_rain_dashboard_update", default=False, required=False
    )
    PREFECT_PROJECT = Parameter("prefect_project", default="staging", required=False)

    # Dump to GCS after? Should only dump to GCS if materializing to datario
    # DUMP_TO_GCS = Parameter("dump_to_gcs", default=False, required=False)

    # MAXIMUM_BYTES_PROCESSED = Parameter(
    #     "maximum_bytes_processed",
    #     required=False,
    #     default=dump_to_gcs_constants.MAX_BYTES_PROCESSED_PER_TABLE.value,
    # )

    # Preprocessing gypscie parameters
    preprocessing_gypscie = Parameter("preprocessing_gypscie", default=False, required=False)
    # Gypscie parameters
    workflow_id = Parameter("workflow_id", default=41, required=False)
    environment_id = Parameter("environment_id", default=1, required=False)
    domain_id = Parameter("domain_id", default=1, required=False)
    project_id = Parameter("project_id", default=1, required=False)
    # gypscie_project_name = Parameter("project_name", default="rionowcast_precipitation", required=False)  # noqa: E501
    # treatment_version = Parameter("treatment_version", default=1, required=False)

    # Gypscie processor parameters
    processor_name = Parameter("processor_name", default="etl_alertario22", required=False)
    dataset_processor_id = Parameter("dataset_processor_id", default=43, required=False)  # mudar

    load_data_function_id = Parameter("load_data_function_id", default=53, required=False)
    parse_date_time_function_id = Parameter(
        "parse_date_time_function_id", default=54, required=False
    )
    drop_duplicates_function_id = Parameter(
        "drop_duplicates_function_id", default=55, required=False
    )
    replace_inconsistent_values_function_id = Parameter(
        "replace_inconsistent_values_function_id", default=56, required=False
    )
    add_lat_lon_function_id = Parameter("add_lat_lon_function_id", default=57, required=False)
    save_data_function_id = Parameter("save_data_function_id", default=58, required=False)
    rain_gauge_metadata_path = Parameter("rain_gauge_metadata_path", default=227, required=False)

    # Parameters for saving data preprocessed on GCP
    dataset_id_previsao_chuva = Parameter(
        "dataset_id_previsao_chuva", default="clima_previsao_chuva", required=False
    )
    table_id_previsao_chuva = Parameter(
        "table_id_previsao_chuva", default="preprocessamento_pluviometro_alertario", required=False
    )

    # Dataset parameters
    station_type = Parameter("station_type", default="rain_gauge", required=False)
    source = Parameter("source", default="alertario", required=False)

    # Dataset path, if it was saved on ETL flow or it will be None
    # dataset_path = Parameter("dataset_path", default=None, required=False)  # dataset_path
    model_version = Parameter("model_version", default=1, required=False)

    #########################
    #  Start alertario flow #
    #########################
    # timeout_flow(timeout_seconds=300)
    # Inicia o monitoramento em um novo thread
    # monitor_thread = Thread(
    #     target=monitor_flow, args=(300, cor_meteorologia_precipitacao_alertario)
    # )
    # monitor_thread.start()
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
        path_pluviometric, full_path_pluviometric = save_data(
            dfr_pluviometric,
            data_name="pluviometric",
            # treatment_version=treatment_version,
            wait=empty_data_pluviometric,
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
                parameters=alertario_constants.RAIN_DASHBOARD_FLOW_SCHEDULE_PARAMETERS.value,  # noqa
                labels=[
                    constants.RJ_COR_AGENT_LABEL.value,
                ],
                run_name="Update rain dashboard data (triggered by precipitacao_alertario last 15min flow)",  # noqa
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                    constants.RJ_COR_AGENT_LABEL.value,
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
                labels=[constants.RJ_COR_AGENT_LABEL.value],
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
        path_meteorological, full_path_meteorological = save_data(
            dfr_meteorological, data_name="meteorological", wait=empty_data_meteorological
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

    #####################################
    #  Start preprocessing gypscie flow #
    #####################################
    with case(empty_data_pluviometric, False):
        with case(preprocessing_gypscie, True):
            api = access_api()

            dataset_info = get_dataset_info(station_type, source)

            # Get processor information on gypscie
            with case(dataset_processor_id, None):
                dataset_processor_response, dataset_processor_id = get_dataset_processor_info(
                    api, processor_name
                )
            dfr_pluviometric_converted = convert_columns_type(
                dfr_pluviometric, columns=["id_estacao"], new_types=[int]
            )
            # dfr_pluviometric_gypscie = convert_sp_timezone_to_utc(dfr_pluviometric_converted)
            path_pluviometric_gypscie, full_path_pluviometric_gypscie = save_data(
                dfr_pluviometric_converted,
                data_name="gypscie",
                columns=["id_estacao", "data_medicao", "acumulado_chuva_5min"],
                data_type="parquet",
                suffix=False,
            )
            full_path_pluviometric_gypscie_ = rename_files(
                full_path_pluviometric_gypscie, rename="dados_alertario_raw"
            )
            register_dataset_response = register_dataset_on_gypscie(
                api, filepath=full_path_pluviometric_gypscie_[0], domain_id=domain_id
            )

            model_params = get_dataflow_alertario_params(
                workflow_id=workflow_id,
                environment_id=environment_id,
                project_id=project_id,
                rain_gauge_data_id=register_dataset_response["id"],
                rain_gauge_metadata_path=rain_gauge_metadata_path,
                load_data_function_id=load_data_function_id,
                parse_date_time_function_id=parse_date_time_function_id,
                drop_duplicates_function_id=drop_duplicates_function_id,
                replace_inconsistent_values_function_id=replace_inconsistent_values_function_id,
                add_lat_lon_function_id=add_lat_lon_function_id,
                save_data_function_id=save_data_function_id,
            )

            # Send dataset ids to gypscie to get predictions
            output_dataset_ids = execute_dataflow_on_gypscie(
                api,
                model_params,
            )

            # dataset_processor_task_id = execute_dataset_processor(
            #     api,
            #     processor_id=dataset_processor_id,
            #     dataset_id=[dataset_response["id"]],
            #     environment_id=environment_id,
            #     project_id=project_id,
            #     parameters=processor_parameters,
            # )
            # wait_run = task_wait_run(api, dataset_processor_task_id, flow_type="processor")
            # dataset_path = download_datasets_from_gypscie(
            #     api, dataset_names=[dataset_response["id"]], wait=wait_run
            # )
            dataset_names = get_dataset_name_on_gypscie(api, output_dataset_ids)  # new
            ziped_dataset_paths = download_datasets_from_gypscie(api, dataset_names=dataset_names)
            dataset_paths = unzip_files(ziped_dataset_paths)
            dfr_gypscie_ = path_to_dfr(dataset_paths)
            # output_datasets_id = get_output_dataset_ids_on_gypscie(api, dataset_processor_task_id)
            dfr_gypscie = add_caracterization_columns_on_dfr(
                dfr_gypscie_, model_version, update_time=True
            )

            # Save pre-treated data on local file with partitions
            now_datetime = get_now_datetime()
            prediction_data_path, prediction_data_full_path = task_create_partitions(
                data=dfr_gypscie,
                partition_date_column=dataset_info["partition_date_column"],
                savepath="model_prediction",
                suffix=now_datetime,
                wait=dfr_gypscie,
            )

            ################################
            #  Save preprocessing on GCP   #
            ################################

            # Upload data to BigQuery
            create_table = create_table_and_upload_to_gcs(
                data_path=prediction_data_path,
                dataset_id=dataset_id_previsao_chuva,
                table_id=table_id_previsao_chuva,
                dump_mode=DUMP_MODE,
                biglake_table=False,
            )

            # Trigger DBT flow run
            with case(MATERIALIZE_AFTER_DUMP, True):
                run_dbt = task_run_dbt_model_task(
                    dataset_id=dataset_id_previsao_chuva,
                    table_id=table_id_previsao_chuva,
                    # mode=materialization_mode,
                    # materialize_to_datario=materialize_to_datario,
                )
                run_dbt.set_upstream(create_table)

# para rodar na cloud
cor_meteorologia_precipitacao_alertario.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_precipitacao_alertario.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
# cor_meteorologia_precipitacao_alertario.executor = LocalDaskExecutor(num_workers=10)
cor_meteorologia_precipitacao_alertario.schedule = minute_schedule
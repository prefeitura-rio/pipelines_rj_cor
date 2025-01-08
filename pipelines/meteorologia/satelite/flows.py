# -*- coding: utf-8 -*-
# pylint: disable=C0103
# flake8: noqa: E501
"""
Flows for emd.
"""
from copy import deepcopy

from prefect import Parameter, case  # pylint: disable=E0611, E0401
from prefect.run_configs import KubernetesRun  # pylint: disable=E0611, E0401
from prefect.storage import GCS  # pylint: disable=E0611, E0401

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from prefeitura_rio.pipelines_utils.custom import Flow  # pylint: disable=E0611, E0401

# pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.state_handlers import (
    handler_initialize_sentry,
    handler_inject_bd_credentials,
)
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    create_table_and_upload_to_gcs,
    get_now_datetime,
    task_run_dbt_model_task,
)

from pipelines.constants import constants
from pipelines.meteorologia.satelite.schedules import (
    aod,
    cmip7,
    cmip9,
    cmip11,
    cmip13,
    cmip15,
    dsi,
    lst,
    mcmip,
    rrqpe,
    sst,
    tpw,
)

# from pipelines.utils.constants import constants as utils_constants
from pipelines.meteorologia.satelite.tasks import (  # create_image,
    create_image,
    define_background,
    download,
    generate_point_value,
    get_dates,
    prepare_data_for_redis,
    rearange_dataframe,
    save_data,
    slice_data,
    tratar_dados,
)
from pipelines.tasks import (  # pylint: disable=E0611, E0401
    get_storage_destination,
    task_build_redis_hash,
    task_create_partitions,
    task_get_redis_client,
    task_get_redis_output,
    task_save_on_redis,
    upload_files_to_storage,
)
from pipelines.utils_rj_cor import sort_list_by_dict_key

with Flow(
    name="COR: Meteorologia - Satelite GOES 16",
    state_handlers=[
        handler_initialize_sentry,
        handler_inject_bd_credentials,
    ],
    parallelism=10,
    skip_if_running=False,
) as cor_meteorologia_goes16:

    # Materialization parameters
    materialize_after_dump = Parameter("materialize_after_dump", default=False, required=False)
    # materialize_to_datario = Parameter("materialize_to_datario", default=False, required=False)
    # materialization_mode = Parameter("mode", default="dev", required=False)

    # Other parameters
    dataset_id = Parameter("dataset_id", default="clima_satelite", required=False)
    band = Parameter("band", default=None, required=False)()
    product = Parameter("product", default=None, required=False)()
    table_id = Parameter("table_id", default=None, required=False)()
    dump_mode = "append"
    mode_redis = Parameter("mode_redis", default="prod", required=False)
    ref_filename = Parameter("ref_filename", default=None, required=False)
    current_time = Parameter("current_time", default=None, required=False)
    # type_image_background can be "with" (with background), "without", "both" or None
    type_image_background = Parameter("type_image_background", default=None, required=False)
    create_point_value = Parameter("create_point_value", default=False, required=False)

    # Starting tasks
    current_time = get_dates(current_time, product)

    date_hour_info = slice_data(current_time=current_time, ref_filename=ref_filename)

    # # Get filenames that were already treated on redis
    # redis_files = get_on_redis(dataset_id, table_id, mode=mode_redis)
    redis_client = task_get_redis_client(infisical_secrets_path="/redis")
    redis_key = task_build_redis_hash(dataset_id, table_id, mode=mode_redis)
    redis_files = task_get_redis_output(redis_client, redis_key=redis_key)
    # redis_files = []

    # Download raw data from API
    filename, redis_files_updated = download(
        product=product,
        date_hour_info=date_hour_info,
        band=band,
        redis_files=redis_files,
        ref_filename=ref_filename,
        wait=redis_files,
        mode_redis=mode_redis,
    )

    # Start data treatment if there are new files
    info = tratar_dados(filename=filename)
    path, output_filepath = save_data(info=info, mode_redis=mode_redis)

    # Create table in BigQuery
    create_table = create_table_and_upload_to_gcs(
        data_path=path,
        dataset_id=dataset_id,
        table_id=table_id,
        dump_mode=dump_mode,
        biglake_table=False,
    )

    # Save new filenames on redis
    task_save_on_redis(
        redis_client=redis_client,
        values=redis_files_updated,
        redis_key=redis_key,
        keep_last=50,
        sort_key=lambda x: x.split("_s")[1].split("_")[0],
        wait=create_table,
    )

    dfr = rearange_dataframe(output_filepath)

    create_img_background, create_img_without_background = define_background(type_image_background)

    with case(create_img_background, True):
        save_image_paths_wb = create_image(info, dfr, "with")
        destination_folder_wb = get_storage_destination(
            path="cor-clima-imagens/satelite/goes16/with_background"
        )
        upload_files_to_storage(
            project="datario",
            bucket_name="datario-public",
            destination_folder=destination_folder_wb,
            source_file_names=save_image_paths_wb,
        )

    with case(create_img_without_background, True):
        save_image_paths_wtb = create_image(info, dfr, "without")
        destination_folder_wtb = get_storage_destination(
            path="cor-clima-imagens/satelite/goes16/without_background"
        )
        upload_files_to_storage(
            project="datario",
            bucket_name="datario-public",
            destination_folder=destination_folder_wtb,
            source_file_names=save_image_paths_wtb,
        )

    with case(create_point_value, True):
        now_datetime = get_now_datetime()
        df_point_values = generate_point_value(info, dfr)
        products_list_ = sorted([var.lower() for var in info["variable"]])
        point_values = task_get_redis_output.map(redis_client, redis_key=products_list_)
        products_list, point_values_updated = prepare_data_for_redis(
            df_point_values,
            products_list=products_list_,
            point_values=point_values,
        )
        # Save new points on redis
        task_save_on_redis.map(
            redis_client=redis_client,
            values=point_values_updated,
            redis_key=products_list,
            keep_last=12,
            sort_key=sort_list_by_dict_key,
            wait=point_values_updated,
        )
        point_values_path, point_values_full_path = task_create_partitions(
            df_point_values,
            partition_date_column="data_medicao",
            savepath="metricas_geoespaciais_goes16",
            suffix=now_datetime,
        )
        create_table_point_value = create_table_and_upload_to_gcs(
            data_path=point_values_path,
            dataset_id=dataset_id,
            table_id="metricas_geoespaciais_goes16",
            dump_mode=dump_mode,
            biglake_table=False,
        )

    # Trigger DBT flow run
    with case(materialize_after_dump, True):
        run_dbt = task_run_dbt_model_task(
            dataset_id=dataset_id,
            table_id=table_id,
            # mode=materialization_mode,
            # materialize_to_datario=materialize_to_datario,
        )
        run_dbt.set_upstream(create_table)

        run_dbt_point_value = task_run_dbt_model_task(
            dataset_id=dataset_id,
            table_id="metricas_geoespaciais_goes16",
            # mode=materialization_mode,
            # materialize_to_datario=materialize_to_datario,
        )
        run_dbt_point_value.set_upstream(create_table_point_value)

        # current_flow_labels = get_current_flow_labels()

        # materialization_flow = create_flow_run(
        #     flow_name=utils_constants.FLOW_EXECUTE_DBT_MODEL_NAME.value,
        #     project_name=constants.PREFECT_DEFAULT_PROJECT.value,
        #     parameters={
        #         "dataset_id": dataset_id,
        #         "table_id": table_id,
        #         "mode": materialization_mode,
        #         "materialize_to_datario": materialize_to_datario,
        #     },
        #     labels=current_flow_labels,
        #     run_name=f"Materialize {dataset_id}.{table_id}",
        # )

        # materialization_flow.set_upstream(upload_table)

        # wait_for_materialization = wait_for_flow_run(
        #     materialization_flow,
        #     stream_states=True,
        #     stream_logs=True,
        #     raise_final_state=True,
        # )


# para rodar na cloud
# cor_meteorologia_goes16.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
# cor_meteorologia_goes16.run_config = KubernetesRun(
#     image=constants.DOCKER_IMAGE.value,
#     labels=[constants.RJ_COR_AGENT_LABEL.value],
# )
cor_meteorologia_goes16_rrqpe = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_rrqpe.name = (
    "COR: Meteorologia - Satelite GOES 16 - RRQPE - Taxa de precipitação"
)
cor_meteorologia_goes16_rrqpe.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_rrqpe.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_rrqpe.schedule = rrqpe

cor_meteorologia_goes16_tpw = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_tpw.name = (
    "COR: Meteorologia - Satelite GOES 16 - TPW - Quantidade de água precipitável"
)
cor_meteorologia_goes16_tpw.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_tpw.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_tpw.schedule = tpw

cor_meteorologia_goes16_cmip = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip.name = (
    "COR: Meteorologia - Satelite GOES 16 - CMIP - Infravermelho longo banda 13"
)
cor_meteorologia_goes16_cmip.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip.schedule = cmip13

cor_meteorologia_goes16_mcmip = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_mcmip.name = (
    "COR: Meteorologia - Satelite GOES 16 - MCMIP - Nuvem e umidade"
)
cor_meteorologia_goes16_mcmip.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_mcmip.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_mcmip.schedule = mcmip

cor_meteorologia_goes16_dsi = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_dsi.name = (
    "COR: Meteorologia - Satelite GOES 16 - DSI - Índices de estabilidade da atmosfera"
)
cor_meteorologia_goes16_dsi.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_dsi.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_dsi.schedule = dsi

cor_meteorologia_goes16_lst = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_lst.name = (
    "COR: Meteorologia - Satelite GOES 16 - LST - Temperatura da superfície da terra"
)
cor_meteorologia_goes16_lst.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_lst.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_lst.schedule = lst

cor_meteorologia_goes16_sst = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_sst.name = (
    "COR: Meteorologia - Satelite GOES 16 - SST - Temperatura da superfície do oceano"
)
cor_meteorologia_goes16_sst.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_sst.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_sst.schedule = sst

cor_meteorologia_goes16_aod = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_aod.name = (
    "COR: Meteorologia - Satelite GOES 16 - AOD - Profundidade óptica aerossol"
)
cor_meteorologia_goes16_aod.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_aod.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_aod.schedule = aod

cor_meteorologia_goes16_cmip7 = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip7.name = (
    "COR: Meteorologia - Satelite GOES 16 - CMIP - Janela de ondas curtas banda 7"
)
cor_meteorologia_goes16_cmip7.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip7.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip7.schedule = cmip7

cor_meteorologia_goes16_cmip9 = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip9.name = (
    "COR: Meteorologia - Satelite GOES 16 - CMIP - Vapor d'água em níveis médios banda 9"
)
cor_meteorologia_goes16_cmip9.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip9.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip9.schedule = cmip9

cor_meteorologia_goes16_cmip11 = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip11.name = (
    "COR: Meteorologia - Satelite GOES 16 - CMIP - Fase do topo da nuvem banda 11"
)
cor_meteorologia_goes16_cmip11.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip11.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip11.schedule = cmip11

cor_meteorologia_goes16_cmip15 = deepcopy(cor_meteorologia_goes16)
cor_meteorologia_goes16_cmip15.name = (
    "COR: Meteorologia - Satelite GOES 16 - CMIP - Janela de ondas longas contaminada banda 15"
)
cor_meteorologia_goes16_cmip15.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_goes16_cmip15.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
)
cor_meteorologia_goes16_cmip15.schedule = cmip15

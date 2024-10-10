# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: disable=C0103, C0301
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import Parameter, case  # pylint: disable=E0611, E0401
from prefect.run_configs import KubernetesRun  # pylint: disable=E0611, E0401
from prefect.storage import GCS  # pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.custom import Flow  # pylint: disable=E0611, E0401
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials
from prefeitura_rio.pipelines_utils.tasks import (  # pylint: disable=E0611, E0401
    create_table_and_upload_to_gcs,
    get_now_datetime,
    task_run_dbt_model_task,
)

from pipelines.constants import constants  # pylint: disable=E0611, E0401
from pipelines.meteorologia.radar.mendanha.constants import (
    constants as radar_constants,  # pylint: disable=E0611, E0401
)

# from pipelines.tasks import task_get_redis_client
from pipelines.meteorologia.radar.mendanha.schedules import (  # pylint: disable=E0611, E0401
    TIME_SCHEDULE,
)
from pipelines.meteorologia.radar.mendanha.tasks import (  # pylint: disable=E0611, E0401; combine_radar_files,
    access_api,
    add_new_image,
    base64_to_bytes,
    compress_to_zip,
    create_visualization_no_background,
    create_visualization_with_background,
    download_files_storage,
    get_and_format_time,
    get_colorbar_title,
    get_filenames_storage,
    get_radar_parameters,
    get_storage_destination,
    img_to_base64,
    remap_data,
    rename_keys_redis,
    save_images_to_local,
    save_img_on_redis,
    send_zip_images_api,
    task_open_radar_file,
    upload_file_to_storage,
)

# from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs
# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run
from pipelines.tasks import (  # pylint: disable=E0611, E0401
    task_build_redis_hash,
    task_create_partitions,
    task_get_redis_client,
    task_get_redis_output,
    task_save_on_redis,
)

# preprocessing imports
from pipelines.utils.gypscie.tasks import (
    access_api as access_api_gypscie,  # pylint: disable=E0611, E0401
)
from pipelines.utils.gypscie.tasks import (
    add_columns_on_dfr,
    download_datasets_from_gypscie,
    execute_dataset_processor,
    get_dataset_info,
    get_dataset_processor_info,
    path_to_dfr,
    register_dataset_on_gypscie,
    task_wait_run,
)

# create_visualization_with_background, prefix_to_restore, save_data,
# from pipelines.utils_rj_cor import build_redis_hash  # pylint: disable=E0611, E0401


# from pipelines.utils.tasks import create_table_and_upload_to_gcs


with Flow(
    name="COR: Meteorologia - Mapa de Refletividade Radar do Mendanha",
    state_handlers=[handler_inject_bd_credentials],
    skip_if_running=False,
    parallelism=100,
    # skip_if_running=True,
) as cor_meteorologia_refletividade_radar_flow:

    # Prefect Parameters
    MODE = Parameter("mode", default="prod")
    RADAR_NAME = Parameter("radar_name", default="men")
    RADAR_PRODUCT_LIST = Parameter("radar_products_list", default=["reflectivity_horizontal"])

    DATASET_ID = Parameter("dataset_id", default=radar_constants.DATASET_ID.value)
    TABLE_ID = Parameter("table_id", default=radar_constants.TABLE_ID.value)
    SAVE_IMAGE_WITH_BACKGROUND = Parameter("save_image_with_background", default=False)
    SAVE_IMAGE_WITHOUT_BACKGROUND = Parameter("save_image_without_background", default=False)
    SAVE_IMAGE_WITH_COLORBAR = Parameter("save_image_with_colorbar", default=False)
    SAVE_IMAGE_WITHOUT_COLORBAR = Parameter("save_image_without_colorbar", default=False)
    DUMP_MODE = Parameter("dump_mode", default="append")
    # BASE_PATH = "pipelines/rj_cor/meteorologia/radar/precipitacao/"
    BUCKET_NAME = "rj-escritorio-scp"

    # Preprocessing gypscie parameters
    preprocessing_gypscie = Parameter("preprocessing_gypscie", default=False, required=False)
    # Gypscie parameters
    environment_id = Parameter("environment_id", default=1, required=False)
    domain_id = Parameter("domain_id", default=1, required=False)
    project_id = Parameter("project_id", default=1, required=False)
    project_name = Parameter("project_name", default="rionowcast_precipitation", required=False)

    # Gypscie processor parameters
    processor_name = Parameter("processor_name", default="etl_alertario22", required=True)
    dataset_processor_id = Parameter("dataset_processor_id", default=43, required=False)  # mudar

    # Parameters for saving data on GCP
    materialize_after_dump = Parameter("materialize_after_dump", default=False, required=False)
    dump_mode = Parameter("dump_mode", default=False, required=False)
    dataset_id_previsao_chuva = Parameter(
        "dataset_id_previsao_chuva", default="clima_previsao_chuva", required=False
    )
    table_id_previsao_chuva = Parameter(
        "table_id_previsao_chuva", default="preprocessamento_radar_mendanha", required=False
    )

    # Dataset parameters
    station_type = Parameter("station_type", default="radar", required=False)
    source = Parameter("source", default="mendanha", required=False)

    # Dataset path, if it was saved on ETL flow or it will be None
    dataset_path = Parameter("dataset_path", default=None, required=False)  # dataset_path
    model_version = Parameter("model_version", default=1, required=False)

    ############################
    #  Start radar flow        #
    ############################

    redis_client = task_get_redis_client(infisical_secrets_path="/redis")
    redis_hash = task_build_redis_hash(DATASET_ID, TABLE_ID, name="images", mode=MODE)
    redis_hash_processed = task_build_redis_hash(
        DATASET_ID, TABLE_ID, name="processed_images", mode=MODE
    )
    files_saved_redis = task_get_redis_output(redis_client, redis_key=redis_hash_processed)
    # files_saved_redis = get_on_redis(DATASET_ID, TABLE_ID, mode=MODE)
    files_on_storage_list, files_to_save_redis = get_filenames_storage(
        BUCKET_NAME, files_saved_redis=files_saved_redis
    )

    radar_files = download_files_storage(
        bucket_name=BUCKET_NAME,
        files_to_download=files_on_storage_list,
        destination_path="temp/",
    )
    radar = task_open_radar_file(radar_files[0])
    grid_shape, grid_limits = get_radar_parameters(radar)
    radar_2d = remap_data(radar, RADAR_PRODUCT_LIST, grid_shape, grid_limits)

    # Create visualizations
    formatted_time, filename_time = get_and_format_time(radar_files)
    cbar_title = get_colorbar_title(RADAR_PRODUCT_LIST[0])
    fig = create_visualization_no_background(
        radar_2d, radar_product=RADAR_PRODUCT_LIST[0], cbar_title=cbar_title, title=formatted_time
    )

    img_base64 = img_to_base64(fig)
    img_bytes = base64_to_bytes(img_base64)

    # update the name of images that are already on redis and save them as png
    img_base64_dict = rename_keys_redis(redis_hash, img_bytes)
    all_img_base64_dict = add_new_image(img_base64_dict, img_bytes)
    saved_images_path = save_images_to_local(all_img_base64_dict, folder="images")

    save_img_on_redis(
        redis_hash, "radar_020.png", img_bytes, saved_images_path
    )  # esperar baixar imagens que já estão no redis
    # save_img_on_redis.set_upstream(saved_images_path)

    zip_filename = compress_to_zip("/images.zip", saved_images_path)

    api = access_api()
    response = send_zip_images_api(api, "uploadfile", zip_filename)

    # save images to appear on COR integrated platform
    with case(SAVE_IMAGE_WITHOUT_BACKGROUND, True):
        with case(SAVE_IMAGE_WITH_COLORBAR, True):
            saved_last_img_path = save_images_to_local(
                {filename_time: img_bytes}, folder="last_image"
            )
            destination_blob_name, source_file_name = get_storage_destination(
                "cor-clima-imagens/radar/mendanha/refletividade_horizontal/without_background/with_colorbar",
                filename_time,
                saved_last_img_path,
            )
            upload_file_to_storage(
                project="datario",
                bucket_name="datario-public",
                destination_blob_name=destination_blob_name,
                source_file_name=source_file_name,
            )

        # save images to appear on Escritório de Dados climate platform
        with case(SAVE_IMAGE_WITHOUT_COLORBAR, True):
            fig_without_backgroud_colorbar = create_visualization_no_background(
                radar_2d,
                radar_product=RADAR_PRODUCT_LIST[0],
                cbar_title=None,
                title=None,
            )
            img_base64_without_backgroud_colorbar = img_to_base64(fig_without_backgroud_colorbar)
            img_bytes_without_backgroud_colorbar = base64_to_bytes(
                img_base64_without_backgroud_colorbar
            )
            saved_without_background_colorbar_img_path = save_images_to_local(
                {filename_time: img_bytes_without_backgroud_colorbar},
                folder="images_without_background_colorbar",
            )
            (
                destination_blob_name_without_backgroud_colorbar,
                source_file_name_without_backgroud_colorbar,
            ) = get_storage_destination(
                "cor-clima-imagens/radar/mendanha/refletividade_horizontal/without_background/without_colorbar",
                filename_time,
                saved_without_background_colorbar_img_path,
            )
            upload_file_to_storage(
                project="datario",
                bucket_name="datario-public",
                destination_blob_name=destination_blob_name_without_backgroud_colorbar,
                source_file_name=source_file_name_without_backgroud_colorbar,
            )

    with case(SAVE_IMAGE_WITH_BACKGROUND, True):
        fig_with_backgroud = create_visualization_with_background(
            radar_2d,
            radar_product=RADAR_PRODUCT_LIST[0],
            cbar_title=cbar_title,
            title=formatted_time,
        )
        img_base64_with_backgroud = img_to_base64(fig_with_backgroud)
        img_bytes_with_backgroud = base64_to_bytes(img_base64_with_backgroud)
        saved_with_background_img_path = save_images_to_local(
            {filename_time: img_bytes_with_backgroud}, folder="images_with_background"
        )
        (
            destination_blob_name_with_backgroud,
            source_file_name_with_backgroud,
        ) = get_storage_destination(
            "cor-clima-imagens/radar/mendanha/refletividade_horizontal/with_background/with_colorbar",
            filename_time,
            saved_with_background_img_path,
        )
        upload_file_to_storage(
            project="datario",
            bucket_name="datario-public",
            destination_blob_name=destination_blob_name_with_backgroud,
            source_file_name=source_file_name_with_backgroud,
        )

    # Save new filenames on redis
    save_last_update_redis = task_save_on_redis(
        redis_client=redis_client,
        values=files_to_save_redis,
        redis_key=redis_hash_processed,
        keep_last=30,
        wait=response,
    )
    # save_last_update_redis.set_upstream(upload_table)

    ####################################
    #  Start preprocessing flow        #
    ####################################

    with case(preprocessing_gypscie, True):
        api_gypscie = access_api_gypscie()

        dataset_info = get_dataset_info(station_type, source)

        # Get processor information on gypscie
        with case(dataset_processor_id, None):
            dataset_processor_response, dataset_processor_id = get_dataset_processor_info(
                api_gypscie, processor_name
            )
        # TODO: e se o radar_files tiver mais de um arquivo?
        dataset_response = register_dataset_on_gypscie(
            api_gypscie, filepath=radar_files, domain_id=domain_id
        )
        # TODO: verifcar no codigo do augustp se são esses os parametros corretos
        processor_parameters = {
            "dataset1": str(dataset_path).rsplit("/", maxsplit=1)[-1],
            "station_type": station_type,
        }

        dataset_processor_task_id = execute_dataset_processor(
            api_gypscie,
            processor_id=dataset_processor_id,
            dataset_id=[dataset_response["id"]],
            environment_id=environment_id,
            project_id=project_id,
            parameters=processor_parameters,
        )
        wait_run = task_wait_run(api_gypscie, dataset_processor_task_id, flow_type="processor")
        dataset_path = download_datasets_from_gypscie(
            api_gypscie, dataset_names=[dataset_response["id"]], wait=wait_run
        )
        dfr_ = path_to_dfr(dataset_path)
        # output_datasets_id = get_output_dataset_ids_on_gypscie(api, dataset_processor_task_id)
        dfr = add_columns_on_dfr(dfr_, model_version, update_time=True)

        # Save pre-treated data on local file with partitions
        now_datetime = get_now_datetime()
        prediction_data_path = task_create_partitions(
            dfr,
            partition_date_column=dataset_info["partition_date_column"],
            savepath="model_prediction",
            suffix=now_datetime,
        )
        ################################
        #  Save preprocessing on GCP   #
        ################################

        # Upload data to BigQuery
        create_table = create_table_and_upload_to_gcs(
            data_path=prediction_data_path,
            dataset_id=dataset_id_previsao_chuva,
            table_id=table_id_previsao_chuva,
            dump_mode=dump_mode,
            biglake_table=False,
        )

        # Trigger DBT flow run
        with case(materialize_after_dump, True):
            run_dbt = task_run_dbt_model_task(
                dataset_id=dataset_id_previsao_chuva,
                table_id=table_id_previsao_chuva,
                # mode=materialization_mode,
                # materialize_to_datario=materialize_to_datario,
            )
            run_dbt.set_upstream(create_table)


cor_meteorologia_refletividade_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_refletividade_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
    cpu_request=1,
    cpu_limit=1,
    memory_request="2Gi",
    memory_limit="3Gi",
)

cor_meteorologia_refletividade_radar_flow.schedule = TIME_SCHEDULE

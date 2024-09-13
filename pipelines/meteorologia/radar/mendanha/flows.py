# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: disable=C0103
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS

# sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../prefeitura-rio")))
# # sys.path.insert(0, '/home/patricia/Documentos/escritorio_dados/prefeitura-rio/prefeitura-rio')
# print("sys.path:", sys.path)
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

from pipelines.constants import constants

# from pipelines.tasks import task_get_redis_client
# from pipelines.meteorologia.radar.mendanha.schedules import TIME_SCHEDULE
from pipelines.meteorologia.radar.mendanha.constants import constants as radar_constants
from pipelines.meteorologia.radar.mendanha.tasks import (  # prefix_to_restore,; save_data,; create_visualization_with_background,; get_storage_destination,; upload_file_to_storage,; prefix_to_restore,; save_data,
    access_api,
    add_new_image,
    base64_to_bytes,
    combine_radar_files,
    compress_to_zip,
    create_visualization_no_background,
    download_files_storage,
    get_and_format_time,
    get_colorbar_title,
    get_filenames_storage,
    get_radar_parameters,
    img_to_base64,
    remap_data,
    rename_keys_redis,
    save_images_to_local,
    save_img_on_redis,
    send_zip_images_api,
)
from pipelines.utils_rj_cor import build_redis_key

# # Adiciona o diretório `/algum/diretorio/` ao sys.path
# import os, sys  # noqa


# from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run


# from pipelines.tasks import (
#     get_on_redis,
#     save_on_redis,
# )

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
    DUMP_MODE = Parameter("dump_mode", default="append")
    # BASE_PATH = "pipelines/rj_cor/meteorologia/radar/precipitacao/"
    BUCKET_NAME = "rj-escritorio-scp"

    # files_saved_redis = get_on_redis(DATASET_ID, TABLE_ID, mode=MODE)
    files_on_storage_list = get_filenames_storage(BUCKET_NAME, files_saved_redis=[])

    radar_files = download_files_storage(
        bucket_name=BUCKET_NAME,
        files_to_download=files_on_storage_list,
        destination_path="temp/",
    )
    combined_radar = combine_radar_files(radar_files)
    grid_shape, grid_limits = get_radar_parameters(combined_radar)
    radar_2d = remap_data(combined_radar, RADAR_PRODUCT_LIST, grid_shape, grid_limits)

    # Create visualizations
    formatted_time = get_and_format_time(radar_files)
    cbar_title = get_colorbar_title(RADAR_PRODUCT_LIST[0])
    fig = create_visualization_no_background(
        radar_2d, radar_product=RADAR_PRODUCT_LIST[0], cbar_title=cbar_title, title=formatted_time
    )

    img_base64 = img_to_base64(fig)
    img_bytes = base64_to_bytes(img_base64)

    # update the name of images that are already on redis and save them as png
    redis_hash = build_redis_key(DATASET_ID, TABLE_ID, name="images", mode=MODE)
    img_base64_dict = rename_keys_redis(redis_hash, img_bytes)
    all_img_base64_dict = add_new_image(img_base64_dict, img_bytes)
    saved_images_path = save_images_to_local(all_img_base64_dict, folder="images")

    save_img_on_redis(
        redis_hash, "radar_020.png", img_bytes
    )  # esperar baixar imagens que já estão no redis
    save_img_on_redis.set_upstream(saved_images_path)

    zip_filename = compress_to_zip("/images.zip", saved_images_path)

    api = access_api()
    send_zip_images_api(api, "uploadfile", zip_filename)
    access_api.set_upstream(saved_images_path)

    # fig_with_backgroud = create_visualization_with_background(
    #     radar_2d, radar_product=RADAR_PRODUCT_LIST[0], cbar_title=cbar_title, title=formatted_time
    # )
    # img_base64_with_backgroud = img_to_base64(fig_with_backgroud)
    # img_bytes_with_backgroud = base64_to_bytes(img_base64_with_backgroud)
    # saved_with_background_img_path = save_images_to_local(
    #     {formatted_time: img_bytes_with_backgroud}
    # )

    # destination_blob_name, source_file_name = get_storage_destination(
    #     formatted_time, saved_with_background_img_path
    # )
    # upload_file_to_storage(
    #     project="datario",
    #     bucket_name="datario-public",
    #     destination_blob_name=destination_blob_name,
    #     source_file_name=source_file_name,
    # )

    # # Save new filenames on redis
    # save_last_update_redis = save_on_redis(
    #     DATASET_ID,
    #     TABLE_ID,
    #     MODE,
    #     files_on_storage_list,
    #     keep_last=3,
    #     # wait=upload_table,
    # )
    # save_last_update_redis.set_upstream(upload_table)


cor_meteorologia_refletividade_radar_flow.storage = GCS(constants.GCS_FLOWS_BUCKET.value)
cor_meteorologia_refletividade_radar_flow.run_config = KubernetesRun(
    image=constants.DOCKER_IMAGE.value,
    labels=[constants.RJ_COR_AGENT_LABEL.value],
    cpu_request=1,
    cpu_limit=1,
    memory_request="2Gi",
    memory_limit="3Gi",
)

# cor_meteorologia_refletividade_radar_flow.schedule = TIME_SCHEDULE

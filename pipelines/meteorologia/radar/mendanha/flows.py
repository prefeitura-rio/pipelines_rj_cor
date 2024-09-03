# -*- coding: utf-8 -*-
# flake8: noqa: E501
# pylint: disable=C0103
"""
Flows for setting rain dashboard using radar data.
"""
from prefect import Parameter
from prefect.run_configs import KubernetesRun
from prefect.storage import GCS
from prefeitura_rio.pipelines_utils.custom import Flow
from prefeitura_rio.pipelines_utils.state_handlers import handler_inject_bd_credentials

# from prefeitura_rio.pipelines_utils.tasks import create_table_and_upload_to_gcs

# from prefect.tasks.prefect import create_flow_run, wait_for_flow_run

from pipelines.constants import constants

# from pipelines.meteorologia.radar.mendanha.schedules import TIME_SCHEDULE
from pipelines.meteorologia.radar.mendanha.constants import (
    constants as radar_constants,
)
from pipelines.meteorologia.radar.mendanha.tasks import (
    download_files_storage,
    get_filenames_storage,
    # save_data,
)

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

    # Other Parameters
    DATASET_ID = radar_constants.DATASET_ID.value
    TABLE_ID = radar_constants.TABLE_ID.value
    DUMP_MODE = "append"
    # BASE_PATH = "pipelines/rj_cor/meteorologia/radar/precipitacao/"

    # Tasks
    BUCKET_NAME = "rj-escritorio-scp"
    # files_saved_redis = get_on_redis(DATASET_ID, TABLE_ID, mode=MODE)
    files_on_storage_list = get_filenames_storage(BUCKET_NAME, files_saved_redis=[])

    download_files_task = download_files_storage(
        bucket_name=BUCKET_NAME,
        files_to_download=files_on_storage_list,
        # destination_path=f"{BASE_PATH}radar_data/",
        destination_path="temp/",
    )
    # change_json_task = change_predict_rain_specs(
    #     files_to_model=files_on_storage_list,
    #     destination_path=f"{BASE_PATH}radar_data/",
    # )
    # download_files_task.set_upstream(change_json_task)
    # dfr = run_model(wait=download_files_task)
    # # dfr.set_upstream(download_files_task)
    # # run_model_task.set_upstream(change_json_task)
    # save_data_path = save_data(dfr)
    # upload_table = create_table_and_upload_to_gcs(
    #     data_path=save_data_path,
    #     dataset_id=DATASET_ID,
    #     table_id=TABLE_ID,
    #     dump_mode=DUMP_MODE,
    # )
    # upload_table.set_upstream(run_model_task)

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

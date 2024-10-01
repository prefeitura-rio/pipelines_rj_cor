# -*- coding: utf-8 -*-
"""
Schedules to run all satelite products
"""
from datetime import datetime, timedelta

from prefect.schedules import Schedule
from prefect.schedules.clocks import IntervalClock

from pipelines.constants import constants

rrqpe = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "taxa_precipitacao_goes_16",
                "product": "RRQPEF",
                # "create_image": True,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
tpw = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "quantidade_agua_precipitavel_goes_16",
                "product": "TPWF",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
cmip13 = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "infravermelho_longo_banda_13_goes_16",
                "product": "CMIPF",
                "band": "13",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
mcmip = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "imagem_nuvem_umidade_goes_16",
                "product": "MCMIPF",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
dsi = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "indice_estabilidade_derivado_goes_16",
                "product": "DSIF",
                # "create_image": True,
                "create_point_value": True,
                "type_image_background": "both",
            },
        )
    ]
)
lst = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "temperatura_superficie_terra_goes_16",
                "product": "LSTF",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
sst = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "temperatura_superficie_oceano_goes_16",
                "product": "SSTF",
                # "create_image": False,
                "create_point_value": True,
                "type_image_background": "both",
            },
        )
    ]
)
aod = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "profundidade_optica_aerossol_goes_16",
                "product": "AODF",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
# Fonte nome das bandas do CMI
# https://journals.ametsoc.org/view/journals/aies/3/2/AIES-D-23-0065.1.xml#tbl1
cmip7 = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "janela_ondas_curtas_banda_7_goes_16",
                "product": "CMIPF",
                "band": "7",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
cmip9 = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "vapor_agua_niveis_medios_banda_9_goes_16",
                "product": "CMIPF",
                "band": "9",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
cmip11 = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "fase_topo_nuvem_banda_11_goes_16",
                "product": "CMIPF",
                "band": "11",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
cmip15 = Schedule(
    clocks=[
        IntervalClock(
            interval=timedelta(minutes=5),
            start_date=datetime(2021, 1, 1, 0, 4, 0),
            labels=[constants.RJ_COR_AGENT_LABEL.value],
            parameter_defaults={
                "materialize_after_dump": False,
                "materialize_to_datario": False,
                "mode": "prod",
                "mode_redis": "prod",
                "dataset_id": "clima_satelite",
                "table_id": "janela_ondas_longas_contaminada_banda_15_goes_16",
                "product": "CMIPF",
                "band": "15",
                # "create_image": False,
                "create_point_value": False,
                "type_image_background": "both",
            },
        )
    ]
)
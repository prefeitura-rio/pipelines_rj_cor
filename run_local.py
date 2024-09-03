# -*- coding: utf-8 -*-
# flake8: noqa: 402
import base64
from os import getenv
import sys
import os
from pathlib import Path

from dotenv import load_dotenv
from pipelines.meteorologia.radar.mendanha.flows import (
    cor_meteorologia_refletividade_radar_flow as flow,  # TODO: import your flow here
)


# Adiciona o diretório `/algum/diretorio/` ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../prefeitura-rio')))
from prefeitura_rio.pipelines_utils.custom import Flow  # noqa


def run_local(flow: Flow):
    load_dotenv()
    envs = [
        "INFISICAL_TOKEN",
        "INFISICAL_ADDRESS",
        "PREFECT__BACKEND",
        "PREFECT__CLOUD__API",
        "PREFECT_AUTH_TOML_B64",
    ]
    for env in envs:
        if not getenv(env):
            raise SystemError(f"Environment variable {env} not set.")

    prefect_dir = Path.home() / ".prefect"
    prefect_dir.mkdir(parents=True, exist_ok=True)

    with open(prefect_dir / "auth.toml", "w") as f:
        f.write(base64.b64decode(getenv("PREFECT_AUTH_TOML_B64")).decode())

    flow.storage = None
    flow.run_config = None
    flow.schedule = None
    flow.state_handlers = []

    # flow.run(parameters={
    #     "mode": "prod",
    #     "radar_name": "men"
    # }) # TODO: add your parameters here
    flow.run()


run_local(flow)

import base64
from os import getenv
from pathlib import Path

from pipelines.mapa_realizacoes.infopref.flows import (
    rj_escritorio__mapa_realizacoes__infopref__flow as flow, # TODO: import your flow here
)

from prefeitura_rio.pipelines_utils.custom import Flow


def run_local(flow: Flow):
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

    flow.run(parameters={"force_pass": True}) # TODO: add your parameters here
    # flow.run()


run_local(flow)
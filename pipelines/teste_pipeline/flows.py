# -*- coding: utf-8 -*-
"""
Flows for teste
"""

# Pegar dados do Infisical

from prefect.run_configs import LocalRun

from pipelines.teste_pipeline.tasks import get_fake_data, print_data, treat_fake_data
from pipelines.utils.decorators import Flow

with Flow(
    name="COR: Teste Pipeline",
) as teste_pipeline_flow:

    # Get fake data
    data = get_fake_data(index=1)

    # Treat fake data
    treated_data = treat_fake_data(data)

    # Print data
    print_data(treated_data)

teste_pipeline_flow.run_config = LocalRun()


if __name__ == "__main__":
    teste_pipeline_flow.run()

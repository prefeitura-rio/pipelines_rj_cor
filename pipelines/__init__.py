"""
Prefect flows for cor project
"""
from pipelines.bot_semaforo.flows import *
from pipelines.meteorologia.meteorologia_inmet.flows import *
from pipelines.meteorologia.meteorologia_redemet.flows import *
from pipelines.meteorologia.precipitacao_alertario.flows import *
from pipelines.meteorologia.precipitacao_cemaden.flows import *
from pipelines.meteorologia.precipitacao_inea.flows import *
from pipelines.meteorologia.satelite.flows import *
from pipelines.meteorologia.precipitacao_websirene.flows import *

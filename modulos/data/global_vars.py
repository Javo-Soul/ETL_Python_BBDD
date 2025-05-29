import pandas as pd
import datetime
from functools import wraps
import time
from datetime import datetime
## ------------- librerias personalizadas ------------ ##
from  modulos.databaseClient.client import conexionSQL
from modulos.logs.log_config import logger
## --------------- config ini ------------------------- ##
from modulos.config_loader import cargar_config
config = cargar_config()
## ---------------------------------------------------- ##
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()
fecha_actual = fecha_hoy.strftime("%d-%m-%Y %H:%M:%S")
## ----------------------------------------------- ##
conexiones    = conexionSQL()
# ----------------------------------------------- ##
tablasSQL = {
    'tablaTrans'   : config['sql']['tabla_trans'],
    'tabla_audit'  : config['sql']['tabla_audit'],
    'tabla_tefabm' : config['sql']['tabla_tefabm']
}

procSQL = {
    'proc_prod_trans'  : config['procedimientos']['proc_prod_trans'],
}

carpetacsv = {
  'carpetacsv'  : config['paths']['repo_csv']
}
## ----------------------------------------------- ##


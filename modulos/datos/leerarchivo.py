import pandas as pd
import datetime
from functools import wraps
import time
from datetime import datetime

## ------------- librerias personalizadas ------------ ##
from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas import log_tabla as log
from modulos.log_cargas.log_config import logger
from modulos.log_cargas.log_tabla import registroLOGTabla
## ---------------config ini ------------------------- ##
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
## ---------------------------------------------------- ##
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()
fecha_actual = fecha_hoy.strftime("%d-%m-%Y %H:%M:%S")
## ----------------------------------------------- ##
conexiones    = conexionSQL()
log_carga     = log.registroLOGTabla()
registroTabla = registroLOGTabla
# ----------------------------------------------- ##
tablasSQL = {
    'tablaTrans'   : config['sql']['tabla_trans'],
    'tabla_audit'  : config['sql']['tabla_audit'],
    'tabla_tefabm' : config['sql']['tabla_tefabm']
}

procSQL = {
    'proc_prod_trans'  : config['procedimientos']['proc_prod_trans'],
    'proc_prod_audit'  : config['procedimientos']['proc_prod_audit'],
    'proc_prod_tefabm' : config['procedimientos']['proc_prod_tefabm']
}

carpetacsv = {
  'carpetacsv'  : config['paths']['repo_csv']
}
## ----------------------------------------------- ##

######################################################################
def mide_tiempo(func):
  @wraps(func)

  def wrapper(*args, **kwargs):
    start_time   = time.time()
    result       = func(*args,**kwargs)
    end_time     = time.time()
    elapsed_time = end_time - start_time      
    print(f"tiempo de ejecucion de {func.__name__}: {elapsed_time:.2f} segundos","\n")
    return result
  return wrapper



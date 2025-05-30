import pandas as pd
import datetime
from functools import wraps
import time
from datetime import datetime
## ------------- librerias personalizadas ------------ ##
from  modulos.databaseClient.client import conexionSQL
from modulos.logs.log_config import logger
## ---------------------------------------------------- ##
fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
conexiones    = conexionSQL()
# ----------------------------------------------- ##


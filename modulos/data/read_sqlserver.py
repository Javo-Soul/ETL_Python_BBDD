import pandas as pd
from datetime import datetime
from sqlalchemy import Select,MetaData,Table

from modulos.repository.sql_repository import SQLRepository
from config.settings import settings
## ------------- librerias personalizadas ------------ ##
from .global_vars import conexiones,logger
## ----------------------------------------------- ##

######################################################################
class ClaseSQL:
  def __init__(self,repository: SQLRepository):
    self.repository = repository

######################################################################
  def readSqltable(self):
    try:
        fecha_actual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f'leyendo datos de leerBBDDaudit del dia {fecha_actual}')
        # ... lógica de lectura...
        df_tabla = pd.DataFrame() 
        engine   = conexiones.conexionSQLServer()

        with engine.connect() as conn:          
          metadata  = MetaData()
          tabla_sql = Table(settings.sql.db_tabla_sql, metadata, autoload_with=engine)
          query     = Select(tabla_sql).where(tabla_sql.c.OriginAirportID == 15304)
          result    = conn.execute(query)
          rows      = result.fetchall()

          df_tabla  = pd.DataFrame(rows, columns= result.keys())

        ### se le agrega una columna con la fecha de ejecucion
        df_tabla['fecha_ts'] = fecha_actual
        # # --------------------------------------------------#
        # succes, message = 
        
        self.repository.full_load_process(
            df=df_tabla,
            staging_table = "tabla x",
            target_proc   = {
                    'carga_data'    :"proc",
                }
            )

        # if not succes:
        #     logger.error(f'Error en Carga Audit: {message} ')
        
        # else:
        #     logger.info(f'proceso de carga audit terminado\n')
        
        return df_tabla
    except Exception as e:
      logger.error(f'leyendo datos de leerBBDDaudit : {e}', exc_info=True)

    ### ----------------------------------------- ###
    ##############################################################
    return

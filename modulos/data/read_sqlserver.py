
#modulos/data/read_sqlserver.py
import pandas as pd
from datetime import datetime
from sqlalchemy import Select,MetaData,Table

from modulos.repository.sql_repository import SQLRepository
from config.settings import settings
## ------------- librerias personalizadas ------------ ##
from .global_vars import conexiones,logger,fecha_actual
## ----------------------------------------------- ##

######################################################################
class ClaseSQL:
  def __init__(self,repository: SQLRepository,filtros:dict={}):
    self.repository = repository
    self.filtros = filtros

######################################################################
  def readSqltable(self):
    try:
        filtros_consulta = {}
        logger.info(f'leyendo datos de readSqltable del dia {fecha_actual}')
        # ... lógica de lectura...
        df_tabla = pd.DataFrame() 
        engine   = conexiones.conexionSQLServer()

        print('self.filtros :', self.filtros)

        with engine.connect() as conn:
          metadata  = MetaData()
          tabla_sql = Table(settings.sql.db_tabla_sql, metadata, autoload_with=engine)
          columnas  = [col.name for col in tabla_sql.columns]
          for key,valor in self.filtros.items():
            for col in columnas:
              if key == col:
                filtros_consulta.update({key:valor})

        print("filtros_consulta : ",filtros_consulta)  

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
            staging_table = settings.postgres.postgres_tabla,
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
      logger.error(f'leyendo datos de readSqltable : {e}', exc_info=True)

    ### ----------------------------------------- ###
    ##############################################################
    return

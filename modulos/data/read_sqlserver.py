#modulos/data/read_sqlserver.py
import pandas as pd
from modulos.repository.sql_repository import SQLRepository
## ------------- librerias personalizadas ------------ ##
from .global_vars import logger,fecha_actual,measure_time,enginepostgres,enginesql

# from services.data_sync_service import DataSyncService
## ----------------------------------------------- ##

######################################################################
class ClaseSQL:
  def __init__(self,repository: SQLRepository,tabla:str,filtros:dict={}):
    self.repository = repository
    self.filtros    = filtros
    self.tabla      = tabla

######################################################################
  @measure_time
  def readSqltable(self):
    try:
        logger.info(f'leyendo datos de readSqltable del dia {fecha_actual}')
        # ... lógica de lectura...
        df_tabla        = pd.DataFrame()
        ## se validan los filtros en base a las columnas existentes
        succes, msfiltros = self.repository.validate_fields(self.tabla
                                                            ,self.filtros)
        ## si las columnas existen, se extrae la data desde el origen
        if succes:
          with enginesql.connect() as conn:
            query     = msfiltros
            print(query)
            result    = conn.execute(query)
            rows      = result.fetchall()

            df_tabla  = pd.DataFrame(rows, columns= result.keys())
        #### se le agrega una columna con la fecha de ejecucion
            df_tabla['fecha_ts'] = fecha_actual
        # ### -------------------------------------------------- ###
            succes,message = self.repository.full_load_process(df_tabla
                                                               ,self.tabla
                                                               ,enginepostgres)

        # ## con el dataframe creado, se insertan los datos en el destino
        # ## es una funcion sync, pero aun falta por pulir
        # # succes, message = sync_service.sync_dataframe_to_table(df_tabla
        # #                                                         ,self.tabla
        # #                                                         ,pk_column="ID")

        # ### -------------------------------------------------- ###
        if not succes:
            logger.error(f'Error en readSqltable: {message} ')
        
        else:
            logger.info(f'proceso de readSqltable terminado')
        
        return df_tabla if not df_tabla.empty else pd.DataFrame()
    except Exception as e:
      logger.error(f'leyendo datos de readSqltable : {e}', exc_info=True)
        ### -------------------------------------------------- ###
    ##############################################################
    return

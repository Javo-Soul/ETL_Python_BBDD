#modulos/repository/repository.py
import os
import pandas as pd
from datetime import datetime
from modulos.logs.log_config import logger
from modulos.utils.utils import class_utils

## -------------------------------------------------- ##
measure_time = class_utils.measure_time
dict_gen     = class_utils.dict_generator

## -------------------------------------------------- ##
class SQLRepository:
    """
    Clase para manejar operaciones de carga y ejecución de procedimientos en la base de datos SQL.
    """
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool

## -------------------------------------------------- ##
    def get_connection(self):
        return self.connection_pool

## -------------------------------------------------- ##
    @measure_time
    def load_to_staging(self, df, table_name, if_exists='append', chunksize=10000):
        """
        Carga datos a una tabla de staging con opción de truncar primero.
        Además, extrae valores de columnas específicas del DataFrame.

        Args:
            df: DataFrame con los datos
            table_name: Nombre de la tabla destino
            truncate: Si True, trunca la tabla antes de cargar
            batch_size: Tamaño del lote para inserción
            extract_columns: Lista de columnas a extraer el primer valor (opcional)

        Returns:
            Tuple (bool, dict, str): Indicador de éxito, diccionario de resultados y mensaje
        """
        dict_result = {}

        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.warning("DataFrame vacío recibido para carga")
            return False, dict_result, "DataFrame vacío"

        try:
            # df.to_sql(table_name,con = self.get_connection()
            #           ,if_exists = if_exists
            #           ,index=False, method='multi', chunksize=chunksize )

            print(table_name,'\n',df.head())

            return True, f"{len(df)} filas insertadas en {table_name}"

        except Exception as e:
            logger.error(f"Error cargando datos en {table_name}: {str(e)}", exc_info=True)
            return False, str(e)
## -------------------------------------------------- ##

    @measure_time
    def execute_stored_procedure(self, proc_name, params=None):
        """
        Ejecuta un procedimiento almacenado
        Args:
            proc_name: Nombre del procedimiento
            params: Parámetros como diccionario {nombre: valor}
        Returns:
            Tuple (bool, str): Indicador de éxito y mensaje
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                # Construir llamada al procedimiento
                if params:
                    param_names = ",".join(f"@{k}=?" for k in params.keys())
                    query = f"EXEC {proc_name} {param_names}"
                    cursor.execute(query, list(params.values()))
                else:
                    cursor.execute(f"EXEC {proc_name}")

                conn.commit()
                return True, f"Procedimiento {proc_name}: Ejecutado correctamente"
                
        except Exception as e:
            logger.error(f"Error ejecutando {proc_name}: {str(e)}", exc_info=True)
            if 'conn' in locals():
                conn.rollback()
            return False, str(e)

## -------------------------------------------------- ##
    @measure_time
    def full_load_process(self, df, staging_table, target_proc:dict):
        """
        Proceso completo de carga: staging + procedimiento
        Args:
            df: DataFrame con datos
            staging_table: Tabla de staging
            target_proc: Procedimiento para consolidar datos
        Returns:
            Tuple (bool, str): Resultado combinado del proceso
        """
        ## Paso 1: Carga a staging
        try:
            #### Paso 1: Carga a staging
            success, msgLoad = self.load_to_staging(df
                                                    ,staging_table)
            # if not success:
            #     return False, f"Fallo carga staging: {msgLoad}"

        # ### Paso 2: Ejecutar procedimiento
        #     for proc in target_proc.values():
        #         success, msgProc = self.execute_stored_procedure(proc)

        #     if not success:
        #         return False, f"Fallo procedimiento: {msgProc}"

        #     return True, "Proceso completo exitoso"
        
        except Exception as e:
            logger.error("Fallo el proceso:",e, exc_info=True)


# repository.py
import os
import pandas as pd
from datetime import datetime
from modulos.logs.log_config import logger
from modulos.utils.utils import class_utils

## -------- archivo config ------------------ ##
from modulos.config_loader import cargar_config
config = cargar_config()
## ------------------------------------------ ##

measure_time = class_utils.measure_time
dict_gen     = class_utils.dict_generator

## -------------------------------------------------- ##
tablaLog = {
    'logTabla'  : config['sql']['tabla_log_tablas'],
    'logArchivo': config['sql']['tabla_log_archivos']
}

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
    def load_to_staging(self, df, table_name, truncate=True, batch_size=10000, extract_columns=None):
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
            dict_result.update({'resultado': 'DataFrame vacío recibido para carga'})
            return False, dict_result, "DataFrame vacío"

        try:
            ###### Si no se pasan columnas a extraer, usar una lista por defecto vacía
            extract_columns = extract_columns or []

            ###### Extraer valores de interés antes de cargar
            valores_extraidos = {col: (df[col].iloc[0] if col in df.columns else None) for col in extract_columns}
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            valores_extraidos.update({k: fecha_actual for k in ('fecha', 'fecha_query') if valores_extraidos.get(k) is None})

            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True

                # Truncar tabla si se solicita
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    logger.info(f"Tabla {table_name} truncada")

                # Preparar datos para insertar
                df = df.where(pd.notnull(df), None)  # Reemplazar NaN por None
                cols = ",".join([f"[{col}]" for col in df.columns])
                placeholders = ",".join(['?'] * len(df.columns))
                query = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

                # Insertar por lotes
                total_rows = len(df)

                for i in range(0, total_rows, batch_size):
                    batch = df.iloc[i:i+batch_size].values.tolist()
                    cursor.executemany(query, batch)
                    logger.debug(f"Lote insertado: {min(i+batch_size, total_rows)}/{total_rows} filas")

                conn.commit()

                logger.info(f"carga de {df['fecha_ts'].count()} datos en la tabla {table_name}")

                dict_result.update({
                            'cantidad_total': df.shape[0]
                            ,'cargados'      : total_rows
                            ,'dif': df.shape[0]-total_rows
                            ,'estado_proc': 'Ejecutado correctamente'})
                dict_result.update(valores_extraidos)

                return True, dict_result, f"{total_rows} filas insertadas en {table_name}"

        except Exception as e:
            logger.error(f"Error cargando datos en {table_name}: {str(e)}", exc_info=True)
            dict_result.update({'estado_proc':str(e)})
            if 'conn' in locals():
                conn.rollback()

            return False, dict_result, str(e)
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
            print(df.head(10), staging_table, target_proc)

            # Paso 1: Carga a staging
        #     success, temp_dict, msgLoad = self.load_to_staging(df
        #                                                        ,staging_table
        #                                                        , truncate=False)
        #     if not success:
        #         return False, f"Fallo carga staging: {msgLoad}"

        # ### Paso 2: Ejecutar procedimiento
        #     for proc in target_proc.values():
        #         success, msgProc = self.execute_stored_procedure(proc)

        #     if not success:
        #         return False, f"Fallo procedimiento: {msgProc}"

        #     return True, "Proceso completo exitoso"
        
        except Exception as e:
            logger.error("Fallo el proceso:",e, exc_info=True)


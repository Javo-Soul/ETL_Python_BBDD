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
    def load_log_table(self,dic:dict):
        """
        Inserta registros en las tablas de log basándose en el diccionario proporcionado.
        """
        try:
            result = dic
            estado = str(result['estado_proc'])
            valor = estado.split(':', 1)[1].strip() if ':' in estado else estado

            result.update({'estado_proc':valor})
        ####################################################
            
            data_type = {col: str for col in dic}
            df = pd.DataFrame(dic, index=[0])
            df = df.astype(data_type)
        ####################################################
            df.replace(to_replace=r"'", value='', regex=True,inplace=True)
            ####################################################
            if result.get('archivo_origen'):
                if not df['archivo_origen'].empty or df['archivo_origen'].iloc[0] != '':
                    df_archivos = df[['archivo_origen','fecha_archivo','cantidad_total'
                                    ,'cargados','dif','estado_proc']]                

                    with self.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(f'''insert into {tablaLog['logArchivo']} (nombre_archivo, fecha_archivo, cantidad_total
                                                                                ,cargados, dif, estado,fecha_carga)
                                        values (?,?,?,?,?,?,?)''',
                                        df_archivos['archivo_origen'].values[0],
                                        df_archivos['fecha_archivo'].values[0],
                                        df_archivos['cantidad_total'].values[0],
                                        df_archivos['cargados'].values[0],
                                        df_archivos['dif'].values[0],
                                        df_archivos['estado_proc'].values[0],
                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        )
                        print("Fila insertada:", cursor.rowcount)
                        conn.commit()

            if result.get('nombre_tabla'):
                if df['estado_proc'].values[0] != 'archivo vacio' or df['nombre_tabla'].values[0] != None:
                    df_tablas   = df[['nombre_tabla','fecha','cantidad_total','cargados','dif','estado_proc']]                    
                    with self.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute(f'''insert into {tablaLog['logTabla']} (nombre_tabla, fecha, cantidad_total
                                                                                ,cargados     , dif  , estado
                                                                                ,fecha_carga)
                                        values (?,?,?,?,?,?,?)''',
                                        df_tablas['nombre_tabla'].values[0],
                                        df_tablas['fecha'].values[0],
                                        df_tablas['cantidad_total'].values[0],
                                        df_tablas['cargados'].values[0],
                                        df_tablas['dif'].values[0],
                                        df_tablas['estado_proc'].values[0],
                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        )

                        conn.commit()
            
            logger.info(f"{cursor.rowcount} datpos Insertados en tabla log_tablas")
            ####################################################
        except Exception as e:
            logger.error("Error al insertar en tabla log_tablas", exc_info=True)

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
            dict_result = dict_gen(tipo = 'log_tabla')
            dict_result.update({'nombre_tabla': staging_table})

            extract_columns=['archivo_origen','nombre_archivo','fecha_archivo','fecha','fecha_query','fecha_ts']

            # Paso 1: Carga a staging
            success, temp_dict, msgLoad = self.load_to_staging(df
                                                               ,staging_table
                                                               , truncate=True
                                                               ,extract_columns = extract_columns)
            dict_result.update(temp_dict)

            if not success:
                dict_result['estado_proc'] = msgLoad
                return False, f"Fallo carga staging: {msgLoad}"

        ### Paso 2: Ejecutar procedimiento
            for proc in target_proc.values():
                success, msgProc = self.execute_stored_procedure(proc)
                dict_result['estado_proc'] = msgProc

            if not success:
                return False, f"Fallo procedimiento: {msgProc}"

            return True, "Proceso completo exitoso"
        
        finally:
            self.load_log_table(dict_result)


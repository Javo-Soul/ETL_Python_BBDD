# repository.py
import pandas as pd
import datetime
import configparser
from functools import wraps
from datetime import datetime
from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas.log_config import logger

config = configparser.ConfigParser()
config.read('config.ini')

tablaLog = {
    'logTabla'  : config['sql']['tabla_log_tablas'],
    'logArchivo': config['sql']['tabla_log_archivos']
}

class SQLRepository:
    def __init__(self, connection_pool):
        self.connection_pool = connection_pool

    @staticmethod
    def measure_time(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            elapsed_time = datetime.now() - start_time
            logger.info(f"{func.__name__} executed in {elapsed_time.total_seconds():.2f} seconds")
            return result
        return wrapper

    def _validar_y_formatear_fecha(self, fecha_str):
        """
        Valida y formatea una fecha string a formato YYYY-MM-DD
        Si no puede parsear, devuelve la fecha actual
        """
        if not fecha_str or pd.isna(fecha_str):
            return datetime.now().strftime('%Y-%m-%d')
        
        fecha_str = str(fecha_str).strip()
        
        # Lista de formatos de fecha a probar
        formatos_fecha = [
            '%Y-%m-%d',    # 2023-12-31
            '%d/%m/%Y',     # 31/12/2023
            '%m/%d/%Y',     # 12/31/2023
            '%Y%m%d',       # 20231231
            '%d-%m-%Y',     # 31-12-2023
            '%Y/%m/%d',    # 2023/12/31
            '%Y-%m-%d %H:%M:%S',  # Con hora
            '%d/%m/%Y %H:%M:%S'    # Con hora
        ]
        
        for formato in formatos_fecha:
            try:
                fecha_obj = datetime.strptime(fecha_str, formato)
                return fecha_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # Si ningún formato funcionó, usar fecha actual
        return datetime.now().strftime('%Y-%m-%d')

## -------------------------------------------------- ## 
    def get_connection(self):
        return self.connection_pool

## -------------------------------------------------- ## 
    def load_log_table(self,dic:dict):
        try:
            result = dic
            estado = str(result['estado_proc'])
            valor = estado.split(':', 1)[1].strip() if ':' in estado else estado

            result.update({'estado_proc':valor})
        ####################################################
            data_type = {col: str for col in dic}

            df = pd.DataFrame(dic, index=[0])
            df = df.astype(data_type)
        # ####################################################
            df.replace(to_replace=r"'", value='', regex=True,inplace=True)
            df_tablas = df[['nombre_tabla','fecha','cantidad_total','cargados','dif','estado_proc']]
            df_archivos = df[['nombre_archivo','fecha','cantidad_total','cargados','dif','estado_proc']]

        ######################################################
        #     with self.get_connection() as conn:
        #         cursor = conn.cursor()

        # ######################################################
        #     cursor.execute(f'''insert into {tablaLog['log_archivos']} (nombre_archivo, fecha_archivo, cantidad_total
        #                                                                ,cargados, dif, estado,fecha_carga)
        #                     values (?,?,?,?,?,?,?)''',
        #                     df_archivos['nombre_archivo'].values[0],
        #                     df_archivos['fecha'].values[0],
        #                     df_archivos['cantidad_total'].values[0],
        #                     df_archivos['cargados'].values[0],
        #                     df_archivos['dif'].values[0],
        #                     df_archivos['estado_proc'].values[0],
        #                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        #                     )
        #     conn.commit()

            with self.get_connection() as conn:
                cursor = conn.cursor()
            ################################################
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

            logger.info(f"Insertado en tabla log_tablas: {df_tablas['nombre_tabla'].values[0]}")
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
            # Si no se pasan columnas a extraer, usar una lista por defecto vacía
            extract_columns = extract_columns or []

            # Extraer valores de interés antes de cargar
            valores_extraidos = {col: (df[col].iloc[0] if col in df.columns else None) for col in extract_columns}
            fecha_actual = datetime.now().strftime('%Y-%m-%d')
            valores_extraidos.update({k: fecha_actual for k in ('fecha', 'fecha_query') if valores_extraidos.get(k) is None})

            dict_result.update(valores_extraidos)
            dict_result.update({'nombre_tabla': table_name})

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

                dict_result.update({'nombre_archivo':'','nombre_tabla': table_name
                            ,'cantidad_total': df.shape[0]
                            ,'cargados'      : total_rows
                            ,'dif': df.shape[0]-total_rows
                            ,'estado_proc': 'Ejecutado correctamente'})

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
    def full_load_process(self, df, staging_table, target_proc):
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
            dict_result = {'nombre_archivo': '','nombre_tabla': staging_table,
                            'fecha': datetime.now().strftime('%Y-%m-%d'),
                            'fecha_query': datetime.now().strftime('%Y-%m-%d'),
                            'cantidad_total': len(df) if isinstance(df, pd.DataFrame) else 0,
                            'cargados': 0,'dif': 0,'estado_proc': ''
                        }

            extract_columns=['nombre_archivo', 'fecha','fecha_query','fecha_ts']
            # Paso 1: Carga a staging
            success, temp_dict, msgLoad = self.load_to_staging(df, staging_table, truncate=True,extract_columns=extract_columns)
            dict_result.update(temp_dict)

            if not success:
                dict_result['estado_proc'] = msgLoad
                return False, f"Fallo carga staging: {msgLoad}"

        ### Paso 2: Ejecutar procedimiento
            success, msgProc = self.execute_stored_procedure(target_proc)
            dict_result['estado_proc'] = msgProc

            if not success:
                return False, f"Fallo procedimiento: {msgProc}"

            return True, "Proceso completo exitoso"
        
        finally:
            self.load_log_table(dict_result)


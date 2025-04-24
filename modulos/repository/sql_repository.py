# repository.py
import pandas as pd
from functools import wraps
from datetime import datetime
from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas.log_config import logger

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

    def get_connection(self):
        return self.connection_pool

    @measure_time
    def load_to_staging(self, df, table_name, truncate=True, batch_size=10000):
        """
        Carga datos a una tabla de staging con opción de truncar primero
        Args:
            df: DataFrame con los datos
            table_name: Nombre de la tabla destino
            truncate: Si True, trunca la tabla antes de cargar
            batch_size: Tamaño del lote para inserción
        Returns:
            Tuple (bool, str): Indicador de éxito y mensaje
        """
        if not isinstance(df, pd.DataFrame) or df.empty:
            logger.warning("DataFrame vacío recibido para carga")
            return False, "DataFrame vacío"

        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.fast_executemany = True
                
                # Truncar si es necesario
                if truncate:
                    cursor.execute(f"TRUNCATE TABLE {table_name}")
                    logger.info(f"Tabla {table_name} truncada")
                
                # Preparar datos
                df = df.where(pd.notnull(df), None)
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
                return True, f"{total_rows} filas insertadas en {table_name}"
                
        except Exception as e:
            logger.error(f"Error cargando datos en {table_name}: {str(e)}", exc_info=True)
            if 'conn' in locals():
                conn.rollback()
            return False, str(e)

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
                return True, f"Procedimiento {proc_name} ejecutado correctamente"
                
        except Exception as e:
            logger.error(f"Error ejecutando {proc_name}: {str(e)}", exc_info=True)
            if 'conn' in locals():
                conn.rollback()
            return False, str(e)

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
        success, msg = self.load_to_staging(df, staging_table, truncate=True)
        if not success:
            return False, f"Fallo carga staging: {msg}"
        ## Paso 2: Ejecutar procedimiento
        success, msg = self.execute_stored_procedure(target_proc)
        if not success:
            return False, f"Fallo procedimiento: {msg}"
        
        return True, "Proceso completo exitoso"


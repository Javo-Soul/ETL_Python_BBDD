#modulos/repository/repository.py
import os
import pandas as pd
from datetime import datetime
from modulos.logs.log_config import logger
from modulos.utils.utils import class_utils
from sqlalchemy import Select,MetaData,Table,and_, or_,text
from sqlalchemy.dialects.postgresql import insert
from  modulos.databaseClient.client import conexionSQL
from modulos.utils.utils import class_utils
## -------------------------------------------------- ##
measure_time = class_utils.measure_time
operadores   = class_utils.operadores()
conexiones   = conexionSQL()
## -------------------------------------------------- ##
class SQLRepository:
    """
    Clase para manejar operaciones de carga y ejecución de procedimientos en la base de datos SQL.
    """
    def __init__(self, engine):
        self.engine = engine

## -------------------------------------------------- ##
    def get_connection(self):
        return self.connection_pool

## -------------------------------------------------- ##
    @measure_time
    def validate_fields(self,tabla ,filtros:dict):
        try:
            with self.engine.connect() as conn:
                metadata  = MetaData()
                tabla_sql = Table(tabla, metadata, autoload_with=self.engine)
                columnas  = [col.name for col in tabla_sql.columns]

                condiciones = []
                for key,config in filtros.items():
                        if hasattr(tabla_sql.c, key):
                            col = tabla_sql.c[key]
                            operador = config.get("op", "==")
                            valor    = config.get("value")
                            if operador in operadores:
                                condiciones.append(operadores[operador](col, valor))
                            else:
                                logger.warning(f"Operador desconocido: {operador}")

                query = Select(tabla_sql).where(and_(*condiciones)) if condiciones else Select(tabla_sql)

            return True,query
        
        except Exception as e:
            logger.error(f"Error en validate_fields: {e}", exc_info=True)
            return False, str(e)

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
            df.to_sql(table_name,con = self.get_connection()
                      ,if_exists = if_exists
                      ,index=False, method='multi', chunksize=chunksize )

            print('\n',df.head(),'\n')

            return True, f"filas_insertadas : {len(df)}"

        except Exception as e:
            logger.error(f"Error cargando datos en {table_name}: {str(e)}", exc_info=True)
            return False, f"Error cargando datos en {table_name}: {str(e)}"
## -------------------------------------------------- ##

    @measure_time
    def upsert_data(self, df, table_name,engine):
        """
        Realiza un UPSERT en la tabla especificada utilizando SQLAlchemy.
        """
        if df.empty:
            logger.warning(f"DataFrame vacío recibido para UPSERT en {table_name}")
            return False, "DataFrame vacío"

        if engine.dialect.name != 'postgresql':
            return False, "El engine proporcionado no corresponde a PostgreSQL"

        try:
            # Reflejar la tabla desde la base de datos
            metadata = MetaData()
            metadata.reflect(engine)
            table = metadata.tables.get(table_name)

            if table is None:
                logger.error(f"No se encontró la tabla '{table_name}' en la base de datos.")
                return False, f"Tabla '{table_name}' no encontrada"

            # Crear sentencia INSERT con los datos
            insert_stmt = insert(table).values(df.to_dict(orient="records"))

            # Preparar columnas para actualizar (excluye clave primaria)
            update_dict = {
                col.name: insert_stmt.excluded[col.name]
                for col in table.columns
                if col.name.lower() != 'id'  # Ajusta si tu clave tiene otro nombre
            }

            # UPSERT con ON CONFLICT DO UPDATE
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['ID'],  # Asegúrate de que 'ID' es tu clave única
                set_=update_dict
            )

            # Ejecutar la transacción
            with engine.begin() as conn:
                result = conn.execute(upsert_stmt)
                rowcount = result.rowcount

            logger.info(f"UPSERT exitoso en '{table_name}': {rowcount} filas afectadas")
            return True, f"{rowcount} filas insertadas/actualizadas en '{table_name}'"

        except Exception as e:
            logger.error(f"Error durante UPSERT en '{table_name}': {str(e)}", exc_info=True)
            return False, f"Error durante UPSERT en '{table_name}': {str(e)}"

## -------------------------------------------------- ##
    @measure_time
    def full_load_process(self, df, target_table, engine):
        """
        Proceso completo de carga tipo UPSERT.
        """
        try:
            success, message = self.upsert_data(df, target_table,engine)
            return success, message
        except Exception as e:
            logger.error("Error en full_load_process:", e, exc_info=True)
            return False, str(e)

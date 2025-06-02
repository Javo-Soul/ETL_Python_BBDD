# services/data_sync_service.py
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.engine import Engine
import pandas as pd
import traceback
from modulos.logs.log_config import logger

class DataSyncService:
    def __init__(self, engine: Engine):
        self.engine = engine

    def sync_dataframe_to_table(self, df: pd.DataFrame, table_name: str, pk_column: str = 'ID', chunksize: int = 10000):
        if df.empty:
            logger.warning("DataFrame vacío, no se realiza carga.")
            return False, "El DataFrame está vacío"

        if self.engine.dialect.name != 'postgresql':
            return False, "El engine proporcionado no corresponde a PostgreSQL"

        if pk_column not in df.columns:
            return False, f"La columna clave primaria '{pk_column}' no existe en el DataFrame"

        try:
            # Reflejar solo la tabla específica
            metadata = MetaData()
            table = Table(table_name, metadata, autoload_with=self.engine)

            valid_columns = set(table.columns.keys())

            with self.engine.begin() as conn:
                for start in range(0, len(df), chunksize):
                    chunk = df.iloc[start:start + chunksize]

                    # Filtramos solo las columnas que existen en la tabla
                    filtered_chunk = chunk[[col for col in chunk.columns if col in valid_columns]]

                    rows = filtered_chunk.to_dict(orient='records')

                    stmt = pg_insert(table).values(rows)

                    # Definir las columnas a actualizar si hay conflicto (excepto la PK)
                    update_dict = {
                        col: stmt.excluded[col]
                        for col in filtered_chunk.columns
                        if col != pk_column and col in table.columns
                    }

                    stmt = stmt.on_conflict_do_update(
                        index_elements=[pk_column],
                        set_=update_dict
                    )

                    conn.execute(stmt)

            logger.info(f"{len(df)} registros sincronizados en '{table_name}' con UPSERT")
            return True, f"{len(df)} registros sincronizados en '{table_name}'"

        except Exception as e:
            logger.error("Error al sincronizar datos con PostgreSQL", exc_info=True)
            traceback.print_exc()
            return False, f"Error al sincronizar: {str(e)}"

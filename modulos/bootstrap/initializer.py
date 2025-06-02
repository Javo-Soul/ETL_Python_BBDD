# modulos/bootstrap/initializer.py
from modulos.repository.sql_repository import SQLRepository
from modulos.databaseClient.client import conexionSQL
from modulos.data import read_sqlserver
from modulos.logs.log_config import logger
from config.settings import settings

def inicializar(filtros: dict):
    try:
        logger.info("Se Ejecuta Inicializador")
        engine = conexionSQL().conexionSQLServer()
        repository = SQLRepository(engine)

        procAudit = read_sqlserver.ClaseSQL(
            repository,
            settings.sql.db_tabla_sql,
            filtros
        )
        df_Audit = procAudit.readSqltable()

    except Exception as e:
        logger.error(f'Error en initializer: {str(e)}')

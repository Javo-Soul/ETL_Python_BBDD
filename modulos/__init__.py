from modulos.repository.sql_repository import SQLRepository
from modulos.databaseClient.client import conexionSQL
from modulos.data import global_vars, read_csv, read_sqlserver
from modulos.logs.log_config import logger
from datetime import datetime, timedelta

def inicializar(filtros:dict):
    try:
        fecha = datetime.now()
        ## --------- Leer archivo de configuración ---------- ##
        logger.info(f"Se Ejecuta Inicializador")
        # ## --------------------------------------------------- ##
        # ## ------------ Crear conexión y repositorio --------- ##
        connection_pool = conexionSQL().conexionPostgress()
        repository = SQLRepository(connection_pool)
        ### --------------------------------------------------- ##
        procAudit = read_sqlserver.ClaseSQL(
            repository
            ,filtros)
        df_Audit = procAudit.readSqltable()
        # # ### ------------------------------------ ###
        # for i in range(dias):
        #     fecha = fecha + timedelta(days=-1)
        #     print('\n------------------',fecha,'------------------')
        #     procTrans = tablaTrans.Clasetrans(
        #         repository
        #         ,fecha
        #     )
        #     df_Trans = procTrans.leerBBDDtrans()
    except Exception as e:
        logger.error('Error en init:', str(e))
    
    return


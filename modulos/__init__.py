from modulos.repository.sql_repository import SQLRepository
from modulos.databaseClient.client import conexionSQL
from modulos.data import global_vars, read_csv, read_sqlserver
from modulos.logs.log_config import logger
from datetime import datetime, timedelta
from modulos.config_loader import cargar_config

def inicializar(dias:int=2):
    try:
        fecha = datetime.now()
        ## --------- Leer archivo de configuración ---------- ##
        logger.info(f"Archivo INI cargado correctamente desde: {cargar_config().sections()}")
        # ## --------------------------------------------------- ##
        # ## ------------ Crear conexión y repositorio --------- ##
        connection_pool = conexionSQL().conexionSQLServer()
        repository = SQLRepository(connection_pool)
        ### --------------------------------------------------- ##
        procAudit = read_sqlserver.ClaseSQL(
            repository)
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


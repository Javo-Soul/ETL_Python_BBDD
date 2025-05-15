from modulos.repository.sql_repository import SQLRepository
from modulos.conexionSQL.client import conexionSQL
from modulos.datos import leerarchivo,tablaAudit,tablaTefabm,tablaTrans
from modulos.logs.log_config import logger
from datetime import datetime, timedelta
from modulos.config_loader import cargar_config

def inicializar(env:str ='test', dias:int=2):
    try:
        fecha = datetime.now()
        ## --------- Leer archivo de configuración ---------- ##
        logger.info(f"Archivo INI cargado correctamente desde: {cargar_config().sections()}")
        ## --------------------------------------------------- ##
        ## ------------ Crear conexión y repositorio --------- ##
        connection_pool = conexionSQL(environment = env).conexionSQLServer()
        repository = SQLRepository(connection_pool)
        ## --------------------------------------------------- ##
        procTefamb = tablaTefabm.Clasetefabm(
            repository=repository  # Inyectamos la dependencia
        )
        df_Tefamb = procTefamb.leerBBDDtefabm()
        ## --------------------------------------------------- ##
        procAudit = tablaAudit.ClaseAudit(
            repository)
        df_Audit = procAudit.leerBBDDaudit()
        # ### ------------------------------------ ###
        for i in range(dias):
            fecha = fecha + timedelta(days=-1)
            print('\n------------------',fecha,'------------------')
            procTrans = tablaTrans.Clasetrans(
                repository
                ,fecha
            )
            df_Trans = procTrans.leerBBDDtrans()
    except Exception as e:
        logger.error('Error en init:', str(e))
    
    return


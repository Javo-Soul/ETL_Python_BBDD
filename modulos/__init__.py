from modulos.repository.sql_repository import SQLRepository
from modulos.conexionSQL.client import conexionSQL
from modulos.datos import leerarchivo,tablaAudit,tablaTefabm,tablaTrans
from modulos.logs.log_config import logger
from datetime import datetime, timedelta


def inicializar(env:str ='test', dias:int=2):
    try:
        fecha = datetime.now()
        ####### Crear conexión y repositorio #######
        connection_pool = conexionSQL(environment = 'test').conexionSQLServer()
        repository = SQLRepository(connection_pool)
        ### ------------------------------------ ###
        procTefamb = tablaTefabm.Clasetefabm(
            repository=repository  # Inyectamos la dependencia
        )
        df_Tefamb = procTefamb.leerBBDDtefabm()
        ### ------------------------------------ ###
        procAudit = tablaAudit.ClaseAudit(
            repository)
        df_Audit = procAudit.leerBBDDaudit()
        ### ------------------------------------ ###
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


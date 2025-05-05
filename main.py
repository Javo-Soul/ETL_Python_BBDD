from datetime import datetime, timedelta
from modulos.repository.sql_repository import SQLRepository
from modulos.conexionSQL.conexionBD2 import conexionSQL
########################################################
from modulos.datos import tablaTefabm as datosTefabm
from modulos.datos import tablaTrans as datosTrans
from modulos.datos import tablaAudit as datosAudit
########################################################
def main():
    fecha = datetime.now()
    #-- 1. Configurar el repository --#
    connection_pool = conexionSQL().conexionSQLServer()
    repository = SQLRepository(connection_pool)
    ## ------------------------------------##
    tablaTefamb = datosTefabm.Clasetefabm(
        repository=repository  # Inyectamos la dependencia
    )
    df_Tefamb = tablaTefamb.leerBBDDtefabm()
    ## ------------------------------------##
    tablaAudit = datosAudit.ClaseAudit(
        repository)
    df_Audit = tablaAudit.leerBBDDaudit()
    ## ------------------------------------##
    ###########################################################
    ######## se define la cantidad de dias que extrae #########
    # for i in range(3):
    #     fecha = fecha + timedelta(days=-1)
    #     print('\n------------------',fecha,'------------------')
    #     tablaTrans = datosTrans.Clasetrans(
    #         repository
    #         ,fecha
    #     )
    #     df_Trans = tablaTrans.leerBBDDtrans()
    # trans.generaTablaAct()

if __name__ == "__main__":
    main()


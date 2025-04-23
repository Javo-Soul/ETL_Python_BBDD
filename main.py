from datetime import datetime, timedelta
from modulos.datos import tablaTefabm as datos
from modulos.repository.sql_repository import SQLRepository
from modulos.conexionSQL.conexionBD2 import conexionSQL
########################################################
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()

def main():
    # 1. Configurar el repository
    connection_pool = conexionSQL().conexionSQLServer()
    repository = SQLRepository(connection_pool)

    tablaTefamb = datos.leerTrans(fecha=datetime.now().date(), dias=0,
        repository=repository  # Inyectamos la dependencia
    )
    # 3. Usar normalmente
    df_Tefamb = tablaTefamb.leerBBDDtefabm()

    ###########################################################
    ######## se define la cantidad de dias que extrae #########

    # dias = 1
    # i = 0
    # for i in range(dias):
    #     fecha = fecha_consulta + timedelta(days= -2)
    #     trans = datos.leerTrans(fecha, dias)
    #     print('-------------------- ',fecha,'-------------------- ')
    #     df = trans.leerBBDDtrans()
    #     df.to_csv('leerBBDDtrans.csv',sep=';')
        # df = trans.leerBBDDaudit()
        # i =+ 1
    # trans.generaTablaAct()

if __name__ == "__main__":
    main()


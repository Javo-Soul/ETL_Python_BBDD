from datetime import datetime, timedelta
from modulos.datos import leerarchivo as datos

########################################################
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()

def main():
    pass
    ###########################################################
    tablaTefamb = datos.leerTrans(fecha_consulta, dias = 0)
    df_Tefamb   = tablaTefamb.leerBBDDtefabm()

    ###########################################################
    ######## se define la cantidad de dias que extrae #########
    # dias = 1
    # i = 0
    # for i in range(dias):
    #     fecha = fecha_consulta + timedelta(days= -2)
    #     trans = datos.leerTrans(fecha, dias)
    #     print('-------------------- ',fecha,'-------------------- ')
    #     trans.leerBBDDtrans()
    #     trans.leerBBDDaudit()
    #     i =+ 1

    # trans.generaTablaAct()

if __name__ == "__main__":
    main()


git ls-files | grep "\.txt$"
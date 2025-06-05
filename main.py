# main.py
from modulos.bootstrap.initializer import inicializar
from modulos.databaseClient.client import conexionSQL
from sqlalchemy import MetaData,Table,Select
import pandas as pd

########################################################
def main():
    engine = conexionSQL().conexionSQLServer()

    with engine.connect() as conn:
        metadata = MetaData()
        tabla    = Table("data_test",metadata,autoload_with=engine)
        query    = Select(tabla)
        result   = conn.execute(query)
        rows   = result.fetchall()
    
    df_result = pd.DataFrame(rows, columns=result.keys())
    print(df_result)

    # df_data   = pd.read_csv('data_csv/sample.csv', sep=';')
    # df_data.to_sql("data_test",con=engine,schema= "dbo",if_exists='append', index=False)


    # filtros = {
    # "OriginAirportID": {"op": "==", "value": 15304},
    # "DayofMonth"     : {"op": ">=", "value": 15},
    # # "ID"             : {"op": "==", "value": 9844}
    # }
    # proceso = inicializar(filtros)

if __name__ == "__main__":
    main()


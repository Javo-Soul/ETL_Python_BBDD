# main.py
from modulos.bootstrap.initializer import inicializar
from modulos.databaseClient.client import conexionSQL
from sqlalchemy import MetaData,Table,Select
import pandas as pd

########################################################
def main():
    filtros = {
    "OriginAirportID": {"op": "==", "value": 15304},
    "DayofMonth"     : {"op": ">=", "value": 15},
    # "ID"             : {"op": "==", "value": 9844}
    }
    proceso = inicializar(filtros)

if __name__ == "__main__":
    main()

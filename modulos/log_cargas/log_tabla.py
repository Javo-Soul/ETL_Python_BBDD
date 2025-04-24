import os
import configparser
from pathlib import Path
import pandas as pd
from datetime import datetime

######################## librerias personales ################################
from modulos.conexionSQL import conexionBD2 as conSQL
from modulos.log_cargas.log_config import logger

######################## Tablas SQL Consolidado ##############################
conexion   =  conSQL.conexionSQL()
##############################################################################
config     = configparser.ConfigParser()
config.read('config.ini')

TABLAS_SQL = {
    'log_archivos': config['sql']['tabla_log_archivos'],
    'log_tablas' : config['sql']['tabla_log_tablas'],
}

paths = {  
    'repo_csv' : config['paths']['repo_csv'],
    'repo_txt' : config['paths']['repo_txt'],
}

#####################################################################
class registroLOGTabla:
    ######################################################################
    def insertTablaLog(self, dic:dict):
        try:
        ####################################################
            data_type = {col: str for col in dic}

            df = pd.DataFrame(dic, index=[0])
            df = df.astype(data_type)
        ####################################################
            df.replace(to_replace=r"'", value='', regex=True,inplace=True)
            df_tablas = df[['nombre_tabla','fecha','cantidad_total','cargados','dif','estado_proc']]
            df_archivos = df[['nombre_archivo','fecha','cantidad_total','cargados','dif','estado_proc']]
        ################################################
            with conexion.conexionSQLServer() as conn:
                cursor = conn.cursor()
            ##
            cursor.execute(f'''insert into {TABLAS_SQL['log_archivos']} (nombre_archivo, fecha_archivo, cantidad_total
                                                                       ,cargados, dif, estado,fecha_carga)
                            values (?,?,?,?,?,?,?)''',
                            df_archivos['nombre_archivo'].values[0],
                            df_archivos['fecha'].values[0],
                            df_archivos['cantidad_total'].values[0],
                            df_archivos['cargados'].values[0],
                            df_archivos['dif'].values[0],
                            df_archivos['estado_proc'].values[0],
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            )
            conn.commit()

            with conexion.conexionSQLServer() as conn:
                cursor = conn.cursor()
            ################################################
            cursor.execute(f'''insert into {TABLAS_SQL['log_tablas']} (nombre_tabla, fecha, cantidad_total
                                                                       ,cargados     , dif  , estado
                                                                       ,fecha_carga)
                            values (?,?,?,?,?,?,?)''',
                            df_tablas['nombre_tabla'].values[0],
                            df_tablas['fecha'].values[0],
                            df_tablas['cantidad_total'].values[0],
                            df_tablas['cargados'].values[0],
                            df_tablas['dif'].values[0],
                            df_tablas['estado_proc'].values[0],
                            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            )
            conn.commit()

            logger.info(f"Insertado en tabla log_tablas: {df_tablas['nombre_tabla'].values[0]}")            
        except Exception as e:
            logger.error("Error al insertar en tabla log_tablas", exc_info=True)


import pyodbc
import os
import json
from dotenv import load_dotenv
import configparser
load_dotenv()
import logging
from modulos.log_cargas.log_config import logger
##-------- archivo config ---------------
config = configparser.ConfigParser()
config.read('config.ini')

bd_config = {
    'host'      : config['BBDD']['host'],
    'base_datos': config['BBDD']['base_datos']
}
#----------------------------------------
# Configuración del logger
#----------------------------------------

class conexionSQL:
    def __init__(self, environment = 'test'):
        self.environment   = environment
        self._carga_credenciales()
    
    ##################################################
    def _carga_credenciales(self):
        if self.environment == 'test':
            logger.info("Cargando credenciales de test")
            self.localhost     = bd_config['host']
            self.localDatabase = bd_config['base_datos']
            self.produccion    = os.getenv('Server_Produccion')
            self.contingencia  = os.getenv('Server_Contingencia')
            self.IWS           = os.getenv('Server_IWS')

        else:
            logger.info("Cargando credenciales de Producción")
            self.localhost     = bd_config['host']
            self.localDatabase = bd_config['base_datos']
            self.produccion    = os.environ.get('Server_Produccion')
            self.contingencia  = os.environ.get('Server_Contigencia')
            self.IWS           = os.environ.get('IWS')
    
    ##################################################
    def conexion_produccion(self):
        connection = ''
        try:
            credentials = self.produccion
            credentials = json.loads(credentials)
            usuario     = credentials.get('Usuario')
            password    = credentials.get('Password')
            Host        = credentials.get('IP')

            connection = pyodbc.connect(
                driver ='{iSeries Access ODBC Driver}',
                system = Host,
                uid    = usuario,
                pwd    = password)

            logger.info(f"Conexión a la base de datos Produccion .70 ")

        except Exception as e:
            logger.error("Error de conexión SQL", exc_info=True)

        return connection

    ##################################################    
    def conexion_IWS(self):
        connection = ''
        try:
            credentials = self.IWS
            credentials = json.loads(credentials)
            user        = credentials.get('Usuario')
            password    = credentials.get('Password')
            database    = credentials.get('IP')

            connection_string = f'''Driver=SQL Anywhere 17;UID={user};PWD={password};DatabaseName={database};ServerName=acuarius_iws;Host=datawarehouse-01:2638'''
            connection = pyodbc.connect(connection_string)

            logger.info(f"Conexión a la base de datos IWS")
        except Exception as e:
            logger.error("Error de conexión a IWS", exc_info=True)

        return connection

    ##################################################
    def conexion_contingencia(self):
        connection = ''
        try:
            credentials = self.contingencia
            credentials = json.loads(credentials)
            usuario     = credentials.get('Usuario')
            password    = credentials.get('Password')
            Host        = credentials.get('IP')
            
            connection = pyodbc.connect(
                driver='{iSeries Access ODBC Driver}',
                system = Host,
                uid = usuario,
                pwd = password)
            logger.info(f"Conexión a la base de datos Contingencia .47")

        except Exception as e:
            logger.error("Error de conexión a Servidor Contingencia", exc_info=True)

        return connection

    ##################################################
    def conexionSQLServer(self):
        conn = ''
        try:
            conn = ''
            server = self.localhost
            database = self.localDatabase
            conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';Trusted_Connection=yes')

            logger.info(f"Conexión a la base de datos SQL Server {server}")

        except Exception as e:
            logger.error("Error de conexión SQL", exc_info=True)

        return conn

    ##################################################
    def querySql(self, query, tipo, valores=None):
        response = []
        try:
            conexion = self.conexionSQLServer()
            cursor = conexion.cursor()

            if tipo == 'select':
                cursor.execute(query, valores or [])
                response = cursor.fetchone()

            elif tipo == 'select all':
                cursor.execute(query, valores or [])
                response = cursor.fetchall()

            elif tipo == 'insert':
                with conexion.cursor() as cursor:
                    cursor.execute(query, valores or [])
                    response = 'Ejecutado Exitosamente'

            elif tipo == 'exec':
                cursor.execute(query, valores or [])
                while cursor.nextset():
                    pass
                response = 'Ejecutado Exitosamente'

            else:
                response = ['Accion no válida']

        except pyodbc.Error as ex:
            logger.error(f"Error general en operación SQL", exc_info=True)
            response = ex

        except Exception as e:
            logger.error("Error general en operación SQL", exc_info=True)
            response = e

        finally:
            try:
                if conexion:
                    conexion.commit()
                    conexion.close()
            except:
                pass

        return response


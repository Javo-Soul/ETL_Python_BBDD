# modulos/conexionesSQL/client.py
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError,DatabaseError
##-------------------------------------##
from modulos.logs.log_config import logger
from config.settings import settings
### ----------------------------------------


class conexionSQL:
    def __init__(self):
        self.environment   = settings.environment
        self._carga_credenciales()
    ##################################################
    def _carga_credenciales(self):
        logger.info(f"Cargando credenciales de environment: {self.environment}")
        self.sql_host     = settings.sql.db_host_sql
        self.sql_Database = settings.sql.db_sql
        self.sql_port     = settings.sql.db_port_sql
        self.sql_user     = settings.sql.db_user_sql
        self.sql_pass     = settings.sql.db_pass_sql.get_secret_value()

        self.postgress_host = settings.postgres.postgress_host
        self.postgress_Database = settings.postgres.postgress_db
        self.postgress_port    = settings.postgres.postgress_port
        self.postgress_user    = settings.postgres.postgress_user
        self.postgress_pass    = settings.postgres.postgress_pass.get_secret_value()
    
    ##################################################
    def conexionPostgress(self):
        conn = None
        # try:
        #     conn   = None
        #     server = self.localhost
        #     database = self.localDatabase
        #     logger.info(f"Conexión a la base de datos SQL Server {server}")
        # except Exception as e:
        #     logger.error("Error de conexión SQL", exc_info=True)

        # return conn

    ##################################################
    def conexionSQLServer(self):
        conn = None
        try:
            url   = None
            server = self.sql_host
            port   = self.sql_port
            user   = self.sql_user
            pass_  = self.sql_pass
            database = self.sql_Database

            url = f"mssql+pytds://{user}:{pass_}@{server}:{port}/{database}"
            logger.info(f"Conexión a la base de datos SQL Server {server}")
        except SQLAlchemyError as e:
            logger.error("SQLAlchemyError: ", exc_info=True)

        except DatabaseError as e:
            logger.error("SQLAlchemyError: ", exc_info=True)

        except Exception as e:
            logger.error("Error desconocido: ", exc_info=True)

        return create_engine(url,echo=False,future=True)

    ##################################################
    def querySql(self, query, tipo, valores=None):
        response = []
        # try:
        #     conexion = self.conexionSQLServer()
        #     cursor = conexion.cursor()

        #     if tipo == 'select':
        #         cursor.execute(query, valores or [])
        #         response = cursor.fetchone()

        #     elif tipo == 'select all':
        #         cursor.execute(query, valores or [])
        #         response = cursor.fetchall()

        #     elif tipo == 'insert':
        #         with conexion.cursor() as cursor:
        #             cursor.execute(query, valores or [])
        #             response = 'Ejecutado Exitosamente'

        #     elif tipo == 'exec':
        #         cursor.execute(query, valores or [])
        #         while cursor.nextset():
        #             pass
        #         response = 'Ejecutado Exitosamente'

        #     else:
        #         response = ['Accion no válida']

        # except pyodbc.Error as ex:
        #     logger.error(f"Error general en operación SQL", exc_info=True)
        #     response = ex

        # except Exception as e:
        #     logger.error("Error general en operación SQL", exc_info=True)
        #     response = e

        # finally:
        #     try:
        #         if conexion:
        #             conexion.commit()
        #             conexion.close()
        #     except:
        #         pass

        # return response


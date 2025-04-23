import pandas as pd
import datetime
from datetime import datetime
from typing import Optional
from modulos.repository.sql_repository import SQLRepository  # Importación clara
## ------------- librerias personalizadas ------------ ##
from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas import log_tabla as log
from modulos.log_cargas.log_config import logger
## ---------------config ini ------------------------- ##
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
## ---------------------------------------------------- ##
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()
fecha_actual = fecha_hoy.strftime("%d-%m-%Y %H:%M:%S")
## ----------------------------------------------- ##
conexiones = conexionSQL()
log_carga = log.registroLOGTabla()
# ----------------------------------------------- ##
tablasSQL = {
    'tablaTrans'   : config['sql']['tabla_trans'],
    'tabla_audit'  : config['sql']['tabla_audit'],
    'tabla_tefabm' : config['sql']['tabla_tefabm']
}

procSQL = {
    'proc_prod_trans'  : config['procedimientos']['proc_prod_trans'],
    'proc_prod_audit'  : config['procedimientos']['proc_prod_audit'],
    'proc_prod_tefabm' : config['procedimientos']['proc_prod_tefabm']
}

carpetacsv = {
  'carpetacsv'  : config['paths']['repo_csv']
}
## ----------------------------------------------- ##

######################################################################
class leerTrans:
  def __init__(self, fecha: datetime, dias: int, repository: SQLRepository):
    self.fecha      = fecha
    self.dias       = dias
    self.repository = repository
    self.dia        = fecha.day
    self.mes        = fecha.month
    self.año        = fecha.year

######################################################################
  def leerBBDDtefabm(self):
      try:
          logger.info('Leyendo datos de TEFABM')
          # ... lógica de lectura...
          c1    = conexiones.conexion_contingencia().cursor()
          query = f''' select a.TEFRUE,a.TEFRSP,a.TEFTAM,a.TEFDAM,a.TEFFAN
          ,a.TEFRBF,a.TEFNBF,a.TEFBCO,a.TEFCTA,a.TEFTCT,a.TEFMTR
          ,a.TEFREF,a.TEFESF,a.TEFEMD,a.TEFITF,a.TEFTRN,a.TEFHOR
          ,a.TEFPRC,a.TEFFL1,a.TEFFL2,a.TEFFL3,a.TEFFL4,a.TEFFL5
          ,a.TEFFL6,a.TEFLMM,a.TEFLMD,a.TEFLMY
          from prdhifiles.tefabm AS a 
          '''
          c1.execute(query)
          c1 = c1.fetchall()
          #----------------------------------------------##
          data = { 'tefrue':[],'tefrsp':[],'teftam':[],'tefdam':[],'teffan':[],'tefrbf':[],'tefnbf':[]
                  ,'tefbco':[],'tefcta':[],'teftct':[],'tefmtr':[],'tefref':[],'tefesf':[],'tefemd':[]
                  ,'tefitf':[],'teftrn':[],'tefhor':[],'tefprc':[],'teffl1':[],'teffl2':[],'teffl3':[]
                  ,'teffl4':[],'teffl5':[],'teffl6':[],'teflmm':[],'teflmd':[],'teflmy':[]}

          for row in c1:
            for key, value in zip(data.keys(), row):
              data[key].append(value)

          df = pd.DataFrame.from_dict(data)
          #----------------------------------------------##
          df_datatype = {col: str for col in data}
          df = df.replace('  ','', regex=True)
          df['fecha_ts'] = fecha_actual
          df = df.astype(df_datatype)
          # Cargar usando el repository

          success, message = self.repository.full_load_process(
              df=df,
              staging_table = tablasSQL['tabla_tefabm'],
              target_proc   = procSQL['proc_prod_tefabm']
          )

          if not success:
              logger.error(f"Error en carga TEFABM: {message}")

          return df

      except Exception as e:
          logger.error(f"Error leyendo TEFABM: {str(e)}", exc_info=True)
          return pd.DataFrame()

######################################################################
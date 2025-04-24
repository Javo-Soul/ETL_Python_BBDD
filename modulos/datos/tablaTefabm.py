import pandas as pd
from modulos.repository.sql_repository import SQLRepository  # Importación clara
## ------------- librerias personalizadas ------------ ##
from .leerarchivo import conexiones,fecha_actual,logger,tablasSQL,procSQL,registroTabla
## ----------------------------------------------- ##

######################################################################
class Clasetefabm:
  def __init__(self,repository: SQLRepository):
    self.repository = repository

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
          else:
              logger.info(f'proceso de carga TEFABM terminado\n')
              
              df_tablas = {'nombre_archivo':''
                           ,'nombre_tabla': tablasSQL['tabla_tefabm']
                           ,'fecha':''
                           ,'cantidad_total': df['fecha_ts'].count()
                           ,'cargados'      : df['fecha_ts'].count()
                           ,'dif'           : 0
                           ,'estado_proc'   :'Ejecutado Exitosamente'}
              
              registroTabla.insertTablaLog(df)

          return df

      except Exception as e:
          logger.error(f"Error leyendo TEFABM: {str(e)}", exc_info=True)
          return pd.DataFrame()

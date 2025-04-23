import pandas as pd
import datetime
from functools import wraps
import time
from datetime import datetime

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
def mide_tiempo(func):
  @wraps(func)
  def wrapper(*args, **kwargs):
    start_time   = time.time()
    result       = func(*args,**kwargs)
    end_time     = time.time()
    elapsed_time = end_time - start_time      
    print(f"tiempo de ejecucion de {func.__name__}: {elapsed_time:.2f} segundos","\n")
    return result
  return wrapper

class leerTrans:
  def __init__(self,fecha, dias):
    self.fecha = fecha
    self.dias  = dias
    self.dia   = fecha.day
    self.mes   = fecha.month
    self.año   = fecha.year

######################################################################
  @mide_tiempo
  def leerBBDDtefabm(self):
    try:
      logger.info(f'leyendo datos de leerBBDDtefabm')
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
      ######## se definen listas #####################
      data = { 'tefrue':[],'tefrsp':[],'teftam':[],'tefdam':[],'teffan':[],'tefrbf':[],'tefnbf':[]
              ,'tefbco':[],'tefcta':[],'teftct':[],'tefmtr':[],'tefref':[],'tefesf':[],'tefemd':[]
              ,'tefitf':[],'teftrn':[],'tefhor':[],'tefprc':[],'teffl1':[],'teffl2':[],'teffl3':[]
              ,'teffl4':[],'teffl5':[],'teffl6':[],'teflmm':[],'teflmd':[],'teflmy':[]}

      for row in c1:
        for key, value in zip(data.keys(), row):
          data[key].append(value)

      df = pd.DataFrame.from_dict(data)
      ## ------------------------------------------ ##
      df_datatype = {col: str for col in data}

      df = df.replace('  ','', regex=True)
      df['fecha_ts'] = fecha_actual
      df = df.astype(df_datatype)
      ## ------------------------------------------ ##
      self.insert_with_batch(df, tablasSQL['tabla_tefabm'])

    except Exception as e:
      logger.error(f'leyendo datos de leerBBDDtefabm: {e}', exc_info=True)
      df = pd.DataFrame()
    return df

######################################################################
  @mide_tiempo
  def leerBBDDtrans(self):
    try:
      logger.info(f'leyendo datos de leerBBDDtefabm')
      c1 = conexiones.conexion_contingencia().cursor()
      query = f''' SELECT 'TEF' Origen,TO_DATE(CASE WHEN LENGTH(CAST( A.TRABDD AS VARCHAR(2))) = 1 THEN 
      0||A.TRABDD ELSE VARCHAR(A.TRABDD) END  ||CASE WHEN LENGTH(CAST(A.TRABDM AS VARCHAR(2))) = 1 THEN 
      0||A.TRABDM ELSE VARCHAR(A.TRABDM) END ||CHAR(A.TRABDY),'DDMMYYYY') AS FECHA,A.TRABRN OFICINA
      ,A.TRAGLN CTACTBLE,D.GLMDSC NOMCUENTA,A.TRAACC NUMEROCTA,A.TRANAR GLOSA
      ,Case When A.Tradcc = 0 then A.Traamt else 0 END DEBITO,Case When A.Tradcc = 5 then A.Traamt else 0 END CREDITO
      ,Case When A.Tradcc = 5 then A.Traamt * - 1 else Traamt END *-1 Monto
      ,A.TRATMS Fechahora,A.TRACKN ID,repeat ('0' , 12 - length(TRIM(C.AUDRFN))) || C.AUDRFN  TRACE_ID
      ,A.trabdy,A.trabdm,A.trabdd,cusidn AS rut
      FROM prdcyfiles.TRANS AS A
      LEFT JOIN prdcyfiles.GLMST AS D ON A.TRACCY = D.GLMCCY AND A.TRAGLN = D.GLMGLN 
      LEFT JOIN PRDCYFILES.AUDHI AS C ON C.audckn = A.trackn AND C.AUDMNT = A.traamt 
      and A.tratms = C.audtms AND A.traacc = C.AUDOAC and C.audtcd IN (610,611,613,614) and AUDFLG = 'A' and AUDAPF = 'A'
      LEFT JOIN PRDCYFILES.CUMST on tracun = cuscun
      WHERE  A.trabdy = ?  and A.trabdm in ? and A.trabdd = ? --fecha contable
      and A.trabth = 59004 and not A.tragln IN (2110990110013400,2115990110013800,2110990110013300)
      UNION ALL
      SELECT
      'Rem' Origen,TO_DATE(CASE WHEN LENGTH(CAST( A.TRABDD AS VARCHAR(2))) = 1 THEN
      0||A.TRABDD ELSE VARCHAR(A.TRABDD) END  ||
      CASE WHEN LENGTH(CAST(A.TRABDM AS VARCHAR(2))) = 1 THEN 
      0||A.TRABDM ELSE VARCHAR(A.TRABDM) END ||
      CHAR(A.TRABDY),'DDMMYYYY') AS FECHA,A.TRABRN OFICINA,A.TRAGLN CTACTBLE,D.GLMDSC NOMCUENTA,A.TRAACC NUMEROCTA,A.TRANAR GLOSA
      ,Case When A.Tradcc = 0 then A.Traamt else 0 END DEBITO
      ,Case When A.Tradcc = 5 then A.Traamt else 0 END CREDITO
      ,Case When A.Tradcc = 5 then A.Traamt *   - 1 else Traamt END *-1 Monto
      ,A.TRATMS Fechahora,A.TRACKN ID,repeat ('0' , 12 - length(TRIM(A.traacc))) || A.traacc  TRACE_ID
      ,A.trabdy,A.trabdm,A.trabdd,'0' RUT
      FROM PRDCYFILES.trans AS A
      LEFT JOIN prdcyfiles.GLMST AS D ON A.TRACCY = D.GLMCCY AND A.TRAGLN = D.GLMGLN 
      WHERE TRABDY = ? AND TRABDM = ? and A.trabdd = ? AND TRAGLN = 2115990110013800 AND TRABTH = 59014
      limit 1000
      '''
      params = (self.año, self.mes, self.dia, 
                self.año, self.mes, self.dia)

      c1.execute(query,params)
      c1 = c1.fetchall()
      ######## se definen listas #####################
      data = {'origen':[] ,'fecha':[],'oficina':[],'ctactble':[],'nomcuenta':[],'numerocta':[]
              ,'glosa':[],'debito':[],'credito':[],'monto':[],'fechahora':[],'id':[],'trace_id':[]
              ,'trabdy':[],'trabdm':[],'trabdd':[] ,'rut':[]}

      for row in c1:
        for key,value in zip(data.keys(),row):
          data[key].append(value)

      df = pd.DataFrame.from_dict(data)
      df_datatype = {col: str for col in data}
      df = df.astype(df_datatype)
      df = df.replace('   ', '', regex=True)
      df['fecha_ts'] = fecha_actual

      # self.insert_with_batch(df, tablasSQL['tablaTrans'])
    except Exception as e:
      logger.error(f'leyendo datos de leerBBDDtrans : {e}', exc_info=True)
      df = pd.DataFrame()
    return df

######################################################################
  @mide_tiempo
  def leerBBDDaudit(self):
    try:
      logger.info('leyendo datos de leerBBDDaudit')
      c1    = conexiones.conexion_contingencia().cursor()
      query = f''' select TO_DATE(CASE WHEN LENGTH(CAST( AUDDTD AS VARCHAR(2))) = 1 THEN 
        0||AUDDTD ELSE VARCHAR(AUDDTD) END  ||
        CASE WHEN LENGTH(CAST(AUDDTM AS VARCHAR(2))) = 1 THEN 
        0||AUDDTM ELSE VARCHAR(AUDDTM) END ||
        CHAR(AUDDTY),'DDMMYYYY') AS FECHA,(Audflg)Estado 
      ,(Audtlr)Cajero,(Audtbr)CodOficina,(Brnnme)NomOficina
      ,(Audckn)NumDocto,(Audtcd)CodTrx,(Auddsc)Glosa,(Audmnt)Monto
      ,(AUDOAC)CuentaSocio,(AUDDGL)CuentaDebito,(AUDCGL)CuentaCredito
      ,(AUDCCY)Moneda,(audapf)Contabilizada
      ,repeat ('0' , 12 - length(TRIM(AUDRFN))) || AUDRFN  TRACE_ID
      ,(Audtms) AS fechahora,audtme AS hora,Cusidn AS rut

      FROM prdCyfiles.AUDIT
      ------------------------------------
      LEFT JOIN prdCyfiles.CNTRLBRN ON  AUDTBR = BRNNUM
      ------------------------------------
      LEFT JOIN PRDCYFILES.ACMST ON acmacc = AUDOAC
      ------------------------------------
      LEFT JOIN PRDCYFILES.cumst on CUSCUN = acmcun 
      ------------------------------------
      where audtcd in (610,611,613,614)   and Audflg = 'A'
      order by Audtms
      '''

      c1.execute(query)
      c1 = c1.fetchall()
      ######## se definen diccionario con columnas #####################
      data = {'fecha':[] ,'estado':[],'cajero':[],'codoficina':[],'nomoficina':[],'numdocto':[],'codtrx':[] ,'glosa':[]
              ,'monto':[],'cuentasocio':[],'cuentadebito':[],'cuentacredito':[],'moneda':[],'contabilizada':[]
              ,'trace_id':[],'fecha_hora':[],'hora':[],'rut':[]}

      for row in c1:
        for key,value in zip(data.keys(), row):
          data[key].append(value)

      df = pd.DataFrame.from_dict(data)

      df_datatype = {col: str for col in data}

      df = df.astype(df_datatype)
      ### ----------------------------------------- ###
      df['rut']   = df['rut'].str.replace(' ', '', regex=True)
      df['glosa'] = df['glosa'].str.replace(' ', '', regex=True)
      df['fecha_ts'] = fecha_actual

      self.insert_with_batch(df, tablasSQL['tabla_audit'])
    except Exception as e:
      logger.error(f'leyendo datos de leerBBDDaudit : {e}', exc_info=True)
      df = pd.DataFrame()

    ### ----------------------------------------- ###
    ##############################################################
    return df

######################################################################
  @mide_tiempo
  def insert_with_batch(self, df, tabla, batch_size = 10000):
    conn                    = conexiones.conexionSQLServer()
    cursor                  = conn.cursor()
    cursor.fast_executemany = True
    cant                    = df['fecha_ts'].count()
    print('Insertando ', cant , ' Datos en ' , tabla)
    cargados                = 0
    res_proc                = ''

    ####################################################################
    cols = ",".join([f"[{col}]" for col in df.columns])
    query = f"INSERT INTO {tabla} ({cols}) VALUES ({','.join(['?' for _ in df.columns])})"

    try:
      ########## Divide el dataframe en lotes #############
      truncate = f'''truncate table {tabla}'''
      cursor.execute(truncate)

      #####################################################
      for i in range(0, len(df), batch_size):
          batch = df.iloc[i:i+batch_size].values.tolist()
          cursor.executemany(query, batch)

      conn.commit()
      cursor.close()

    except Exception as e:
      logger.error(f'Error al insertar datos en {tabla} : {e}', exc_info=True)

######################################################################
#   def generaTablaAct(self):
#     diccionario = {}
#     BBDD        =  f'''[{database}].'''
#     nombre_tabla,fecha_data,cantidad_total = [],[],[]
#     cargados,dif,estado,fecha_carga   = [],[],[],[]
    
#     try:      
#       tablasAct = f''' '{tabla_trans.replace('.[t_paso_ctr_op]','')}' , '{tabla_audit.replace('.[t_paso_ctr_op]','')}'
#                     , '{tabla_tefabm.replace('.[t_paso_ctr_op]','')}' '''

#       query = f'''SELECT [nombre_tabla],[fecha],[cantidad_total]
#                 ,[cargados],[dif],[estado],[fecha_carga]
#                 FROM [prod_ctrl_contable].[ctrl_op_contable].[log_tablas_cargadas]
#                 where convert(date,fecha_carga,103) = convert(date,getdate() ,103) 
#                 and nombre_tabla in ({tablasAct}) 
#                 order by [fecha_carga] desc '''

#       selectQuery = conexiones.querySql(query, 'select all')

#       for row in selectQuery:
#         nombre_tabla.append(row[0].replace(BBDD,''))
#         fecha_data.append(row[1])
#         cantidad_total.append(row[2])
#         cargados.append(row[3])
#         dif.append(row[4])
#         estado.append(row[5])
#         fecha_carga.append(row[6])

#       diccionario.update({'nombre_tabla': nombre_tabla,'fecha': fecha_data,'cantidad_total': cantidad_total,
#                           'cargados': cargados,'dif': dif,'estado': estado,'fecha_carga': fecha_carga})

#       df = pd.DataFrame.from_dict(diccionario)
      
#       df.to_csv(carpetacsv+'tabla_actualizada.csv', sep=';')
#       print(df)

#     except Exception as e:
#       print(e)


import pandas as pd
import numpy as np
import datetime
from functools import wraps
import os
import time
from datetime import datetime

from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas import log_tabla as log

import warnings
import configparser

## ---------------config ini ------------------------- ##
config = configparser.ConfigParser()
config.read('config.ini')

## ---------------------------------------------------- ##
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()
fecha_actual = fecha_hoy.strftime("%d-%m-%Y %H:%M:%S")
## ----------------------------------------------- ##
conexiones = conexionSQL()
log_carga = log.registroLOGTabla()
warnings.filterwarnings('ignore')
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
## ----------------------------------------------- ##
carpetacsv             =  f'''D:\\Users\\jcarrion.externo\\Documents\\Python Scripts(Prod)\\Audit-trans\\datos\\archivocsv\\'''
carpetalog             =  f'''D:\\Users\\jcarrion.externo\\Documents\\Python Scripts(Prod)\\Audit-trans\\log_cargas\log\\'''
## ----------------------------------------------- ##
class leerTrans:
  def __init__(self,fecha, dias):
    self.fecha = fecha
    self.dias  = dias
    self.dia   = fecha.day
    self.mes   = fecha.month
    self.año   = fecha.year

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

  ######################################################################
  @mide_tiempo
  def leerBBDDtefabm(self):
      print('leerBBDDtefabm')
    # c1    = conexiones.conexion_contingencia().cursor()
    # query = f''' select a.TEFRUE,a.TEFRSP,a.TEFTAM,a.TEFDAM,a.TEFFAN
    # ,a.TEFRBF,a.TEFNBF,a.TEFBCO,a.TEFCTA,a.TEFTCT,a.TEFMTR
    # ,a.TEFREF,a.TEFESF,a.TEFEMD,a.TEFITF,a.TEFTRN,a.TEFHOR
    # ,a.TEFPRC,a.TEFFL1,a.TEFFL2,a.TEFFL3,a.TEFFL4,a.TEFFL5
    # ,a.TEFFL6,a.TEFLMM,a.TEFLMD,a.TEFLMY

    # from prdhifiles.tefabm AS a 
    # '''
    # c1.execute(query)
    # c1 = c1.fetchall()
    # ################################################
    # diccionario = {}
    # ######## se definen listas #####################
    # tefrue,tefrsp,teftam,tefdam,teffan,tefrbf,tefnbf,tefbco,tefcta,teftct = [],[],[],[],[],[],[],[],[],[]
    # tefmtr,tefref,tefesf,tefemd,tefitf,teftrn,tefhor,tefprc,teffl1,teffl2 = [],[],[],[],[],[],[],[],[],[]
    # teffl3,teffl4,teffl5,teffl6,teflmm,teflmd,teflmy =  [],[],[],[],[],[],[]

    # for row in c1:
    #   tefrue.append(row[0])
    #   tefrsp.append(row[1])
    #   teftam.append(row[2])
    #   tefdam.append(row[3])
    #   teffan.append(row[4])
    #   tefrbf.append(row[5])
    #   tefnbf.append(row[6])
    #   tefbco.append(row[7])
    #   tefcta.append(row[8])
    #   teftct.append(row[9])
    #   tefmtr.append(row[10])
    #   tefref.append(row[11])
    #   tefesf.append(row[12])
    #   tefemd.append(row[13])
    #   tefitf.append(row[14])
    #   teftrn.append(row[15])
    #   tefhor.append(row[16])
    #   tefprc.append(row[17])
    #   teffl1.append(row[18])
    #   teffl2.append(row[19])
    #   teffl3.append(row[20])
    #   teffl4.append(row[21])
    #   teffl5.append(row[22])
    #   teffl6.append(row[23])
    #   teflmm.append(row[24])
    #   teflmd.append(row[25])
    #   teflmy.append(row[26])

    # diccionario.update({'tefrue':tefrue,'tefrsp':tefrsp,'teftam':teftam,'tefdam':tefdam,'teffan':teffan
    #                     ,'tefrbf':tefrbf,'tefnbf':tefnbf,'tefbco':tefbco,'tefcta':tefcta,'teftct':teftct
    #                     ,'tefmtr':tefmtr,'tefref':tefref,'tefesf':tefesf,'tefemd':tefemd,'tefitf':tefitf
    #                     ,'teftrn':teftrn,'tefhor':tefhor,'tefprc':tefprc,'teffl1':teffl1,'teffl2':teffl2
    #                     ,'teffl3':teffl3,'teffl4':teffl4,'teffl5':teffl5,'teffl6':teffl6,'teflmm':teflmm
    #                     ,'teflmd':teflmd,'teflmy':teflmy})

    # df = pd.DataFrame.from_dict(diccionario)
    # ## ------------------------------------------ ##
    # df_datatype = {'tefrue':str,'tefrsp':str,'teftam':str,'tefdam':str,'teffan':str
    #               ,'tefrbf':str,'tefnbf':str,'tefbco':str,'tefcta':str,'teftct':str
    #               ,'tefmtr':str,'tefref':str,'tefesf':str,'tefemd':str,'tefitf':str
    #               ,'teftrn':str,'tefhor':str,'tefprc':str,'teffl1':str,'teffl2':str
    #               ,'teffl3':str,'teffl4':str,'teffl5':str,'teffl6':str,'teflmm':str
    #               ,'teflmd':str,'teflmy':str}

    # df = df.replace('  ','', regex=True)

    # #.str.replace(' ', '', regex=True)
    # df['fecha_ts'] = fecha_actual
    # df = df.astype(df_datatype)
    # df.to_csv('test.csv', sep=';')
    # ## ------------------------------------------ ##
    # self.insert_with_batch(df, tabla_tefabm)

    # return df


#   ######################################################################
#   @mide_tiempo
#   def leerBBDDtrans(self):
#     c1 = conexiones.conexion_contingencia().cursor()
#     query = f''' SELECT 'TEF' Origen,TO_DATE(CASE WHEN LENGTH(CAST( A.TRABDD AS VARCHAR(2))) = 1 THEN 
#     0||A.TRABDD ELSE VARCHAR(A.TRABDD) END  ||CASE WHEN LENGTH(CAST(A.TRABDM AS VARCHAR(2))) = 1 THEN 
#     0||A.TRABDM ELSE VARCHAR(A.TRABDM) END ||CHAR(A.TRABDY),'DDMMYYYY') AS FECHA,A.TRABRN OFICINA
#     ,A.TRAGLN CTACTBLE,D.GLMDSC NOMCUENTA,A.TRAACC NUMEROCTA,A.TRANAR GLOSA
#     ,Case When A.Tradcc = 0 then A.Traamt else 0 END DEBITO,Case When A.Tradcc = 5 then A.Traamt else 0 END CREDITO
#     ,Case When A.Tradcc = 5 then A.Traamt * - 1 else Traamt END *-1 Monto
#     ,A.TRATMS Fechahora,A.TRACKN ID,repeat ('0' , 12 - length(TRIM(C.AUDRFN))) || C.AUDRFN  TRACE_ID
#     ,A.trabdy,A.trabdm,A.trabdd,cusidn AS rut

#     FROM prdcyfiles.TRANS AS A
#     LEFT JOIN prdcyfiles.GLMST AS D ON A.TRACCY = D.GLMCCY AND A.TRAGLN = D.GLMGLN 
#     LEFT JOIN PRDCYFILES.AUDHI AS C ON C.audckn = A.trackn AND C.AUDMNT = A.traamt 
#     and A.tratms = C.audtms AND A.traacc = C.AUDOAC and C.audtcd IN (610,611,613,614) and AUDFLG = 'A' and AUDAPF = 'A'
#     LEFT JOIN PRDCYFILES.CUMST on tracun = cuscun
#     WHERE  A.trabdy = {self.año}  and A.trabdm in {self.mes} and A.trabdd = {self.dia} --fecha contable
#     and A.trabth in  (59004) and not A.tragln IN (2110990110013400,2115990110013800,2110990110013300)
#     UNION ALL
#     SELECT
#     'Rem' Origen,TO_DATE(CASE WHEN LENGTH(CAST( A.TRABDD AS VARCHAR(2))) = 1 THEN
#     0||A.TRABDD ELSE VARCHAR(A.TRABDD) END  ||
#     CASE WHEN LENGTH(CAST(A.TRABDM AS VARCHAR(2))) = 1 THEN 
#     0||A.TRABDM ELSE VARCHAR(A.TRABDM) END ||
#     CHAR(A.TRABDY),'DDMMYYYY') AS FECHA,A.TRABRN OFICINA,A.TRAGLN CTACTBLE,D.GLMDSC NOMCUENTA,A.TRAACC NUMEROCTA,A.TRANAR GLOSA
#     ,Case When A.Tradcc = 0 then A.Traamt else 0 END DEBITO
#     ,Case When A.Tradcc = 5 then A.Traamt else 0 END CREDITO
#     ,Case When A.Tradcc = 5 then A.Traamt *   - 1 else Traamt END *-1 Monto
#     ,A.TRATMS Fechahora,A.TRACKN ID,repeat ('0' , 12 - length(TRIM(A.traacc))) || A.traacc  TRACE_ID
#     ,A.trabdy,A.trabdm,A.trabdd,'0' RUT
#     FROM PRDCYFILES.trans AS A
#     LEFT JOIN prdcyfiles.GLMST AS D ON A.TRACCY = D.GLMCCY AND A.TRAGLN = D.GLMGLN 
#     WHERE TRABDY = {self.año} AND TRABDM = {self.mes} and A.trabdd = {self.dia} AND TRAGLN = 2115990110013800 AND TRABTH = 59014
#     limit 1000
#     '''

#     c1.execute(query)
#     c1 = c1.fetchall()
#     ################################################
#     diccionario = {}
#     ######## se definen listas #####################
#     origen,fecha,oficina,ctactble,nomcuenta,numerocta,glosa  = [],[],[],[],[],[],[]
#     debito,credito,monto,fechahora,id,trace_id,trabdy,trabdm = [],[],[],[],[],[],[],[]
#     trabdd ,rut        = [],[]

#     for row in c1:
#       origen.append(row[0])
#       fecha.append(row[1])
#       oficina.append(row[2])
#       ctactble.append(row[3])
#       nomcuenta.append(row[4])
#       numerocta.append(row[5])
#       glosa.append(row[6])
#       debito.append(row[7])
#       credito.append(row[8])
#       monto.append(row[9])
#       fechahora.append(row[10])
#       id.append(row[11])
#       trace_id.append(row[12])
#       trabdy.append(row[13])
#       trabdm.append(row[14])
#       trabdd.append(row[15])
#       rut.append(row[16])

#     diccionario.update({'origen':origen,'fecha':fecha,'oficina':oficina,'ctactble':ctactble,'nomcuenta':nomcuenta
#                         ,'numerocta':numerocta,'glosa':glosa,'debito':debito,'credito':credito,'monto':monto
#                         ,'fechahora':fechahora,'id':id,'trace_id':trace_id,'trabdy':trabdy,'trabdm':trabdm
#                         ,'trabdd':trabdd,'rut':rut})

#     df = pd.DataFrame.from_dict(diccionario)

#     df_datatype = {'origen':str,'fecha':str,'oficina':str,'ctactble':str,'nomcuenta':str
#                   ,'numerocta':str,'glosa':str,'debito':str,'credito':str,'monto':str
#                   ,'fechahora':str,'id':str,'trace_id':str,'trabdy':str,'trabdm':str
#                   ,'trabdd':str,'rut':str}

#     df = df.astype(df_datatype)
#     df = df.replace('   ', '', regex=True)

#     df['fecha_ts'] = fecha_actual
#     df.to_csv('test.csv', sep=';')
#     self.insert_with_batch(df, tabla_trans)
#     return df

#   ######################################################################
#   @mide_tiempo
#   def leerBBDDaudit(self):
#     c1    = conexiones.conexion_contingencia().cursor()
#     query = f''' select TO_DATE(CASE WHEN LENGTH(CAST( AUDDTD AS VARCHAR(2))) = 1 THEN 
#       0||AUDDTD ELSE VARCHAR(AUDDTD) END  ||
#       CASE WHEN LENGTH(CAST(AUDDTM AS VARCHAR(2))) = 1 THEN 
#       0||AUDDTM ELSE VARCHAR(AUDDTM) END ||
#       CHAR(AUDDTY),'DDMMYYYY') AS FECHA,(Audflg)Estado 
#     ,(Audtlr)Cajero,(Audtbr)CodOficina,(Brnnme)NomOficina
#     ,(Audckn)NumDocto,(Audtcd)CodTrx,(Auddsc)Glosa,(Audmnt)Monto
#     ,(AUDOAC)CuentaSocio,(AUDDGL)CuentaDebito,(AUDCGL)CuentaCredito
#     ,(AUDCCY)Moneda,(audapf)Contabilizada
#     ,repeat ('0' , 12 - length(TRIM(AUDRFN))) || AUDRFN  TRACE_ID
#     ,(Audtms) AS fechahora,audtme AS hora,Cusidn AS rut

#     FROM prdCyfiles.AUDIT
#     ------------------------------------
#     LEFT JOIN prdCyfiles.CNTRLBRN ON  AUDTBR = BRNNUM
#     ------------------------------------
#     LEFT JOIN PRDCYFILES.ACMST ON acmacc = AUDOAC
#     ------------------------------------
#     LEFT JOIN PRDCYFILES.cumst on CUSCUN = acmcun 
#     ------------------------------------
#     where audtcd in (610,611,613,614)   and Audflg = 'A'
#     order by Audtms
#     '''

#     c1.execute(query)
#     c1 = c1.fetchall()
#     ################################################
#     diccionario = {}
#     ######## se definen listas #####################
#     fecha,estado,cajero,codoficina,nomoficina,numdocto,codtrx ,glosa  =  [],[],[],[],[],[],[],[]
#     monto,cuentasocio,cuentadebito,cuentacredito,moneda,contabilizada =  [],[],[],[],[],[]
#     trace_id,fechahora,hora,rut =  [],[],[],[]

#     for row in c1:
#       fecha.append(row[0])
#       estado.append(row[1])
#       cajero.append(row[2])
#       codoficina.append(row[3])
#       nomoficina.append(row[4])
#       numdocto.append(row[5])
#       codtrx.append(row[6])
#       glosa.append(row[7])
#       monto.append(row[8])
#       cuentasocio.append(row[9])
#       cuentadebito.append(row[10])
#       cuentacredito.append(row[11])
#       moneda.append(row[12])
#       contabilizada.append(row[13])
#       trace_id.append(row[14])
#       fechahora.append(row[15])
#       hora.append(row[16])
#       rut.append(row[17])

#     diccionario.update({'fecha':fecha,'estado':estado,'cajero':cajero,'codoficina':codoficina,'nomoficina':nomoficina
#                         ,'numdocto':numdocto,'codtrx':codtrx,'glosa':glosa,'monto':monto,'cuentasocio':cuentasocio
#                         ,'cuentadebito':cuentadebito,'cuentacredito':cuentacredito,'moneda':moneda,'contabilizada':contabilizada
#                         ,'trace_id':trace_id,'fecha_hora':fechahora,'hora':hora,'rut':rut})

#     df = pd.DataFrame.from_dict(diccionario)

#     df_datatype = {'fecha':str,'estado':str,'cajero':str,'codoficina':str,'nomoficina':str
#                         ,'numdocto':str,'codtrx':str,'glosa':str,'monto':str,'cuentasocio':str
#                         ,'cuentadebito':str,'cuentacredito':str,'moneda':str,'contabilizada':str
#                         ,'trace_id':str,'fecha_hora':str,'hora':str,'rut':str}
    
#     df = df.astype(df_datatype)
#     ### ----------------------------------------- ###
#     df['rut']   = df['rut'].str.replace(' ', '', regex=True)
#     df['glosa'] = df['glosa'].str.replace(' ', '', regex=True)

#     df['fecha_ts'] = fecha_actual
#     ### ----------------------------------------- ###
#     self.insert_with_batch(df,tabla_audit)
#     ##############################################################
#     return df

#   ######################################################################
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

      # if tabla == tabla_trans:
      #   exec_proc = f'''exec {proc_produccion_trans}'''
      #   cursor.execute(exec_proc)

      # elif tabla == tabla_audit:
      #   exec_proc = f'''exec {proc_produccion_audit}'''
      #   cursor.execute(exec_proc)

      # elif tabla == tabla_tefabm:
      #   exec_proc = f'''exec {proc_produccion_tefabm}'''
      #   cursor.execute(exec_proc)

      conn.commit()
      cursor.close()

    except Exception as e:
      print(e)



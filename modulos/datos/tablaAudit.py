import pandas as pd
from datetime import datetime
from modulos.repository.sql_repository import SQLRepository
## ------------- librerias personalizadas ------------ ##
from .leerarchivo import conexiones,fecha_actual,logger,tablasSQL,procSQL
## ----------------------------------------------- ##

######################################################################
class ClaseAudit:
  def __init__(self,repository: SQLRepository):
    self.repository = repository

######################################################################
  def leerBBDDaudit(self):
    try:
        logger.info(f'leyendo datos de leerBBDDaudit del dia {fecha_actual}')
            # ... lógica de lectura...
            # cambiar al servidor de contingencia
        c1    = conexiones.conexion_contingencia().cursor()
        query = f''' select TO_DATE(CASE WHEN LENGTH(CAST( AUDDTD AS VARCHAR(2))) = 1 THEN 
            0||AUDDTD ELSE VARCHAR(AUDDTD) END  ||
            CASE WHEN LENGTH(CAST(AUDDTM AS VARCHAR(2))) = 1 THEN 
            0||AUDDTM ELSE VARCHAR(AUDDTM) END ||
            CHAR(AUDDTY),'DDMMYYYY') AS FECHA,(Audflg) Estado
            ,(Audtlr) Cajero,(Audtbr) CodOficina,(Brnnme) NomOficina
            ,(Audckn) NumDocto,(Audtcd) CodTrx,(Auddsc) Glosa,(Audmnt) Monto
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
            limit 100
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
        # --------------------------------------------------#
        succes, message = self.repository.full_load_process(
            df=df,
            staging_table = tablasSQL['tabla_audit'],
            target_proc   = {
                    'carga_data'    :f"{procSQL['proc_prod_audit']}",
                }
            )

        if not succes:
            logger.error(f'Error en Carga Audit: {message} ')
        
        else:
            logger.info(f'proceso de carga audit terminado\n')
        
        return df
    except Exception as e:
      logger.error(f'leyendo datos de leerBBDDaudit : {e}', exc_info=True)
      
      dict_result = {'nombre_archivo': '','nombre_tabla': tablasSQL['tabla_audit'],
                            'fecha': datetime.now().strftime('%Y-%m-%d') ,
                            'fecha_query': datetime.now().strftime('%Y-%m-%d') ,
                            'cantidad_total': 0,
                            'cargados': 0,'dif': 0,'estado_proc': ''
                            ,'estado_proc': f"Error leyendo leerBBDDaudit {str(e)}"
                        }
 
      self.repository.load_log_table(dict_result)     

    ### ----------------------------------------- ###
    ##############################################################
    return df

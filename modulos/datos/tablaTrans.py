import pandas as pd
from modulos.repository.sql_repository import SQLRepository  # Importación clara
## ------------- librerias personalizadas ------------ ##
from .leerarchivo import conexiones,fecha_actual,logger,tablasSQL,procSQL,datetime
## ----------------------------------------------- ##

######################################################################
class Clasetrans:
    def __init__(self,repository:SQLRepository,fecha:datetime = fecha_actual):
        """
        Inicializa el transformador de datos
        Args:
            repository: Instancia de SQLRepository (obligatorio)
            fecha: Fecha para procesamiento (opcional, default=ahora)
            dias: Días a considerar (opcional, default=0)
        """
        self.repository = repository
        self.fecha = fecha
        self.dia   = fecha.day
        self.mes   = fecha.month
        self.año   = fecha.year

#   @mide_tiempo
    def leerBBDDtrans(self):
        try:
          # ... lógica de lectura...
          # cambiar al servidor de contingencia
            logger.info(f'leyendo datos de leerBBDDtrans del dia {self.fecha}')
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
            WHERE  A.trabdy = ?  and A.trabdm in (?) and A.trabdd = ? --fecha contable
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
            WHERE TRABDY = ? AND TRABDM in (?) and A.trabdd = ? AND TRAGLN = 2115990110013800 AND TRABTH = 59014
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
        # ------------------------------------------------------------#
            success, message = self.repository.full_load_process(
            df=df,
            staging_table = tablasSQL['tablaTrans'],
            target_proc   = procSQL['proc_prod_trans']
        )

            if not success:
                logger.error(f"Error en carga Trans:{message}")
            else:
                logger.info(f'proceso de carga Trans terminado\n')

            return df
        
        except Exception as e:
            logger.error(f'leyendo datos de leerBBDDtrans : {e}', exc_info=True)
            dict_result = {'nombre_archivo': '','nombre_tabla': tablasSQL['tablaTrans'],
                                    'fecha': datetime.now().strftime('%Y-%m-%d') ,
                                    'fecha_query': datetime.now().strftime('%Y-%m-%d') ,
                                    'cantidad_total': 0,
                                    'cargados': 0,'dif': 0,'estado_proc': ''
                                    ,'estado_proc': f"Error leyendo leerBBDDtrans {str(e)}"
                                }

            self.repository.load_log_table(dict_result)
            df = pd.DataFrame()
        return df


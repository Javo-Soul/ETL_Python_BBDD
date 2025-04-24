import pandas as pd
import datetime
from functools import wraps
import time
from datetime import datetime

## ------------- librerias personalizadas ------------ ##
from  modulos.conexionSQL.conexionBD2 import conexionSQL
from modulos.log_cargas import log_tabla as log
from modulos.log_cargas.log_config import logger
from modulos.log_cargas.log_tabla import registroLOGTabla
## ---------------config ini ------------------------- ##
import configparser
config = configparser.ConfigParser()
config.read('config.ini')
## ---------------------------------------------------- ##
fecha_hoy = datetime.now()
fecha_consulta = fecha_hoy.date()
fecha_actual = fecha_hoy.strftime("%d-%m-%Y %H:%M:%S")
## ----------------------------------------------- ##
conexiones    = conexionSQL()
log_carga     = log.registroLOGTabla()
registroTabla = registroLOGTabla
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


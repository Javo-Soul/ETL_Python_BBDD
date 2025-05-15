# repository.py
import pandas as pd
import datetime
import configparser
from functools import wraps
from datetime import datetime
from modulos.logs.log_config import logger
## --------- Leer archivo de configuración ---------- ##
from modulos.config_loader import cargar_config
config = cargar_config()
## -------------------------------------------------- ##

tablaLog = {
    'logTabla'  : config['sql']['tabla_log_tablas'],
    'logArchivo': config['sql']['tabla_log_archivos']
}

class class_utils:
    def __init__(self):
        pass

    #######################################################################
    @staticmethod
    def measure_time(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = datetime.now()
            result = func(*args, **kwargs)
            elapsed_time = datetime.now() - start_time
            logger.info(f"{func.__name__} executed in {elapsed_time.total_seconds():.2f} seconds")
            return result
        return wrapper

    #######################################################################    
    def _validar_y_formatear_fecha(self, fecha_str):
        """
        Valida y formatea una fecha string a formato YYYY-MM-DD
        Si no puede parsear, devuelve la fecha actual
        """
        if not fecha_str or pd.isna(fecha_str):
            return datetime.now().strftime('%Y-%m-%d')
        
        fecha_str = str(fecha_str).strip()
        
        # Lista de formatos de fecha a probar
        formatos_fecha = [
            '%Y-%m-%d',    # 2023-12-31
            '%d/%m/%Y',     # 31/12/2023
            '%m/%d/%Y',     # 12/31/2023
            '%Y%m%d',       # 20231231
            '%d-%m-%Y',     # 31-12-2023
            '%Y/%m/%d',    # 2023/12/31
            '%Y-%m-%d %H:%M:%S',  # Con hora
            '%d/%m/%Y %H:%M:%S'    # Con hora
        ]
        
        for formato in formatos_fecha:
            try:
                fecha_obj = datetime.strptime(fecha_str, formato)
                return fecha_obj.strftime('%Y-%m-%d')
            except ValueError:
                continue
        
        # Si ningún formato funcionó, usar fecha actual
        return datetime.now().strftime('%Y-%m-%d')

    #######################################################################
    def dict_generator(df:pd.DataFrame = pd.DataFrame(),tipo:str= "" , otros:dict = {}):
        """ 
        df    : DataFrame() dataframe vacio
        tipo  : str() tipo de dict que se necesite generar
        otros : dict() campos que se necesite agregar al dict
        """
        try:
            dict_result = {}

            if df.empty and tipo == 'log_archivo': 
                dict_result = {'nombre_archivo': None
                               ,'nombre_tabla': None
                               ,'fecha': datetime.now().strftime('%Y-%m-%d')
                               ,'cantidad_total': 0
                               ,'cargados': 0,'dif': 0,'estado_proc': 'archivo vacio'
                                }
            
                dict_result.update(otros)

            elif not df.empty and tipo == 'log_tabla':
                dict_result = {'nombre_archivo': None,'nombre_tabla': None,
                                        'fecha': datetime.now().strftime('%Y-%m-%d'),
                                        'fecha_query': datetime.now().strftime('%Y-%m-%d'),
                                        'cantidad_total': len(df) if isinstance(df, pd.DataFrame) else 0,
                                        'cargados': 0,'dif': 0,'estado_proc': ''
                                    }
                dict_result.update(otros)
        
            else:
                dict_result
            
            return dict_result

        except Exception as e:
            logger.error(f"Error Generando DF vacio : {str(e)}")

    # df = pd.read_csv('archivocsv/EXAP672250416145408.csv', sep=";")


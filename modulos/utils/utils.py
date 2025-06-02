# modulos/utils/utils.py
import pandas as pd
import datetime
from sqlalchemy import and_, or_
from functools import wraps
from datetime import datetime
from modulos.logs.log_config import logger
## --------- Leer archivo de configuración ---------- ##

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

    def operadores():
        OPERADORES = {
            "==": lambda col, val: col == val,
            "!=": lambda col, val: col != val,
            ">":  lambda col, val: col > val,
            "<":  lambda col, val: col < val,
            ">=": lambda col, val: col >= val,
            "<=": lambda col, val: col <= val,
            "like": lambda col, val: col.like(val),
            "ilike": lambda col, val: col.ilike(val),
            "in": lambda col, val: col.in_(val if isinstance(val, list) else [val]),
        }

        return OPERADORES

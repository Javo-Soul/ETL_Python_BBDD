import os
import configparser
import logging
import logging.handlers
from pathlib import Path
import pandas as pd
from datetime import datetime
import configparser
from config.settings import settings

## -------------------------------------------------- ##
######################################################################
class registroLOG:
    def setup_logging(log_name= settings.archivo_log, log_level=logging.INFO):
        logs_dir = Path(__file__).resolve().parents[2]/ 'logs'
        logs_dir.mkdir(exist_ok=True)

        logger = logging.getLogger(log_name)

        if not logger.handlers:  # <- Esta línea evita duplicar handlers
            logger.setLevel(log_level)

            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)

            file_handler = logging.handlers.RotatingFileHandler(
                filename=logs_dir / f'{log_name}.log',
                maxBytes=10*1024*1024,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setFormatter(formatter)

            logger.addHandler(console_handler)
            logger.addHandler(file_handler)

        return logger

logger = registroLOG.setup_logging()


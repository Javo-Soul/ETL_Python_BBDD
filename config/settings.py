# config/settings.py
from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr
from typing import Optional
import logging


class SQLSettings(BaseSettings):
    ### database config origen
    db_host_sql:Optional[str]       = Field(None,env = 'DB_HOST_SQL')
    db_port_sql:Optional[str]       = Field(None,env = 'DB_PORT_SQL')
    db_sql:Optional[str]            = Field(None,env = 'DB_SQL')
    db_user_sql:Optional[str]       = Field(None,env = 'DB_USER_SQL')
    db_pass_sql:Optional[SecretStr] = Field(None,env = 'DB_PASS_SQL')

    #tablas para leer
    db_tabla_sql:Optional[str] = Field(None,env = 'DB_TABLA_SQL')

class PostgresSettings(BaseSettings):
    ### database config destino
    postgress_host:Optional[str]       = Field(None,env = 'POSTGRESS_HOST')
    postgress_port:Optional[str]       = Field(None,env = 'POSTGRESS_PORT')
    postgress_db:Optional[str]         = Field(None,env = 'POSTGRESS_DB')
    postgress_user:Optional[str]       = Field(None,env = 'POSTGRESS_USER')
    postgress_pass:Optional[SecretStr] = Field(None,env = 'POSTGRESS_PASS')


class Settings(BaseSettings):
    environment:str = Field('production', env="ENVIRONMENT")
    sql:SQLSettings = SQLSettings()
    postgres: PostgresSettings = PostgresSettings()
    class Config:
        env_file = '.env'
        env_file_coding = 'utf-8'
        extra = 'ignore' 

settings = Settings()

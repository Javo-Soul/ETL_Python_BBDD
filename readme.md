✅ README.md — PostgreSQL + pgAdmin con Docker
markdown
Copiar
Editar
# PostgreSQL + pgAdmin Docker Setup

Este proyecto levanta una instancia de PostgreSQL y pgAdmin usando Docker y `docker-compose`, con configuración a través de un archivo `.env`.
---

## 📁 Estructura del proyecto

ETL_Python_BBDD/
├── config/
│   ├── __init__.py
│   ├── settings.py            # se cargan todas las variables de entorno
│   ├── init/                  # carpeta con las .sql que usa docker
│   │   ├─── init.sql          # script sql      
│───├── logs/
│   │   ├───archivo.log        # archivo con los log de ejecucion
├── modulos/
│   ├── correo/
│   │   ├──__init__.py
│   │   ├──send_mail.py         # script que permite enviar un resultado por correo
├── data/
│   ├── __init__.py
│   ├── global_vars.py          # se cargan todas las variables de clases
│   ├── read_csv.py             # lee un archivo CSV
│   ├── read_sqlserver.py       # Lee una base de datos SQL Server
├── databaseClient/
│   ├── __init__.py
│   ├── client.py               # contiene los engine para conectarse a BBDD
├── logs/
│   ├── __init__.py
│   ├── log_config.py           # configuracion de logger
├── repository/
│   ├── __init__.py
│   ├── sql_repository.py       # maneja las inserciones a la base de datos
├── utils/
│   ├── __init__.py
│   ├── utils.py
---

## 🔧 Configuración

### Archivo `.env`

```env
ENVIRONMENT=test
DB_HOST_SQL=host
DB_PORT_SQL=1433
DB_SQL=database
DB_USER_SQL=user
DB_PASS_SQL=password
DB_TABLA_SQL=tabla_SQL

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_TABLA=data_test

# pgAdmin
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin123
PGADMIN_PORT=5050


🚀 Levantar los contenedores
Asegúrate de tener Docker instalado y ejecutando. Luego, en la raíz del proyecto:

bash
Copiar
Editar
docker compose up -d
Verifica que estén corriendo:

bash
Copiar
Editar
docker ps
🌐 Acceder a pgAdmin
URL: http://localhost:5050

Email: admin@admin.com
Password: admin123

🔌 Conectar a PostgreSQL desde pgAdmin
Entra a pgAdmin.

Crea un nuevo servidor.
Usa los siguientes datos:

Name: lo que quieras (ej: Postgres Local)
Host: postgres
Port: 5432
Username: postgres
Password: postgres123.

🧼 Apagar y limpiar
Para detener los contenedores:

bash
Copiar
Editar
docker compose down
Para borrar volúmenes (incluye la base de datos):

bash
Copiar
Editar
docker compose down -v

✅ Notas adicionales
Si cambias el puerto, asegúrate de actualizarlo también en pgAdmin y en el mapeo de ports.

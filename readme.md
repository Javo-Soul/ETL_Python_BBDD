# 🚀 ETL_Python_BBDD — PostgreSQL + pgAdmin con Docker

Este proyecto implementa un pipeline ETL en Python (3.12.3) que se conecta a distintas bases de datos, con configuración por variables de entorno. También incluye el levantamiento de PostgreSQL y pgAdmin usando Docker y `docker-compose`.

---

## 📁 Estructura del Proyecto
```
ETL_Python_BBDD/
├── config/
│ ├── init.py
│ ├── settings.py # Carga variables de entorno
│ └── init/
│ └── init.sql # Script SQL usado por Docker
├── logs/
│ └── archivo.log # Log de ejecución
├── modulos/
│ └── correo/
│ ├── init.py
│ └── send_mail.py # Envío de correos con resultados
├── data/
│ ├── init.py
│ ├── global_vars.py # Variables globales para el ETL
│ ├── read_csv.py # Lectura de archivos CSV
│ └── read_sqlserver.py # Lectura desde SQL Server
├── databaseClient/
│ ├── init.py
│ └── client.py # Motores de conexión a BBDD
├── logs/
│ ├── init.py
│ └── log_config.py # Configuración de logger
├── repository/
│ ├── init.py
│ └── sql_repository.py # Inserciones a la base de datos
├── utils/
│ ├── init.py
│ └── utils.py # Funciones auxiliares
```
---

## 🔧 Configuración

### 📄 Archivo `.env`

Debes crear un archivo `.env` en la raíz del proyecto con las siguientes variables:

```env
ENVIRONMENT=test

# SQL Server
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
🐳 Levantar los contenedores
Asegúrate de tener Docker y Docker Compose instalados. Luego, en la raíz del proyecto, ejecuta:

docker compose up -d
Verifica que los contenedores estén activos:

bash
docker ps
🌐 Acceder a pgAdmin
URL: http://localhost:5050

Email: admin@admin.com

Password: admin123

🔌 Conectar a PostgreSQL desde pgAdmin
Inicia sesión en pgAdmin.

Crea un nuevo servidor.

Usa los siguientes datos de conexión:

Name: Postgres Local (o el que prefieras)
Host: postgres
Port: 5432
Username: postgres
Password: postgres123
🧼 Apagar y limpiar
Para detener los contenedores:

bash
docker compose down
Para detener y borrar volúmenes (incluye la base de datos):

bash
docker compose down -v
✅ Notas Adicionales
Si cambias el puerto de PostgreSQL o pgAdmin, actualízalo también en:

El archivo .env

El docker-compose.yml

La configuración del servidor en pgAdmin

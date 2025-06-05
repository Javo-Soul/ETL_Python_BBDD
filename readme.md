# 🚀 ETL_Python_BBDD — ETL con SQL Server, PostgreSQL y pgAdmin en Docker

Este proyecto implementa un pipeline ETL en Python (3.12.3) que se conecta a distintas bases de datos utilizando variables de entorno. El entorno incluye SQL Server, PostgreSQL y pgAdmin, todo gestionado mediante Docker y `docker-compose`.

> ⚠️ **Importante**: Este entorno está pensado solo para desarrollo. No utilices las credenciales por defecto en entornos de producción.

---

## 🧩 Tecnologías usadas

- **Python 3.12.3**
- **SQL Server 2019**
- **PostgreSQL 16**
- **pgAdmin 4**
- **Docker & Docker Compose**

---

## ⚙️ Requisitos previos

- Docker Desktop instalado
- Docker Compose
- Git (opcional)
- 4 GB o más de RAM disponibles para Docker

---

## ⚡ Configuración rápida

1. Clona el repositorio:
   ```bash
   git clone https://github.com/Javo-Soul/ETL_Python_BBDD.git
   cd ETL_Python_BBDD
   ```

2. Inicia los contenedores:
   ```bash
   docker-compose build --no-cache sqlserver

   docker-compose up -d
   
   # si algo de esto falla, ve mas abajo a ❗Solución de problemas
   ```

3. Espera 2–3 minutos a que todos los servicios se inicialicen por completo.
---

## 📁 Estructura del proyecto

```
ETL_Python_BBDD/
├── docker-compose.yml
├── Dockerfile.mssql
├── init/
│   ├── postgres-init/         # Scripts SQL para PostgreSQL
│   │   └── init.sql
│   └── sqlserver-init/        # Scripts SQL para SQL Server
│       ├── entrypoint.sh
│       └── init.sql
├── config/
│   ├── __init__.py
│   └── settings.py            # Carga variables de entorno
├── logs/
│   └── archivo.log            # Log de ejecución
├── services/
│   ├── send_mail.py           # Envío de correos con el resultado del programa
│   └── data_sync_service.py   # Actualiza los datos en la base de datos PostgreSQL
├── modulos/
│   ├── bootstrap/
│   │   └── initializer.py     # Inicialización del sistema
│   ├── correo/
│   │   └── send_mail.py       # Envío de correos
│   ├── data/
│   │   ├── global_vars.py     # Variables globales para el ETL
│   │   ├── read_csv.py        # Lectura de archivos CSV
│   │   └── read_sqlserver.py  # Lectura desde SQL Server
│   ├── databaseClient/
│   │   └── client.py          # Motores de conexión a bases de datos
│   ├── logs/
│   │   └── log_config.py      # Configuración de logging
│   ├── repository/
│   │   └── sql_repository.py  # Inserciones a la base de datos
│   └── utils/
│       └── utils.py           # Funciones auxiliares
```

---

## 🔄 Flujo del ETL

1. **Extract**: Se obtienen datos desde SQL Server.
2. **Transform**: Se procesan con Python (p. ej., limpieza, enriquecimiento).
3. **Load**: Los datos se insertan en PostgreSQL.
4. **Notificación**: Se envía un correo con el resultado del proceso.

---

## 🛠️ Configuración personalizada

### 📄 Archivo `.env`

Edita el archivo `.env` con tus valores:

```env
# Entorno
ENVIRONMENT=test

# SQL Server
DB_HOST_SQL=localhost
DB_PORT_SQL=1433
DB_SQL=database_test
DB_USER_SQL=python_dev
DB_PASS_SQL=python_dev123.
DB_TABLA_SQL=data_test

# PostgreSQL
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres123.
POSTGRES_TABLA=data_test

# pgAdmin
PGADMIN_DEFAULT_EMAIL=admin@admin.com
PGADMIN_DEFAULT_PASSWORD=admin123
PGADMIN_PORT=5050
```

### Agregar scripts de inicialización

- **SQL Server**: Coloca tus scripts en `init/sqlserver-init/`
- **PostgreSQL**: Coloca tus scripts en `init/postgres-init/`

---

## 🧪 Servicios disponibles

### ✅ SQL Server 2019
- Puerto: `1433`
- Usuario SA: `sa`
- Contraseña: `SqlServer2024!`
- Usuario dev: `python_dev / python_dev123.`
- Base de datos inicial: `database_test`

### ✅ PostgreSQL 16
- Puerto mapeado: `5433` (interno 5432)
- Usuario: `postgres`
- Contraseña: `postgres123.`
- Base de datos: `postgres`

### ✅ pgAdmin 4
- URL: [http://localhost:5050](http://localhost:5050)
- Email: `admin@admin.com`
- Contraseña: `admin123`

#### 🔌 Conectar pgAdmin a PostgreSQL

1. Inicia sesión en pgAdmin.
2. Crea un nuevo servidor con:
   - **Name**: `Postgres Local`
   - **Host**: `postgres`
   - **Port**: `5432`
   - **Username**: `postgres`
   - **Password**: `postgres123.`

---

## 🧾 Comandos útiles

### Ver logs de SQL Server
```bash
docker logs -f sqlserver_container
```

### Conectarse a SQL Server desde contenedor
```bash
docker exec -it sqlserver_container /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "SqlServer2024!" -Q "SELECT name FROM sys.databases"
```

### Ejecutar un script SQL manualmente
```bash
docker exec sqlserver_container /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "SqlServer2024!" -i /docker-entrypoint-initdb.d/init.sql
```

### Reiniciar todo el entorno
```bash
docker-compose down -v
docker-compose up -d
```

---

## ❗ Solución de problemas

### No se crearon los usuarios y las tablas en SQL Server
1. Verifica manualmente: 
docker exec -it sqlserver_container ls -la /docker-entrypoint-initdb.d/

deberias ver algo asi :
drwxrwxrwx 1 root root 4096 Jun  4 23:37 .
drwxr-xr-x 1 root root 4096 Jun  5 00:33 ..
-rwxrwxrwx 1 root root  847 Jun  5 00:23 entrypoint.sh
-rwxrwxrwx 1 root root 2375 Jun  4 22:43 init.sql

2. Ejecuta manualmente el script: 

docker exec sqlserver_container /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "SqlServer2024!" -i /docker-entrypoint-initdb.d/init.sql

deberias ver algo asi :
Changed database context to 'master'.
Changed database context to 'database_test'.

### SQL Server no inicia
1. Verifica los logs:
   ```bash
   docker logs sqlserver_container
   ```

2. Asegúrate de que la contraseña cumple con los requisitos:
   - Al menos 8 caracteres
   - Una mayúscula, una minúscula, un número y un símbolo

3. Si el problema persiste:
   ```bash
   docker-compose down -v
   docker-compose up -d
   ```

---


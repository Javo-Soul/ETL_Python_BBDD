✅ README.md — PostgreSQL + pgAdmin con Docker
markdown
Copiar
Editar
# PostgreSQL + pgAdmin Docker Setup

Este proyecto levanta una instancia de PostgreSQL y pgAdmin usando Docker y `docker-compose`, con configuración a través de un archivo `.env`.
---

## 📁 Estructura del proyecto

.
├── .env
├── docker-compose.yml
└── README.md

yaml
Copiar
Editar

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

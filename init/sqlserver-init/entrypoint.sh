#init/sqlserver-init/entrypoint.sh
#!/bin/bash
set -e

# Iniciar SQL Server en segundo plano
/opt/mssql/bin/sqlservr &

# Esperar a que SQL Server esté realmente listo
echo "Waiting for SQL Server to be available..."
max_attempts=30
attempt=1
while [ $attempt -le $max_attempts ]; do
  if /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -Q "SELECT 1;" > /dev/null 2>&1; then
    echo "SQL Server is ready after $attempt attempts"
    break
  fi
  echo "Attempt $attempt/$max_attempts - SQL Server not ready yet..."
  sleep 2
  attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
  echo "SQL Server did not become available after $max_attempts attempts"
  exit 1
fi

# Ejecutar scripts SQL
for script in /docker-entrypoint-initdb.d/*.sql; do
  if [ -f "$script" ]; then
    echo "Executing initialization script: $script"
    /opt/mssql-tools/bin/sqlcmd -S localhost -U SA -P "$MSSQL_SA_PASSWORD" -i "$script" -b -r
    if [ $? -ne 0 ]; then
      echo "ERROR: Failed to execute $script"
      exit 1
    fi
  else
    echo "No SQL scripts found in /docker-entrypoint-initdb.d/"
    ls -la /docker-entrypoint-initdb.d/
  fi
done

# Mantener el contenedor en ejecución
wait
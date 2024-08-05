import snowflake.connector

# Configuración de la cuenta y credenciales
account = "ab35449.eu-west-1"
user = "ANGELNAVACERRADA"
password = "An@20240214"
warehouse = "COMPUTE_WH"
database = "DEV_XELIO"
schema = "RAW"

# Conexión a Snowflake
ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
)

cs = ctx.cursor()

# Leer el contenido del archivo snowproc.py
with open('snowproc.py', 'r') as file:
    procedure_code = file.read()

# Limpiar la indentación del código Python
procedure_code = "\n".join([line.rstrip() for line in procedure_code.split("\n")])

# Insertar el código Python en el procedimiento almacenado
create_procedure_sql = f"""
    CREATE OR REPLACE PROCEDURE filter_by_role_procedure(
        table_name STRING,
        role STRING
    )
    RETURNS TABLE(role STRING)
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    HANDLER = 'filter_by_role_git_deploy'
    AS
    $$
{procedure_code}
    $$
"""

print("SQL del Procedimiento:")
print(create_procedure_sql)

try:
    # Cambiar al warehouse, base de datos y esquema especificados
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")

    # Crear o reemplazar el procedimiento almacenado en Snowflake
    cs.execute(create_procedure_sql)
    print("Procedimiento creado exitosamente")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error al crear el procedimiento: {e}")
finally:
    cs.close()
    ctx.close()

import snowflake.connector

# Configuración de conexión a Snowflake
account = 'in12547'
user = 'ANGELNAVACERRADA'
password = 'e6MM$i2R'
warehouse = 'my_warehouse'
database = 'my_database'
schema = 'my_schema'

# Conectar a Snowflake
ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
)

cs = ctx.cursor()

# Leer el código del procedimiento desde el archivo
with open('snowproc.py', 'r') as file:
    procedure_code = file.read()

# Crear o reemplazar el procedimiento en Snowflake
try:
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")
    cs.execute(f"""
        CREATE OR REPLACE PROCEDURE filter_by_role_procedure(
            table_name STRING,
            role STRING
        )
        RETURNS TABLE(role STRING, column1 TYPE, column2 TYPE, ...)
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.8'
        HANDLER = 'filter_by_role'
        AS
        $$
        {procedure_code}
        $$
    """)
finally:
    cs.close()
    ctx.close()

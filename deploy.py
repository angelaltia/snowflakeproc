import snowflake.connector
# Configuración de la cuenta y credenciales
account = "ab35449.eu-west-1"
user = "ANGELNAVACERRADA"
password = "An@20240214"
warehouse = "COMPUTE_WH"
database = "DEV_XELIO"
schema = "DWH"

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
    PACKAGES = ('snowflake-snowpark-python')
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

##################################################################
with open('convert.py', 'r') as file:
    procedure_code = file.read()

procedure_code = "\n".join([line.rstrip() for line in procedure_code.split("\n")])

create_procedure_sql = f"""
    CREATE OR REPLACE PROCEDURE DEV_XELIO.DWH.CONVERT_PRICE("SERIE" VARCHAR(16777216), "INFLATION_SERIE_ACTUAL" VARCHAR(16777216), "ACTUAL_INFLATION_YEAR" NUMBER(38,0), "OLD_VERSION_INFLATION" VARCHAR(16777216), "INFLATION_SERIE" VARCHAR(16777216), "NEW_INFLATION_YEAR" NUMBER(38,0), "NEW_VERSION_INFLATION" VARCHAR(16777216), "NEW_UNIT_CURRENCY" VARIANT, "SERIE_CONVERSOR" VARCHAR(16777216), "VERSION_FXR" VARCHAR(16777216), "PSINPUT" VARCHAR(16777216), "CONDITIONAL" ARRAY)
    RETURNS TABLE ()
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    EXECUTE AS OWNER
    AS
    $$
{procedure_code}
    $$
"""

print("SQL del Procedimiento:")
print(create_procedure_sql)

try:
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")
    cs.execute(create_procedure_sql)
    print("Procedimiento creado exitosamente")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error al crear el procedimiento: {e}")

##################################################################
with open('captured.py', 'r') as file:
    procedure_code = file.read()

procedure_code = "\n".join([line.rstrip() for line in procedure_code.split("\n")])

create_procedure_sql = f"""
    CREATE OR REPLACE PROCEDURE DEV_XELIO.DWH.CALCULATE_SOLAR_CAPTURED_PRICES("SELECTED_PRICE_SERIE" VARCHAR(16777216), "SELECTED_VERSION_GREEN" VARCHAR(16777216), "V_GEOGRAPHIC" VARCHAR(16777216), "V_CURT_FIXED_TRIGGER" VARCHAR(16777216), "V_CURTAILMENT_VALUE" FLOAT, "V_MODE" VARCHAR(16777216))
    RETURNS TABLE ()
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    EXECUTE AS OWNER
    AS
    $$
{procedure_code}
    $$
"""

print("SQL del Procedimiento:")
print(create_procedure_sql)

try:
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")
    cs.execute(create_procedure_sql)
    print("Procedimiento creado exitosamente")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error al crear el procedimiento: {e}")

##################################################################
with open('spread.py', 'r') as file:
    procedure_code = file.read()

procedure_code = "\n".join([line.rstrip() for line in procedure_code.split("\n")])

create_procedure_sql = f"""
    CREATE OR REPLACE PROCEDURE DEV_XELIO.DWH.CALCULATE_SPREAD_PRICES("SELECTED_PRICE_SERIE" VARCHAR(16777216))
    RETURNS TABLE ()
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.11'
    PACKAGES = ('snowflake-snowpark-python')
    HANDLER = 'main'
    COMMENT='Caluclate Spread prices'
    EXECUTE AS OWNER
    AS
    $$
{procedure_code}
    $$
"""

print("SQL del Procedimiento:")
print(create_procedure_sql)

try:
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")
    cs.execute(create_procedure_sql)
    print("Procedimiento creado exitosamente")
except snowflake.connector.errors.ProgrammingError as e:
    print(f"Error al crear el procedimiento: {e}")
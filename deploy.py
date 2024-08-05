import snowflake.connector

account = "ab35449.eu-west-1"
user = "ANGELNAVACERRADA"
password = "An@20240214"
warehouse = "COMPUTE_WH"
database = "DEV_XELIO"
schema = "RAW"

ctx = snowflake.connector.connect(
    user=user,
    password=password,
    account=account
)

cs = ctx.cursor()

with open('snowproc.py', 'r') as file:
    procedure_code = file.read()
procedure_code = procedure_code.replace("'", "''")
try:
    cs.execute(f"USE WAREHOUSE {warehouse}")
    cs.execute(f"USE DATABASE {database}")
    cs.execute(f"USE SCHEMA {schema}")
    cs.execute(f"""
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
    """)
finally:
    cs.close()
    ctx.close()

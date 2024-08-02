import snowflake.connector
import os

# Configuración de conexión a Snowflake
ctx = snowflake.connector.connect(
    account = 'll47096.eu-west-3',
    user = 'ANGELNAVACERRADA',
    password = 'e6MM$i2R',
    warehouse = 'my_warehouse',
    database = 'my_database',
    schema = 'my_schema'
)

print("Conexión exitosa!")
ctx.close()

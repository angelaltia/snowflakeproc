import snowflake.connector
import os

ctx = snowflake.connector.connect(
    account = "ab35449.eu-west-1",
    user = "ANGELNAVACERRADA",
    password = "An@20240214",
    warehouse = "COMPUTE_WH",
    database = "DEV_XELIO",
    schema = "RAW"
)

print("Conexión exitosa!")
ctx.close()

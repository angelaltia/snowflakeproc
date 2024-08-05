import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col
def filter_by_role_git_deploy(session, table_name, role):
    df = session.table(table_name)
    return df.filter(col("role") == role)
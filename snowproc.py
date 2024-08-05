def filter_by_role_git_deploy(session, table_name):
    return session.table(table_name).limit(10)
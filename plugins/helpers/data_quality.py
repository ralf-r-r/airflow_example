def create_checks():
    sql_null_test = "SELECT COUNT(*) FROM {} WHERE {} is null"
    sql_nonempty_test = "SELECT COUNT(*) FROM {}"

    tables = ['users', '"time"', "songs", "songplays", "artists"]
    ids = ["userid", "start_time", "songid", "playid", "artistid"]
    quality_checks = []
    for k, table in enumerate(tables):
        quality_checks.append(
            dict(sql_query=sql_null_test.format(table, ids[k]),
                 expected_result={"condition":"==", "value":0},
                 error_message="Data quality check failed. {} is null in table {}".format(ids[k], table),
                 success_message="Data quality check successful. {} is not null in table {}".format(ids[k], table)
                 )
        )

    for k, table in enumerate(tables):
        quality_checks.append(
            dict(sql_query=sql_nonempty_test.format(table),
                 expected_result={"condition":">=", "value":0},
                 error_message="Data quality check failed. {} is empty".format(table),
                 success_message="Data quality check successful. {} is not empty".format(table)
                 )
        )

    return quality_checks
def foreach_batch_function(df, epoch_id):
    DRIVER_NAME = "org.postgresql.Driver"
    mode = "append"
    url = "jdbc:postgresql://localhost/agroprod?user=postgres&password=postgres"
    props = {"user": "postgres", "password": "postgres", "driver": DRIVER_NAME}
    df.write.jdbc(url=url, table='sample', mode=mode, properties=props)

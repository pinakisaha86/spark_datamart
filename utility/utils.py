

def read_from_mysql():
    df = spark \
        .read.format("jdbc") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .options(**jdbc_params) \
        .load()
    return df

def read_from_mongodb():
    return df

def read_from_sftp():
    return df

def read_from_s3():
    return df
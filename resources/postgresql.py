'''

setup postgresql



'''

url = "jdbc:postgresql://localhost:5432/test"

properties = {
    "user": "postgres",
    "password": "4xObAsU0",
    "driver": "org.postgresql.Driver"
}
df = spark.read.jdbc(url, "test_schema.test_table", properties=properties)

employees = [
    (1, "Scott", "Tiger", 1000.0,"united states", "+1 123 456 7890", "123 45 6789"),
    (2, "Henry", "Ford", 1250.0,"India", "+91 234 567 8901", "456 78 9123"),
    (3, "Nick", "Junior", 750.0,"united KINGDOM", "+44 111 111 1111", "222 33 4444"),
    (4, "Bill", "Gomes", 1500.0,"AUSTRALIA", "+61 987 654 3210", "789 12 6118")
]

df = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                    )



table_name = "test_schema.employees"
df2 = spark.read.jdbc(url, table_name, properties=properties)
df.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)

ROOT_DIR= "/Users/iong/IdeaProjects/crypto-trading"
files = f"{ROOT_DIR}/data/raw/exchanges/binance/csv/*/*.csv"
df = spark.read.csv(files, header=True, inferSchema=True)
df.write.jdbc(url=url, table="test_schema.raw_binance", mode="overwrite", properties=properties)

export PYSPARK_SUBMIT_ARGS="--jars /opt/spark/jars/postgresql-42.2.24.jar pyspark-shell"

sudo -u postgres psql test

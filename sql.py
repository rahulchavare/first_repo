#this is the sample file of python
from pyspark.sql import SparkSession
from pyspark.sql.funtions import *

spark=SparkSession.builder.appname("test").master("local").getOrCreate()
input_path="/class/op1/*"
host="jdbc:/connect.user"
user="puser"
pwd="pass123"
driver="org.apache.postgre.sql"
op_path="/user/hive/warehouse/dev.db/credit_card"
op_path2="/user/hive/warehouse/dev.db/debit_card"
df=spark.read.format("parquet").option("inferschema","true").load(input_path)
df.write.format("csv").option("header","true").save(op_path)
df_sql=spark.read.format("jdbc").option("url",host).option("username",user).option("password",pwd).option("driver",driver).option("dbtable","prod.debit_data").load()
df_sql.write.format("parquet").option("header","true").save(op_path_2)

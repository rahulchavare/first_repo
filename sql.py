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
df=spark.read.format("parquet").option("inferschema","true").load(input_path)
df.write.format("csv").option("header","true").save(op_path)


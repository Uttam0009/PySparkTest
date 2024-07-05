# Databricks notebook source
# DBTITLE 1,Mongodb Atlas Cluster dbs connection with databricks cluster
# reference analyticalvidhya.com/blog/2021/04/how-to-connect-databrics-and-mongodb-atlas-using-python-api, atlas cluster : video in data engineer youtube channal
# compute->select clusters->librery->Maven->search packages->spark pakages->'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1','HyukjinKwon:spark-xml:0.1.1-s_2.10'
# Mongodb-atlas->create project folder->create cluster->add network access->add databricks cluster ip or empty ip address 0.0.0.0->create db and table in atlas cluster
# cluster connect->Drivers->select add connection string->and copy this
# this sconnection string or compas connection string throug connect mongodb comapas with cluster and show dbs & tables


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

connectionString= "mongodb+srv://root-user:12345@cluster-1.fdkyxqj.mongodb.net/?retryWrites=true&w=majority&appName=Cluster-1"  

spark = SparkSession.builder \
    .config('spark.mongodb.input.uri',connectionString) \
    .config('spark.mongodb.input.uri', connectionString) \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1').getOrCreate()
    
# Reading from MongoDB ("com.mongodb.spark.sql.DefaultSource" or "mongo")
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", connectionString) \
    .option("database", "test-blog") \
    .option("collection", "posts") \
    .load()
df.show()


#write data into mongodb with appropriate schema
# schema = StructType([
#     StructField("name", StringType(), True),
#     StructField("city", StringType(), True),
#      StructField("age", IntegerType(), True)
# ])
# j_dict = [
#     {"name": "PATEL", "age": 49, "city": "Kheda"},
#     {"name": "BHAGATSHINGH", "city": "Chandigadha","age": 23},
#     {"name": "Gandhiji", "age": 50, "city": "Porbandar"}
# ]
# df = spark.createDataFrame(data=j_dict,schema=schema)
# df.printSchema()
# df.show()

# create and write new human table in test-blog db
# df.write.format("com.mongodb.spark.sql.DefaultSource") \
#     .option("uri", connectionString) \
#     .option("database", "test-blog") \
#     .option("collection", "human") \
#     .mode("overwrite") \
#     .save()

# read human table
df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
    .option("uri", connectionString) \
    .option("database", "test-blog") \
    .option("collection", "human") \
    .load()
df.printSchema()
df.show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Not connected mysql localhost
# dbutils.fs.ls("/FileStore/tables/")
# dbutils.fs.rm("/FileStore/tables/mysql_connector_java_8_0_27.jar")
# %sh ping 127.0.0.1  
# nc -zv 127.0.0.1 3306 # in linux terminal
# %fs rm -r dbfs:/FileStore/jars

# from pyspark.sql import SparkSession

# Create SparkSession
# spark = SparkSession.builder \
#     .appName('SparkByExamples.com') \
#     .config("spark.jars", "/dbfs/FileStore/jars/70e7b0e8_b414_4222_b4b5_afef8dee81b6-mysql_connector_java_8_0_13-4ac45.jar") \
#     .getOrCreate()

# spark = SparkSession.builder.appName('SparkByExamples.com').config("spark.jars", "/FileStore/tables/mysql_connector_java_8_0_13.jar").getOrCreate()

# Set Spark log level to DEBUG to capture detailed logs
# spark.sparkContext.setLogLevel("DEBUG")

# Read from MySQL Table using the correct IP address
# df = spark.read.format("jdbc") \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .option("url", "jdbc:mysql://127.0.0.1:3306/testdb") \
#     .option("dbtable", "books") \
#     .option("user", "root") \
#     .option("password", "123uttam") \
#     .load()

# df.show()



# jdbc_hostname = "127.0.0.1"
# jdbc_port = 3306
# jdbc_database = "testdb"
# jdbc_username = "root"
# jdbc_password = "123uttam"

# # JDBC URL
# jdbc_url = 'jdbc:mysql://localhost:3306/testdb'
# # f"jdbc:mysql://{jdbc_hostname}:{jdbc_port}/{jdbc_database}"

# # Path to the JDBC driver in DBFS
# driver_path = "/FileStore/tables/mysql_connector_j_8_3_0.jar"

# # Add the JDBC driver to the Spark context
# spark.sparkContext.addPyFile(driver_path)

# # Read data from MySQL into a DataFrame
# df = spark.read.format("jdbc") \
#     .option("url", jdbc_url) \
#     .option("dbtable", "books") \
#     .option("user", jdbc_username) \
#     .option("password", jdbc_password) \
#     .option("driver", "com.mysql.cj.jdbc.Driver") \
#     .load()

# # Show the first few rows of the DataFrame
# df.show()


# COMMAND ----------

# DBTITLE 1,Spark-SQL  notes
# %fs rm -r dbfs:/user # remove hive user folder
# primary key,foreign key not supported in spark-sql
# string = varchar in spark-sql 
# ALL Spark-SQL table is a DELTA table (part of Delta Lake)

# COMMAND ----------

# DBTITLE 1,Create table in Spark-SQL
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS default.Employee (
# MAGIC   EmpId INT NOT NULL,
# MAGIC   EmpName STRING,
# MAGIC   EmpBOD STRING,
# MAGIC   EmpJoiningDate STRING,
# MAGIC   PrevExperience INT,
# MAGIC   Salary INT,
# MAGIC   Address STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (1,"Laith Perry","Mar 29, 2020","Jan 31, 2019",2,64072,"990-9029 Duis Street");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (2,"Rudyard Coleman","Sep 14, 2019","Oct 15, 2018",1,98374,"7834 Tempus Road");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (3,"Adam Anthony","Jun 1, 2019","Apr 11, 2020",3,44817,"Ap #608-2097 Ultrices Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (4,"Malcolm Weeks","Dec 5, 2019","Jul 1, 2019",5,13820,"P.O. Box 710, 6880 Lacinia. St.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (5,"Reuben Montgomery","Mar 22, 2019","Apr 24, 2019",1,87591,"P.O. Box 620, 1484 Adipiscing Avenue");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (6,"Porter Wooten","Dec 19, 2019","Aug 30, 2020",5,60392,"543-3259 Ipsum Av.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (7,"Amal Ortiz","Dec 7, 2018","May 5, 2019",3,49310,"155 Magna Street");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (8,"Jerome Lewis","May 8, 2019","Jun 20, 2020",6,78689,"4654 Vel Avenue");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (9,"Wesley Church","May 24, 2019","Aug 13, 2019",9,16118,"P.O. Box 327, 5041 Metus. Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (10,"Ferdinand Nichols","Apr 21, 2020","Aug 26, 2020",4,51307,"Ap #500-5583 Ipsum. Av.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (11,"Chandler Curry","Aug 16, 2020","Dec 18, 2018",2,34767,"3362 Inceptos Road");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (12,"Caleb Patterson","Aug 3, 2020","Jul 5, 2020",3,29678,"7244 Semper Av.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (13,"Bruce Salas","Oct 17, 2019","Apr 15, 2020",1,45594,"P.O. Box 587, 6844 Sem. Av.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (14,"Ferdinand Herman","Sep 16, 2019","Mar 2, 2019",2,66991,"Ap #436-613 Feugiat Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (15,"Judah Graves","Feb 19, 2020","Jul 9, 2020",8,15595,"Ap #765-977 Senectus Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (16,"Stewart Tate","Jun 2, 2019","Apr 1, 2019",8,14861,"P.O. Box 665, 8517 Enim. Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (17,"Vance Kim","Mar 9, 2020","Jan 6, 2019",4,88918,"P.O. Box 286, 6066 Orci Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (18,"Caleb Parrish","Apr 15, 2019","Nov 6, 2019",4,38084,"947-6623 Auctor Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (19,"Jesse Clay","Jan 16, 2020","Feb 22, 2020",2,15332,"4789 Rhoncus. St.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (20,"Lee Leblanc","Nov 30, 2019","Apr 21, 2020",8,68228,"P.O. Box 179, 2688 Aliquam St.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (21,"Xenos Powers","Apr 2, 2019","Aug 19, 2020",6,83031,"Ap #795-9590 Consectetuer St.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (22,"Forrest Wiley","Mar 6, 2020","Mar 1, 2020",1,13814,"P.O. Box 327, 6168 Aliquam Road");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (23,"Burton Leon","Jul 3, 2020","Mar 16, 2019",3,98234,"3167 Rutrum. Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (24,"Benjamin Mcgowan","May 14, 2020","May 15, 2019",2,24450,"P.O. Box 732, 4057 Malesuada Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (25,"Melvin Cherry","Mar 24, 2020","Jun 27, 2019",4,18501,"Ap #900-8183 Tellus, Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (26,"Wallace Mcfadden","Jan 15, 2019","Sep 3, 2019",3,99971,"Ap #395-6277 Scelerisque, Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (27,"Timon Graham","Nov 25, 2019","Jan 15, 2020",10,65079,"P.O. Box 576, 9201 Vitae Avenue");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (28,"Gary Holloway","Aug 16, 2020","Aug 20, 2020",3,45830,"Ap #544-6653 Luctus, Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (29,"Murphy Richard","Oct 13, 2020","Mar 10, 2019",1,86270,"582-3044 Lectus. Ave");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (30,"Denton Diaz","Oct 26, 2018","Nov 21, 2019",6,70424,"689-9739 Lorem Road");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (31,"Erich Reilly","Dec 12, 2018","Mar 31, 2020",5,32165,"P.O. Box 220, 1359 Primis Rd.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (32,"Asher Perez","Jul 4, 2019","Jan 18, 2020",5,64260,"Ap #811-7962 Erat St.");
# MAGIC INSERT INTO default.Employee (`EmpId`,`EmpName`,`EmpBOD`,`EmpJoiningDate`,`PrevExperience`,`Salary`,`Address`) VALUES (33,"Tanner Sanchez","Sep 29, 2020","Apr 28, 2020",8,47765,"P.O. Box 378, 7803 Rhoncus Av.");
# MAGIC SELECT * FROM default.Employee

# COMMAND ----------

# DBTITLE 1,create database
# MAGIC %sql
# MAGIC -- DROP TABLE default.employee
# MAGIC -- DROP TABLE default.employeess
# MAGIC -- DROP TABLE IF EXISTS spark_sql.Employee;
# MAGIC -- DROP TABLE IF EXISTS spark_sql.employeess
# MAGIC -- CREATE DATABASE spark_sql;
# MAGIC
# MAGIC -- CREATE DATABASE dbjoin;
# MAGIC -- DROP TABLE IF EXISTS dbjoin.Shippers;
# MAGIC -- DROP TABLE IF EXISTS dbjoin.Orders;
# MAGIC -- DROP TABLE IF EXISTS dbjoin.Customer;
# MAGIC -- DROP TABLE IF EXISTS dbjoin.books;
# MAGIC -- DROP TABLE IF EXISTS dbjoin.books2;

# COMMAND ----------

# DBTITLE 1,SQL query 1
df = spark.read.table("default.Employee")   # read Spark-SQl table(delta table)
# df.printSchema()
# df.show()
# spark.sql("DESCRIBE EXTENDED default.employee").show()

# Add Employee table in spark_sql db as employeess using spark dataframe
df.write.mode("overwrite").saveAsTable("spark_sql.employeess")  # mode is only overwrite not replace table
df2 = spark.read.table("spark_sql.employeess")
# df2.printSchema()
# df2.show()


#UPDATE table using pyspark....
# withColumn same column updates is not possible in together. The issue arises because when you chain multiple withColumn transformations that target the same column. This can lead to the confusion seen in your error message.
from pyspark.sql.functions import when
updated_df = df2.withColumn("EmpName", when(df2["EmpId"]==1,"Uttam").otherwise(df2["EmpName"])) \
                .withColumn("Address", when(df2["EmpId"]==1,"Surat").otherwise(df2["Address"])) 
finaldf = updated_df.withColumn("Address", when(updated_df["Salary"]>90000,"Mumbai").otherwise(updated_df["Address"]))
 

finaldf.createOrReplaceTempView("emp")
spark.sql("DESCRIBE emp").show()
# spark.sql("SELECT * FROM emp").show(44)

#SELECT, WHERE, NOT, NOT IN, ASC, DESC, ORDER BY, BETWEEN, LIKE, DESCRIBE EXTENDED.....
# spark.sql("DROP TABLE emp").show()
# spark.sql("DESCRIBE EXTENDED emp").show()
# spark.sql('SELECT EmpId,EmpName,Salary FROM emp').show()
# spark.sql("SELECT * FROM emp WHERE PrevExperience==2").show()
# spark.sql("SELECT * FROM emp WHERE Salary>60000").show()
# spark.sql("SELECT EmpID,EmpName,EmpBOD,Salary FROM emp WHERE EmpID BETWEEN 30 AND 40").show()
# spark.sql("SELECT * FROM emp WHERE EmpName LIKE 'E%'").show()
# spark.sql("SELECT * FROM emp WHERE EmpName LIKE '%y'").show()
# spark.sql("SELECT * FROM emp WHERE EmpId IN(10,12,15)").show()
# spark.sql("SELECT * FROM emp ORDER BY Salary DESC").show()
# spark.sql("SELECT * FROM emp ORDER BY EmpId DESC, Salary ASC").show()
# spark.sql("SELECT * FROM emp WHERE EmpId > 20 AND (Salary > 80000 OR PrevExperience == 2)").show()
# spark.sql("SELECT * FROM emp WHERE EmpId > 27 AND PrevExperience = 1 OR Salary > 90000").show()
# spark.sql("SELECT * FROM emp WHERE EmpId < 10 OR EmpId > 30 OR Salary < 20000").show()
# spark.sql("SELECT * FROM emp WHERE NOT EmpName LIKE 'C%'").show()
# spark.sql("SELECT * FROM emp WHERE NOT Salary BETWEEN 15000 AND 90000").show()
# spark.sql("SELECT * FROM emp WHERE EmpId NOT IN (1,3,5,7,9,11,13,17,19,27)").show()


#INSERT INTO data temp view with original table, it is not possible....
# INSERT INTO data is possible in original Spark-SQL table(delta table)
# spark.sql("INSERT INTO emp (EmpId, EmpName, EmpBOD, EmpJoiningDate, PrevExperience, Salary, Address) VALUES (0, 'Raj', '2008-03-25', '2024-02-28', 1, 70000, 'Bhavanagar')") # get error
# spark.sql("INSERT INTO default.employee (EmpId,EmpName,EmpBOD,EmpJoiningDate,PrevExperience,Salary,Address) VALUES (155, 'Raj', '2008-03-25', '2024-02-28', 1, 70000, 'Bhavanagar')") # run because original table(delta table)


# delete, drop, update is not supported temp view only supported original table (delta table)...
#get error because this quey can only be used on permanent SQL tables, not temporary views.
# spark.sql("ALTER TABLE emp DROP COLUMN EmpId").show()

# we can delete column in spark dataframe another approuch
# finaldf.drop('Address').show()
# spark.sql("SELECT * FROM emp").drop('Address') # In tempview remove column using pyspark drop() fun, not sql query.

# all SPARK-SQL table is Delta table so we can not directly directy deleted as SQL table
# spark.sql("""ALTER TABLE default.employee SET TBLPROPERTIES (
#   'delta.minReaderVersion' = '2',
#   'delta.minWriterVersion' = '5',
#   'delta.columnMapping.mode' = 'name')""")

# spark.sql("ALTER TABLE default.employee DROP COLUMN EmpBOD") # drop column because original table (delta table)
# spark.sql("SELECT * FROM default.employee").show()
# spark.sql("SELECT * FROM spark_sql.employeess").show()


# COMMAND ----------

# DBTITLE 1,Insert, Delete  in Delta table using SQL
# MAGIC %sql
# MAGIC -- SELECT * FROM default.employee
# MAGIC -- SELECT EmpId,EmpName,EmpBOD,Salary FROM emp WHERE EmpID BETWEEN 20 AND 30;
# MAGIC
# MAGIC -- INSERT INTO default.employee (EmpId,EmpName,EmpBOD,EmpJoiningDate,PrevExperience,Salary,Address) VALUES (0, 'Raj', '2008-03-25', '2024-02-28', 1, 70000, 'Bhavanagar')
# MAGIC -- INSERT INTO default.employee (EmpId, EmpName, EmpBOD, EmpJoiningDate, PrevExperience, Salary, Address) VALUES
# MAGIC -- (35, 'Jay', '2008-03-25', '2024-02-28', 1, 70000, 'Surat'),
# MAGIC -- (36, 'Jay', '2008-03-25', '2024-02-28', 1, 70000, 'Surat'),
# MAGIC -- (37, 'AJay', '2008-03-25', '2024-02-28', 1, 170000, 'Dubai'),
# MAGIC -- (38, 'VIJay', '2010-03-25', '2022-02-28', NULL, 75000, 'Dubai'),
# MAGIC -- (39, NULL, NULL, NULL, NULL, 75000, NULL)
# MAGIC -- SELECT * FROM default.employee
# MAGIC
# MAGIC -- -- Set Delta Lake table properties
# MAGIC -- ALTER TABLE default.employee SET TBLPROPERTIES (
# MAGIC --   'delta.minReaderVersion' = '2',
# MAGIC --   'delta.minWriterVersion' = '5',
# MAGIC --   'delta.columnMapping.mode' = 'name'
# MAGIC -- );
# MAGIC
# MAGIC -- -- Drop the EmpJoiningDate column from the employee table
# MAGIC -- ALTER TABLE default.employee DROP COLUMN EmpJoiningDate;
# MAGIC -- SELECT * FROM default.employee
# MAGIC

# COMMAND ----------

# DBTITLE 1,SQL Query 2
# IS NOT NULL, IS NULL....
# spark.sql("SELECT count(*) FROM emp").show()
# spark.sql("SELECT * FROM emp").show(40)
# spark.sql("SELECT EmpId,EmpName,Salary FROM emp WHERE PrevExperience IS NULL").show()
# df = spark.sql("SELECT EmpId,EmpName,Salary FROM emp WHERE Address IS NOT NULL")

# employeessFilter delta table save in spark_sql db
# df.write.format("delta").mode("overwrite").saveAsTable("spark_sql.employeessFilter") 
# spark.read.table("spark_sql.employeessFilter").show()

# Save the DataFrame to a Parquet file at the specified path
# df.write.format("parquet").mode("overwrite").save("/FileStore/tables/employeesF")
# spark.read.parquet("/FileStore/tables/employeesF").show()


#LIMIT....
# spark.sql("SELECT * FROM emp LIMIT 3").show()
# spark.sql("SELECT * FROM emp WHERE Salary > 90000 ORDER BY Salary DESC LIMIT 3").show()

#MAX,MIN....
# spark.sql("SELECT max(Salary) FROM emp").show()
# spark.sql("SELECT min(Salary) AS minSalary FROM emp").show()
# spark.sql("SELECT min(Salary) AS minSalary FROM emp WHERE Salary > 90000").show()

#COUNT....
# spark.sql("SELECT count(EmpId) FROM emp").show()
# spark.sql("SELECT count(Address) AS citySurat FROM emp WHERE Address == 'Surat'").show()
# spark.sql("SELECT count(DISTINCT Salary) AS distinctsal FROM emp").show()

#SUM....
# spark.sql("SELECT sum(Salary) FROM emp").show()
# spark.sql("SELECT sum(Salary) AS SalaryEx1 FROM emp WHERE PrevExperience == 1").show()
# spark.sql("SELECT sum(Salary*2) AS SalaryDoubleDubai FROM emp WHERE Address == 'Dubai'").show()

#AVG....
# spark.sql("SELECT avg(Salary) FROM emp").show()
# spark.sql("SELECT avg(Salary) AS avgSalaryEx2 FROM emp WHERE PrevExperience == 2").show() 
# Return all products with a higher price than the average price:
# spark.sql("SELECT * FROM emp WHERE Salary > (SELECT avg(Salary) FROM emp)").show(40) 
# spark.sql("SELECT * FROM emp WHERE Salary > (SELECT avg(Salary) FROM emp WHERE PrevExperience == 10)").show(40)

#LIKE....
# spark.sql("SELECT * FROM emp WHERE Address LIKE 'S%'").show()
# spark.sql("SELECT * FROM emp WHERE Address LIKE '%ai'").show()
# spark.sql("SELECT * FROM emp WHERE Address LIKE 'D__ai'").show()
# spark.sql("SELECT * FROM emp WHERE EmpName LIKE 'J%y'").show()
# spark.sql("SELECT * FROM emp WHERE EmpName LIKE '%ja%'").show()

#IN, NOT IN....
# spark.sql("SELECT * FROM emp WHERE Address NOT IN ('Surat','Dubai')").show(40)
# spark.sql("SELECT * FROM emp WHERE Address IN ('Surat','Dubai')").show()
# spark.sql("SELECT * FROM emp WHERE EmpName IN (SELECT EmpName FROM emp WHERE Salary > 90000)").show()

#BETWEEN....
# spark.sql("SELECT * FROM emp WHERE Salary BETWEEN 70000 AND 90000").show()
# spark.sql("SELECT * FROM emp WHERE Salary BETWEEN 70000 AND 90000 AND Address IN ('Surat','Dubai')").show()
# spark.sql("SELECT * FROM emp WHERE Salary NOT BETWEEN 15000 AND 90000 ORDER BY Salary").show()
# spark.sql("SELECT * FROM emp WHERE EmpName BETWEEN 'Jay' AND 'VIJay' ORDER BY EmpName").show()
# spark.sql("SELECT * FROM emp WHERE EmpJoiningDate BETWEEN 'Mar 22, 2019' AND 'Sep 14, 2019'").show() #not correct output here

#ALIAS....
# spark.sql("SELECT Salary AS Vetan FROM emp").show()
# spark.sql("SELECT Salary AS Vetan, EmpName AS Kalakar FROM emp").show()
# spark.sql("SELECT EmpId, concat(Salary,', ',EmpName) AS One FROM emp").show()  #concat two columns using alias
# spark.sql("SELECT * FROM emp AS cmp").show()   #alias table temporary name
# spark.sql("SELECT e1.EmpName, e2.Salary FROM emp AS e1, emp AS e2 WHERE e1.EmpName = 'Jay' AND e1.EmpId = e2.EmpId").show()  #both are same result
# spark.sql("SELECT e1.EmpName, e2.Salary FROM emp AS e1 JOIN emp AS e2 ON e1.EmpId = e2.EmpId AND e1.EmpName ='Jay'").show()


# COMMAND ----------

# DBTITLE 1,SQL Create Delta tables
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS dbjoin.Shippers (
# MAGIC   ord_no INT,
# MAGIC   shipper_name STRING,
# MAGIC   phone_no STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- INSERT INTO dbjoin.Shippers (ord_no, shipper_name, phone_no) VALUES
# MAGIC -- ('70001', 'Federal Shipping', '(503) 555-9931'),
# MAGIC -- ('70002', 'United Package', '(503) 555-3199'),
# MAGIC -- ('70004', 'United Package', '(503) 555-3199'),
# MAGIC -- ('70005', 'Speedy Express', '(503) 555-9831'),
# MAGIC -- ('70007', 'Bharat Export', '(0261) 2475 9389'),
# MAGIC -- ('70009', 'Speedy Express', '(503) 555-9831'),
# MAGIC -- ('70010', 'Federal Shipping', '(503) 555-9931'),
# MAGIC -- ('70011', 'Bharat Export', '(0261) 2475 9389'),
# MAGIC -- ('70013', 'Speedy Express', '(503) 555-9831');
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbjoin.Orders (
# MAGIC   ord_no INT,
# MAGIC   purch_amt DECIMAL(10, 2),
# MAGIC   ord_date DATE,
# MAGIC   customer_id INT,
# MAGIC   salesman_id INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- INSERT INTO dbjoin.Orders (ord_no, purch_amt, ord_date, customer_id, salesman_id) VALUES
# MAGIC -- ('70001', '150.50', '2012-10-05', '3005', '5002'),
# MAGIC -- ('70002', '65.26', '2012-10-05', '3002', '5001'),
# MAGIC -- ('70003', '2480.40', '2012-10-10', '3009', '5003'),
# MAGIC -- ('70004', '110.50', '2012-08-17', '3009', '5003'),
# MAGIC -- ('70005', '2400.60', '2012-07-27', '3007', '5001'),
# MAGIC -- ('70007', '948.50', '2012-09-10', '3005', '5002'),
# MAGIC -- ('70008', '5760.00', '2012-09-10', '3002', '5001'),
# MAGIC -- ('70009', '270.65', '2012-09-10', '3001', '5005'),
# MAGIC -- ('70010', '1983.43', '2012-10-10', '3004', '5006'),
# MAGIC -- ('70011', '75.29', '2012-08-17', '3003', '5007'),
# MAGIC -- ('70012', '250.45', '2012-06-27', '3008', '5002'),
# MAGIC -- ('70013', '3045.60', '2012-04-25', '3002', '5001');
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbjoin.Customer (
# MAGIC   customer_id INT,
# MAGIC   cust_name VARCHAR(255),
# MAGIC   city VARCHAR(255),
# MAGIC   grade INT,
# MAGIC   salesman_id INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- INSERT INTO dbjoin.Customer (customer_id, cust_name, city, grade, salesman_id) VALUES
# MAGIC -- ('3001', 'Brad Guzan', 'London', NULL, '5005'),
# MAGIC -- ('3002', 'Nick Rimando', 'New York', '100', '5001'),
# MAGIC -- ('3003', 'Jozy Altidor', 'Moscow', '200', '5007'),
# MAGIC -- ('3004', 'Fabian Johnson', 'Paris', '300', '5006'),
# MAGIC -- ('3005', 'Graham Zusi', 'California', '200', '5002'),
# MAGIC -- ('3007', 'Brad Davis', 'New York', '200', '5001'),
# MAGIC -- ('3008', 'Julian Green', 'London', '300', '5002'),
# MAGIC -- ('3009', 'Geoff Cameron', 'Berlin', '100', '5003');
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbjoin.books (
# MAGIC   title VARCHAR(50),
# MAGIC   price INT,
# MAGIC   language VARCHAR(50),
# MAGIC   author VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- INSERT INTO dbjoin.books (title, price, language, author) VALUES
# MAGIC -- ('Meghdutam', 50, 'Hindi', 'Kalidas'),
# MAGIC -- ('Ramayana', 9000, 'Sanscrit', 'Maharshi Valmiki'),
# MAGIC -- ('Ramayana', 9000, 'Sanscrit', 'Maharshi Valmiki'),
# MAGIC -- ('Ramayana', 9000, 'Sanscrit', 'Maharshi Valmiki'),
# MAGIC -- ('Ramayana', 9000, 'Sanscrit', 'Maharshi Valmiki');
# MAGIC
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS dbjoin.books2 (
# MAGIC   id INT,
# MAGIC   title VARCHAR(50),
# MAGIC   price VARCHAR(50),
# MAGIC   language VARCHAR(50),
# MAGIC   author VARCHAR(50)
# MAGIC )
# MAGIC USING DELTA;
# MAGIC
# MAGIC -- INSERT INTO dbjoin.books2 (id, title, price, language, author) VALUES
# MAGIC -- ('1', 'Kadambari', '500', 'Sanscrit', 'Bhartuhari'),
# MAGIC -- ('2', 'Ramayana', '9000', 'Sanscrit', 'Maharshi Valmiki'),
# MAGIC -- ('2', 'Ramayana', '9000', 'Sanscrit', 'Maharshi Valmiki'),
# MAGIC -- ('2', 'Ramayana', '9000', 'Sanscrit', 'Maharshi Valmiki');
# MAGIC

# COMMAND ----------

# DBTITLE 1,SQL Query 3
# df = spark.sql("SELECT * FROM dbjoin.Shippers")
# df1 = spark.sql("SELECT * FROM dbjoin.Orders")
# df2 = spark.sql("SELECT * FROM dbjoin.Customer")
# df3 = spark.sql("SELECT * FROM dbjoin.books")
# df4 = spark.sql("SELECT * FROM dbjoin.books2")
# df.printSchema()
# spark.sql("SELECT * FROM dbjoin.Shippers").show()

# df.write.format("parquet").mode("overwrite").save("/FileStore/tables/pfShippers")
# df1.write.format("parquet").mode("overwrite").save("/FileStore/tables/pfOrders")
# df2.write.format("parquet").mode("overwrite").save("/FileStore/tables/pfCustomer")
# df3.write.format("parquet").mode("overwrite").save("/FileStore/tables/pfbooks")
# df4.write.format("parquet").mode("overwrite").save("/FileStore/tables/pfbooks2")
df = spark.read.parquet("/FileStore/tables/pfShippers")
df1 = spark.read.parquet("/FileStore/tables/pfOrders")
df2 = spark.read.parquet("/FileStore/tables/pfCustomer")
df3 = spark.read.parquet("/FileStore/tables/pfbooks")
df4 = spark.read.parquet("/FileStore/tables/pfbooks2")
# df3.show()

df.createOrReplaceTempView("shippers")
df1.createOrReplaceTempView("orders")
df2.createOrReplaceTempView("customer")
df3.createOrReplaceTempView("books1")
df4.createOrReplaceTempView("books2")

# spark.sql("SELECT * FROM customer").show()
# spark.sql("SELECT * FROM orders").show()
# spark.sql("SELECT * FROM shippers").show()


# SQL JOIN....
# A JOIN clause is used to combine rows from two or more tables, based on a related column between them.
# (INNER) JOIN: Returns records that have matching values in both tables
# LEFT (OUTER) JOIN: Returns all records from the left table, and the matched records from the right table
# RIGHT (OUTER) JOIN: Returns all records from the right table, and the matched records from the left table
# FULL (OUTER) JOIN: Returns all records when there is a match in either left or right table

#------(((( (INNER) JOIN....
# spark.sql("SELECT * FROM customer AS c INNER JOIN orders AS o ON c.customer_id = o.customer_id").show()
# spark.sql("SELECT * FROM customer INNER JOIN orders ON customer.salesman_id = orders.salesman_id").show()
# spark.sql("SELECT * FROM orders AS o INNER JOIN shippers AS s ON o.ord_no = s.ord_no ORDER BY o.ord_no").show()
# JOIN Three Tables
# spark.sql("SELECT c.customer_id, c.salesman_id, o.ord_no, c.cust_name, c.city, o.purch_amt, o.ord_date, s.shipper_name,\
            # s.phone_no FROM ((customer AS c INNER JOIN orders AS o ON c.customer_id = o.customer_id) \
                # INNER JOIN shippers AS s ON o.ord_no = s.ord_no) ORDER BY o.ord_no ").show()

#------>>>> LEFT (OUTER) JOIN....
# [SELECT * FROM table (table ni column first avse jo * hoy to,here left join apply table ma thase)
# AS t LEFT JOIN table2(table2 ma left join apply thase nahi) AS t2 ON t.id = t2.id]
# spark.sql("SELECT * FROM orders AS o LEFT JOIN shippers AS s ON o.ord_no = s.ord_no").show()
# spark.sql("SELECT * FROM shippers AS s LEFT JOIN orders AS o ON s.ord_no = o.ord_no ORDER BY s.ord_no DESC").show()
# spark.sql("SELECT * FROM shippers AS s LEFT JOIN orders AS o ON s.ord_no = o.ord_no WHERE o.purch_amt > 300.00 ORDER BY s.shipper_name").show()

#------<<<< RIGHT (OUTER) JOIN....
#[SELECT * FROM table (table ni column first avse jo * hoy to,here right join apply table ma thase nahi)
# AS t RIGH JOIN table2(table2 ma right join apply thase) AS t2 ON t.id = t2.id]
# spark.sql("SELECT * FROM orders AS o RIGHT JOIN customer AS c ON o.customer_id = c.customer_id").show()
# spark.sql("SELECT * FROM orders AS o RIGHT JOIN shippers AS s ON o.ord_no = s.ord_no").show()
# spark.sql("SELECT * FROM shippers AS s RIGHT JOIN orders AS o ON s.ord_no = o.ord_no ORDER BY s.ord_no DESC").show()
# spark.sql("SELECT o.ord_no, o.purch_amt, s.shipper_name, o.customer_id FROM shippers AS s RIGHT JOIN orders AS o ON s.ord_no = o.ord_no ORDER BY o.purch_amt").show()

#------)))) FULL (OUTER) JOIN....
# spark.sql("SELECT * FROM customer AS c FULL OUTER JOIN orders AS o ON c.salesman_id = o.salesman_id ORDER BY c.city").show()
# spark.sql("SELECT * FROM orders AS o FULL OUTER JOIN shippers AS s ON o.ord_no = s.ord_no ORDER BY s.shipper_name").show()
# spark.sql("SELECT o.customer_id, o.ord_no, s.phone_no, o.ord_date, o.purch_amt FROM orders AS o FULL OUTER JOIN shippers AS s ON o.ord_no = s.ord_no WHERE o.purch_amt < 900 ORDER BY o.ord_date").show()

#------()() SELF JOIN....
# spark.sql("SELECT * FROM shippers AS a, shippers AS b WHERE a.ord_no = b.ord_no").show()
# spark.sql("SELECT * FROM shippers AS a, shippers AS b WHERE a.ord_no <> b.ord_no").show() # <> means not equls
# spark.sql("SELECT a.ord_no, b.ord_no, a.shipper_name, b.shipper_name FROM shippers AS a, shippers AS b WHERE a.ord_no > b.ord_no AND a.shipper_name <> b.shipper_name AND a.shipper_name <> 'Speedy Express' ORDER BY a.shipper_name").show()


# COMMAND ----------

# DBTITLE 1,SQL Query 4
# spark.sql("SELECT * FROM books1").show()
# spark.sql("DESCRIBE EXTENDED books1").show()
b = spark.sql("SELECT * FROM books2").drop('id')
# b.show() # b is spark dataframe
# b.printSchema()
b.createOrReplaceTempView("b2")


# UNION,UNION ALL....
# UNION must have the same number of columns, columns must also have similar data types
# UNION operator selects only distinct values by default. To allow duplicate values, use UNION ALL
# spark.sql("SELECT * FROM books1 UNION SELECT * FROM b2").show()
# spark.sql("SELECT * FROM books1 UNION ALL SELECT * FROM b2").show()
# spark.sql("SELECT title, author FROM books1 UNION ALL SELECT title,author FROM books2").show()
# spark.sql("SELECT title, author FROM books1 WHERE author <> 'Kalidas' UNION SELECT title, author FROM books2 WHERE author <> 'Kalidas' ORDER BY title").show()


# GROUP BY....
# spark.sql("SELECT * FROM orders").show()
# spark.sql("SELECT * FROM customer").show()
# spark.sql("SELECT count(ord_date) AS count_date, sum(purch_amt) AS sum_amount, avg(purch_amt) AS avg_amount, \
        #  max(purch_amt) AS max_amount, min(purch_amt) AS min_amount, salesman_id FROM orders GROUP BY salesman_id ORDER BY count(ord_date) DESC").show() #unique salesman_id

# GROUP BY With JOIN(we can use)..
# spark.sql("SELECT o.salesman_id, o.customer_id, c.city FROM orders AS o LEFT JOIN customer AS c ON o.customer_id = \
        #   c.customer_id").show()
# spark.sql("SELECT count(o.salesman_id),count(c.city),o.customer_id FROM orders AS o LEFT JOIN customer AS c ON o.customer_id =\
        #   c.customer_id GROUP BY o.customer_id").show() #unique customer_id


# HAVING....
# The HAVING clause was added to SQL because the WHERE keyword cannot be used with aggregate functions.(aggregate fun no where ma use thai sakto nathi )
# here in HAVING ORDER BY COUNT(ord_date) is not supported only support alias name  

# spark.sql("SELECT count(ord_date) AS date FROM orders WHERE count(ord_date) > 2 GROUP BY salesman_id").show() # get error
# spark.sql("SELECT count(ord_date) AS date, max(purch_amt) AS max_amt, salesman_id FROM orders GROUP BY salesman_id \
        #   HAVING count(ord_date) > 2").show()
# spark.sql("SELECT count(ord_date) AS date, max(purch_amt) AS max_amt, salesman_id FROM orders GROUP BY salesman_id \
        #   HAVING count(ord_date) > 2 ORDER BY date").show()

# IN HAVING with JOIN, WHERE, GROUP BY, HAVING, ORDER BY..
# spark.sql("SELECT o.salesman_id, o.customer_id FROM orders AS o INNER JOIN customer AS c ON o.customer_id =\
        #  c.customer_id").show()
# spark.sql("SELECT count(o.salesman_id), o.customer_id FROM orders AS o INNER JOIN customer AS c ON o.cutomer_id =\
        #  c.customer_id GROUP BY o.customer_id").show()
# spark.sql("SELECT count(o.salesman_id) AS count_salesman_id, o.customer_id FROM orders AS o INNER JOIN customer AS c ON o.customer_id = c.customer_id GROUP BY o.customer_id HAVING count(o.salesman_id) > 1 ORDER BY count_salesman_id").show()

# spark.sql("SELECT count(o.salesman_id) AS count_salesman_id, o.customer_id FROM orders AS o INNER JOIN customer AS c ON o.customer_id = c.customer_id WHERE o.customer_id <> 3009 GROUP BY o.customer_id HAVING count(o.salesman_id) > 1 ORDER BY count_salesman_id").show()


#  EXISTS....
# The EXISTS operator is used to test for the existence of any record in a subquery.
# The EXISTS operator returns TRUE if the subquery returns one or more records.
# here EXISTS vali query ni condition mujab bahar vala table ni j selected column show thse.

# spark.sql("SELECT customer_id FROM customer WHERE EXISTS(SELECT * FROM orders WHERE customer.customer_id =  orders.customer_id)").show()
# spark.sql("SELECT customer_id FROM customer WHERE EXISTS(SELECT ord_date, purch_amt FROM orders WHERE customer.customer_id = orders.customer_id AND customer.city = 'London')").show()
# spark.sql("SELECT customer_id FROM customer WHERE EXISTS(SELECT * FROM orders WHERE customer.customer_id = orders.customer_id AND orders.purch_amt = 150.50)").show()
# spark.sql("SELECT customer_id FROM customer WHERE EXISTS(SELECT ord_date, purch_amt FROM orders WHERE customer.customer_id = orders.customer_id AND orders.ord_no = 70013)").show()


# ANY,ALL....
# The ANY,ALL operator might not be supported in your Spark SQL version, we can use join for same result
# spark.sql("SELECT city FROM customer WHERE slaesman_id = ANY(SELECT salesman_id FROM orders WHERE purch_amt > 500)").show()#get error
# spark.sql("SELECT city FROM customer WHERE slaesman_id = ALL(SELECT salesman_id FROM orders WHERE purch_amt > 500)").show()#get error
# spark.sql("SELECT c.city FROM customer c INNER JOIN orders o ON c.salesman_id = o.salesman_id WHERE o.purch_amt > 500").show()
# spark.sql("SELECT DISTINCT c.city FROM customer c INNER JOIN orders o ON c.salesman_id = o.salesman_id WHERE o.purch_amt > 500").show()


# SELECT INTO....
# The SELECT INTO statement copies data from one table into a new table.
# SELECT INTO syntax is not supported for creating a new table directly from another table. 
# spark.sql("SELECT * INTO abc FROM customer").show() #get error
# another way
# spark.sql("CREATE TABLE xyz AS SELECT * FROM customer").show() #stored automatically in spark-warehouse folder(here also 
# show in database table)
# spark.sql("SELECT * FROM xyz").show()

# The purpose of the spark-warehouse folder is to serve as a default location for storing the metadata and data of tables created within a Spark application. When you create tables using Spark SQL or DataFrame APIs without specifying an external location, Spark will automatically create the tables in this directory.(dbfs->user/hive/warehouse)
# It's important to note that the spark-warehouse directory is specific to Spark's internal management of metadata and data. 
# It's not intended for direct manipulation or access by users or external applications.


# INSERT INTO SELECT....
# The INSERT INTO SELECT statement copies data from one table and inserts it into another table.
# The INSERT INTO SELECT statement requires that the data types in source and target tables match.
# spark.sql("INSERT INTO books1 (title, price, language, author) SELECT title , CAST(price AS INT), language, author FROM b2 WHERE author='Maharshi Valmiki'") # insert Maharshi Valmiki's row in parquet file pfbooks from b2
# spark.sql("SELECT * FROM books1").show()


# CASE....
#The CASE expression goes through conditions and returns a value when the first condition is met (like an if-then-else statement). So, once a condition is true, it will stop reading and return the result. If no conditions are true, it returns the value in the ELSE clause.If there is no ELSE part and no conditions are true, it returns NULL.

# spark.sql("SELECT * FROM customer").show()
# spark.sql("DESCRIBE EXTENDED customer").show()
# spark.sql("SELECT cust_name,salesman_id, CASE WHEN salesman_id > 5005 THEN 'salesman id is greater then 5005' WHEN\
        #   salesman_id = 5005 THEN 'salesman id is equals to 5005' ELSE 'salesman id is less then 5005' END\
        #   AS QuantityText FROM customer").show(truncate=False)
# spark.sql("SELECT cust_name,city,grade FROM customer ORDER BY (CASE WHEN grade IS NOT NULL THEN grade ELSE city END)").show()


# NULL Functions IFNULL()....
# spark.sql("SELECT customer_id, salesman_id * purch_amt FROM orders").show()
# spark.sql("SELECT customer_id, salesman_id * (purch_amt + ifnull(ord_no, 0)) FROM orders").show()
# spark.sql("SELECT cust_name, customer_id * (salesman_id + ifnull(grade,0)) AS total FROM customer").show() # IFNULL(grade, 0) = 0, if null to 0 put karase teni jagae.


# Stored Procedures....
# A stored procedure is a prepared SQL code that you can save, so the code can be reused over and over again.
# So if you have an SQL query that you write over and over again, save it as a stored procedure, and then just call it to execute it. You can also pass parameters to a stored procedure, so that the stored procedure can act based on the parameter value(s) that is passed.

# In PySpark SQL, you cannot create stored procedures like you would in traditional relational databases.PySpark SQL operates on DataFrames and does not have support for stored procedures in the same way as SQL Server or MySQL, which operate on tables.
# spark.sql("CREATE PROCEDURE SelectAllCustomers AS SELECT * FROM customer GO").show() # get error, not supported

# Another way we can use function..
# df1 is pforders's dataframe
# def filter_amt(df1, amount):
#     return df1.filter(df1.purch_amt > amount)
# filterdf = filter_amt(df1, 1000.00)
# filterdf.show()

# Operators....
# spark.sql("SELECT 17 % 5 AS modulo").show()
# spark.sql("SELECT 30 - 20 AS sub").show()


# COMMAND ----------

# DBTITLE 1,SQL DATABASE Commands..
#DB Create....
#db created in dbfs/user/spark-warehouse folder, here that folder is meta data of pyspark.(here also show in database table)
# in databricks database is only automatically remove from database table not user when cluster is terminated.
# spark.sql("CREATE DATABASE sss").show()


#DROP db....
# spark.sql("DROP DATABASE sss")


#DB backup....
#It seems like you're trying to execute a SQL command that is not supported in Apache Spark SQL. 
#Spark SQL does not support a BACKUP DATABASE command like SQL Server 
# spark.sql("BACKUP DATABASE sss TO DISK = '/home/uttam/Desktop/sss.bak'") #get error, not supported


#CREATE table....
# spark.sql("CREATE TABLE sss.persons (PersonID INT, LastName VARCHAR(255), FirstName VARCHAR(255), Address VARCHAR(255), City VARCHAR(255))") # table created in sss db 
# spark.sql("DESCRIBE EXTENDED sss.persons").show()
# spark.sql("CREATE TABLE sss.cust_info AS SELECT salesman_id,city FROM customer WHERE salesman_id > 5005")#customer is tempview
# spark.sql("SELECT * FROM sss.cust_info").show()


#DROP TABLE....
# spark.sql("DROP TABLE sss.cust_info").show()


#ALTER TABLE....
#The ALTER TABLE statement is used to add, delete, or modify columns in an existing table.
#The ALTER TABLE statement is also used to add and drop various constraints on an existing table.
# spark.sql("ALTER TABLE sss.persons ADD COLUMNS (Email VARCHAR(255))")
# spark.sql("DESCRIBE EXTENDED sss.persons").show()

#It seems like the DROP COLUMN operation is not supported directly through Spark 
#It seems that the MODIFY COLUMN syntax is not supported in Spark SQL
#RENAME COLUMN operation is not supported directly through Spark SQL's ALTER TABLE
# spark.sql("ALTER TABLE sss.persons DROP COLUMN Email").show() # get error
# spark.sql("ALTER TABLE sss.persons MODIFY COLUMN Email INTEGER").show() # get error
# spark.sql("ALTER TABLE sss.persons RENAME COLUMN Email to email").show() # get error
# spark.sql("ALTER TABLE persons MODIFY COLUMN Email int NOT NULL").show() # get error


# PRIMARY KEY,FOREIGN KEY (primary key,foreign key not supported in spark-sql)....
# spark.sql("CREATE TABLE sss.Personsss (PersonID int NOT NULL, LastName varchar(255) NOT NULL, FirstName varchar(255), Address VARCHAR(255), City varchar(255), PRIMARY KEY (PersonID)") #get error not supported
# spark.sql("CREATE TABLE sss.Orders (OrderID int NOT NULL, OrderNumber int NOT NULL, PersonID int, PRIMARY KEY (OrderID), FOREIGN KEY (PersonID) REFERENCES Persons(PersonID))") #get error not supported


# CHECK....
# CHECK is not supported directy in pyspark
# The following SQL creates a CHECK constraint on the "Age" column when the "Persons" table is created. The CHECK constraint ensures that the age of a person must be 18, or older:
# spark.sql("CREATE TABLE P2 (ID int NOT NULL, LastName varchar(255) NOT NULL, FirstName varchar(255), Age int, CHECK (Age>=18))") #get error


# DEFAULT(not supported in spark-sql)....
# The DEFAULT constraint is used to set a default value for a column.
# The default value will be added to all new records, if no other value is specified.
# spark.sql("CREATE TABLE sss.P2 (ID int NOT NULL, LastName  VARCHAR(255) NOT NULL, FirstName VARCHAR(255), Age int, City VARCHAR(255) DEFAULT 'Sandes')") #get error not supported


#INDEX....
# It seems that the CREATE INDEX statement is not supported directly in Spark SQL 
# The CREATE INDEX statement is used to create indexes in tables.
# Indexes are used to retrieve data from the database more quickly than otherwise. The users cannot see the indexes, they are just used to speed up searches/queries.
# CREATE UNIQUE INDEX index_name ON table_name (column1, column2, ...);
# CREATE INDEX index_name ON table_name (column1, column2, ...);
# ALTER TABLE table_name DROP INDEX index_name;
# spark.sql("CREATE INDEX indx ON sss.cust_info (salesman_id)") #get error because not supported in pyspark

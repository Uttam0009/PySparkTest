# Databricks notebook source
# DBTITLE 1,createDataFrame
# dir(spark)

from pyspark.sql.types import StructField,StructType, IntegerType, StringType
data = [(1, 'A'),(2, 'B'),(3, 'C')]
schema = StructType([
    StructField(name='id', dataType=IntegerType(), nullable=False),
    StructField(name='name', dataType=StringType(), nullable=False)
])
df = spark.createDataFrame(data=data, schema=schema)
# df = spark.createDataFrame(data = data, schema = ('Index', 'Wing'))
df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,createDataFrame with schema
from pyspark.sql.types import StructType, StructField, StringType
data = [(1,('Sukala','Satyam'),200),(2,('Mheta','Man'),300)]
sub_schema = StructType([
                        StructField(name='primary', dataType=StringType(), nullable=False),
                        StructField(name='secondary', dataType=StringType(), nullable=False)
                        ])
schema = StructType([
                    StructField(name='Id', dataType=IntegerType(), nullable=False),
                    StructField(name='Name', dataType=sub_schema, nullable=False),
                    StructField(name='Salary', dataType=IntegerType(), nullable=False)

])
df = spark.createDataFrame(data=data, schema=schema)
print(df)
df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,DBFS commands
# dbutils.fs.help()
# dbutils.fs.help("cp")

# remove any files or directory from DBFS 
# dbutils.fs.rm("/FileStore/query.txt")
# dbutils.fs.rm("/FileStore/tips3.csv")
# dbutils.fs.rm("/FileStore/tables/tips3.csv")
# dbutils.fs.rm("/FileStore/data")
# %fs rm -r dbfs:/FileStore/jars                # remove jars folder with jars folder's all file

# create directory
# dbutils.fs.mkdirs('/FileStore/data')

# lists the content of a directory
# dbutils.fs.ls('/FileStore/data')

# move file or directory, possible across the file systyems
# dbutils.fs.mv('/FileStore/data/tips3.csv','/FileStore')

# rename file possible across the file systyems
# dbutils.fs.mv('/FileStore/data/annual_enterprise_survey_2021_financial_year_provisional_csv.csv','/FileStore/data/annual_enterprise_survey_2021.csv')
# dbutils.fs.mv('/FileStore/data/annual_enterprise_survey_2021.csv','/FileStore/data/fcsv/annual_survey_2021.csv')

# copies a file or directory, possible across file systems
# dbutils.fs.cp('/FileStore/data/tips3.csv','/FileStore')


# COMMAND ----------

# DBTITLE 1,Read csv
# df = spark.read.format('csv').option(key='header',value=True).load(path='dbfs:/FileStore/data/tips3.csv')  # or
# df = spark.read.csv(path='dbfs:/FileStore/data/tips3.csv', header=True)
# df.printSchema()
# print('total number of rows:',df.count())
# df.show(5)
# display(df)

# different columns and data of both csv
# df2 = spark.read.csv(path=['dbfs:/FileStore/data/fcsv/annual_survey_2021.csv','dbfs:/FileStore/data/tips3.csv'],header=True) 
# df2.printSchema()
# print("rows:", df2.count())
# df2.show(41841, truncate=False)
# df2.tail(226)
# display(df2)

# same columns and data
# Read Multiple CSVs
# Read entier folder of csv and diffrent location of csv 
# df3 = spark.read.csv(path=['dbfs:/FileStore/data/scsv/tips.csv','dbfs:/FileStore/data/scsv/tips2.csv','dbfs:/FileStore/data/tips3.csv'],header=True) 
# df3 = spark.read.csv(path=['dbfs:/FileStore/data/scsv','dbfs:/FileStore/data/tips3.csv'],header=True) 
# print("rows:", df3.count())
# df3.show(244)

# Read entier folder of csv and diffrent location of csv with StructType
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
schema = StructType([
                    StructField(name='Total_bill', dataType=FloatType()),
                    StructField(name='Tip', dataType=FloatType()),
                    StructField(name='Sex', dataType=StringType()),
                    StructField(name='Smoker', dataType=StringType()),
                    StructField(name='Day', dataType=StringType()),
                    StructField(name='Time', dataType=StringType()),
                    StructField(name='Size', dataType=IntegerType())
                    ])
df4 = spark.read.csv(path=['dbfs:/FileStore/data/scsv','dbfs:/FileStore/data/tips3.csv'],schema=schema,header=True)
df4.printSchema()
print("rows:", df4.count())
df4.show(5)


# COMMAND ----------

# DBTITLE 1,Write csv
from pyspark.sql.types import StringType, IntegerType, StructField,StructType
# dbutils.fs.mkdirs('/FileStore/data/wcsv')
# dbutils.fs.mv('/FileStore/data/write','/FileStore/data/wcsv/w1', recurse=True)

data = [('Tej',5000),('Gaj',1000),('Bhoj',7000),('Moj',9000)]
schema = StructType([
    StructField(name='Name',dataType=StringType(),nullable=False),
    StructField(name='Salary',dataType=IntegerType(),nullable=False)
    ])
df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show()

# df.write.csv(path='dbfs:/FileStore/data/wcsv/w1', header=True, mode='error')
# df.write.csv(path='dbfs:/FileStore/data/wcsv/w1', header=True, mode='ignore')
# df.write.csv(path='dbfs:/FileStore/data/wcsv/w1', header=True, mode='append')
# df.write.csv(path='dbfs:/FileStore/data/wcsv/w1', header=True, mode='overwrite')

c = spark.read.csv(path='dbfs:/FileStore/data/wcsv/w1',header=True)
c.show()
display(c)


# COMMAND ----------

# DBTITLE 1,Read Write json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# dbutils.fs.mkdirs('/FileStore/data/wjson')
schema = StructType([
    StructField(name='gender',dataType=StringType()),
    StructField(name='id',dataType=IntegerType()),
    StructField(name='name',dataType=StringType()),
    StructField(name='salary',dataType=IntegerType()),
    StructField(name='kk',dataType=IntegerType())
])
# df = spark.read.json(path='dbfs:/FileStore/data/fjson/practice.json')
# df = spark.read.json(path='dbfs:/FileStore/data/fjson/practice2.json',multiLine=True,schema=schema)
# df = spark.read.json(path='dbfs:/FileStore/data/fjson',multiLine=True,schema=schema)
# df.printSchema()
# df.show()

# write json
data =[(1,'Ram'),(2,'Shyam')]
df2 = spark.createDataFrame(data=data, schema=['id','name'])
df2.write.json(path='dbfs:/FileStore/data/wjson/wj1', mode='ignore') # mode='ignore','append','overwrite,'error'
df3 = spark.read.json(path='dbfs:/FileStore/data/wjson/wj1')
df3.show()

# COMMAND ----------

# DBTITLE 1,Read Write parquet
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType
# dbutils.fs.mkdirs('/FileStore/data/wparquet')

# read parquet
# df = spark.read.parquet('dbfs:/FileStore/data/fparquet/userdata2.parquet')
# df = spark.read.parquet('dbfs:/FileStore/data/fparquet') # all parquet in folder
# df.printSchema()
# print('number of rows:',df.count())
# df.show(5)
# display(df)

# write parquet
data = [(1,'Meet'),(2,'Geet')]
schema = StructType([
    StructField(name='id',dataType=IntegerType()),
    StructField(name='name',dataType=StringType()),
])
schema2 = StructType([
    StructField(name='id',dataType=StringType()),
    StructField(name='name',dataType=StringType()),
])
df2 = spark.createDataFrame(data=data, schema= schema)
# df2.printSchema()
# df2.show()
df2.write.parquet('dbfs:/FileStore/data/wparquet/wp1',mode='ignore') # mode='ignore','append','overwrite,'error'
df3 = spark.read.parquet('dbfs:/FileStore/data/wparquet/wp1',schema=schema2) 
# df3.printSchema()
#Here we can given data_type(schema) when creating parquet file, once created we can not able to changed(schema) in parquet


# COMMAND ----------

# DBTITLE 1,partitionBy
# partitionBy it's used partion large dataset into smalller files based on one or multilpe columns
# dbutils.fs.mkdirs('/FileStore/data/wpartition')
data = [(1,'bus','big','TATA'),(2,'car','small','toyota'),(3,'truck','big','TATA'),(4,'bike','verysmall','hero')]
df = spark.createDataFrame(data=data, schema=['id','vehical','size','company'])
# df.show()
# df.printSchema()
# df.write.parquet('dbfs:/FileStore/data/wpartition/wpar1',mode='overwrite',partitionBy='company')
# spark.read.parquet('dbfs:/FileStore/data/wpartition/wpar1').show()
# spark.read.parquet('dbfs:/FileStore/data/wpartition/wpar1/company=hero').show()

# df.write.parquet('dbfs:/FileStore/data/wpartition/wpar2',mode='overwrite',partitionBy=['company','size']) # multilpe columns
# spark.read.parquet('dbfs:/FileStore/data/wpartition/wpar2').show()
spark.read.parquet('dbfs:/FileStore/data/wpartition/wpar2/company=TATA').show()



# COMMAND ----------

# DBTITLE 1,show, withColumn,withColumnRenamed,ArrayType column
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import col,lit,array
data = [(1,'hgdferwtalkicomdfvnfykasm'),(2,'kjdgsfwrscbvnmgj37ryfjhmbmmmmommmmmmmmn'),(3,'kjdgsfwrllllmmmommmmmmmmn')]
schema= StructType([
    StructField(name='id',dataType=IntegerType()),
    StructField(name='comment',dataType=StringType())
])
df = spark.createDataFrame(data=data, schema=schema)
# df.printSchema()
# df.show(3)
# df.show(truncate=False)
# df.show(truncate=8)
# df.show(n=2,truncate=4)
# df.show(truncate=False,vertical=True)
# df.head(2)
# df.tail(1)


# withColumn = typecast,add,update ect of column
data =[(5,'Panchdev',5000),(7,'Saptarshi',7000)]
df = spark.createDataFrame(data=data, schema=['id','name','salary'])
# df.printSchema()
# df.show()
df2 = df.withColumn(colName='salary',col=col('salary').cast('Integer'))
df3 = df2.withColumn('salary',col('salary')*2)
df4 = df3.withColumn('cpsalary',col('salary')*5)
df5 = df4.withColumn('cpsalary',col('cpsalary').cast('Float'))
df6 = df5.withColumn('country',lit('India'))  # lit() = value of country column all rows
# df6.show()
# df6.printSchema()
# help(col)
# help(lit)


#Rename column (DataFrame Are immutable)
data = [(2, "Alice"), (5, "Bob")]
df = spark.createDataFrame(data=data,schema=['id','name'])
# df.show()
df2 = df.withColumnRenamed('id','Id')
# df2.show()


# ArrayType Column
data = [('bhal',[1,2],[3,4]),('tal',[6,4],[5,6])]
schema = StructType([StructField('name',StringType()),
                    StructField('nums',ArrayType(IntegerType())),
                    StructField('nums2',ArrayType(IntegerType()))])
df = spark.createDataFrame(data=data,schema=schema)
df2 = df.withColumn('new1',col('nums')[0].cast(IntegerType()))
df3 = df2.withColumn('new2',array(col('nums'),col('nums2')))
df3.printSchema()
df3.show()


# COMMAND ----------

# DBTITLE 1,explode,split,array,Array_contains
from pyspark.sql.functions import col,explode,split,array,array_contains
#explode
data = [(1,'neel',['python','sql']),(2,'jeel',['spark','sql'])]
df = spark.createDataFrame(data=data, schema=['id','name','skills'])
# df.withColumn('skill',explode(col('skills'))).show()
# df.show()

#split
data = [(1,'neel','python,sql'),(2,'jeel','spark,sql')]
df = spark.createDataFrame(data=data, schema=['id','name','skills'])
# df.withColumn('skillArray',split(col('skills'),',')).show()
# df.show()

#array
data = [(1,'neel','python','sql'),(2,'jeel','spark','sql')]
df = spark.createDataFrame(data=data,schema=['id','name','primary','secondary'])
# df.withColumn('all_skiils',array(col('primary'),col('secondary'))).show()
# df.show()

#Array_contains
data = [(1,'neel',['python','sql']),(2,'jeel',['spark','sql'])]
df = spark.createDataFrame(data=data,schema=['id','name','skills'])
df.withColumn('Hascolumn?',array_contains(col('skills'),'python')).show()
df.show()



# COMMAND ----------

# DBTITLE 1,MapType column,map_keys,map_values,explode in map,Row, Row using class,nested Row
from pyspark.sql.types import *
from pyspark.sql.functions import col,map_keys,map_values
# MapType column 
data = [('Lalo',{'hair':'black','eye':'brown','salary':10000}),('Kalo',{'hair':'brown','eye':'brown','salary':20000})]
schema = StructType([
    StructField('name',StringType()),
    StructField('properties',MapType(StringType(),StringType()))])
df = spark.createDataFrame(data,schema)
# df.withColumn('hair', col('properties')['hair']).show()
# df.show(truncate=False)
# df.printSchema()

# map_keys
# df.withColumn('keys',map_keys(col('properties'))).show(truncate=False)

# map_values    
# df.withColumn('values',map_values(col('properties'))).show(truncate=False)

# explode in map  
# df.select('name','properties',explode(col('properties'))).show(truncate=False) 

# Row
row = Row(name='ram', salary=10000)
row2 = Row(name='shyam', salary=20000)
# spark.createDataFrame(data=(row, row2)).show()
# print(str(row[1])+' '+row[0])

# Row using class
person = Row('name','age')
p1 = person("rohan",21)
p2 = person("raj",22)
# spark.createDataFrame([p1,p2]).show()

# nested Row
data2 = [Row(name='ram', prop=Row(salary=10000,gender='male')),\
      Row(name='shyam', prop=Row(salary=20000,gender='male'))]
spark.createDataFrame(data2).show()



# COMMAND ----------

# DBTITLE 1,column fun,when(),alias(),cast(),like(),asc(),desc(),filter(),where(),sort(),orderBy()
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
from pyspark.sql.functions import col,lit,when

# column function
data = [(1,'neel',('python','sql')),(2,'jeel',('spark','sql'))]
pskill = StructType([
    StructField('primary',StringType()),
    StructField('secondary',StringType())
])
schema = StructType([
    StructField('Id',IntegerType(),False),
    StructField('name',StringType()),
    StructField('skills',pskill)
])
df = spark.createDataFrame(data,schema)
# df.printSchema()
# df.show()
# df.withColumn('age',lit(20)).show()

# df.select(col('name')).show()
# df.select(col('skills')['primary']).show()
# df.select(df['name']).show()
# df.select(df['skills']['primary']).show()


# help(when)
# when() and otherwise()
data = [(1,'Male','neel',25),(2,'Female','jeel',22),(3,'','sonu',24)]
df = spark.createDataFrame(data=data,schema=['id','gender','name','age'])
df.show()
# df.select(col('id'),col('name'),when(col('gender')=='Male','M').when(col('gender')=='Female','F').otherwise('unknown').alias('gender')).show()


# alias(),cast(),like(),asc(),desc(),sort(),filter(),where(),orderBy()
# df2 = df.select(col('id').alias('emp_id'),df.name.alias('emp_name'),col('age').cast('int'))
# df2.printSchema()
# df2.show()
# df.sort(col('name').asc()).show()
# df.sort(col('age').desc()).show()
# df.orderBy(col('age').desc()).show()
# df.orderBy(col('name'),col('age')).show()                     #sort() and orderBy() both are same working
# df.filter(col('name').like('%l')).show()
# df.filter(col('id')==3).show()
# df.where((col('gender')=='Male')|(col('age')==22)).show()   #filter() and where() both are same working


# COMMAND ----------

# DBTITLE 1,distinct(),dropDuplicate(),select(),union(),unionAll(),unionByName(),groupBy(),gropBy agg()
from pyspark.sql.functions import col,count
# distinct() & dropDuplicate()
data = [(1,'Sonu','F',2000),(1,'Sonu','F',2000),(1,'Sonu','F',4000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender','salary'])
# df.show()
# df.distinct().show()
# df.dropDuplicates().show()
# df.dropDuplicates(['gender']).show()
# df.dropDuplicates(['gender','name']).show() # both are duplicate then remove it


# select() here select single,multiple,column by index from list and nested columns..
# df.select('*').show()
# df.select(df.name).show()
# df.select(col('name'),col('gender')).show()
# df.select([col for col in df.columns]).show()


# union() & unionAll() both are same work as merges 2 or more dataframe using same schema without removing dublicates..
data = [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500)]
data2 = [(5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500)]
df1 = spark.createDataFrame(data,schema=['id','name','gender','salary'])
df2 = spark.createDataFrame(data2,schema=['id','name','gender','salary'])
# df1.printSchema()
# df2.printSchema()
# df1.union(df2).show()
# df1.unionAll(df2).show()

#unionByName() here union (merge) two dataframe with diffrent schema by passing allowMissing column..
data= [(1,'sonu','F'),(2,'dev','M'),(3,'tej','M'),(4,'kej','M')]
data2= [(5,'son',5000),(2,'dev',1000),(3,'tej',2000),(4,'kej',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender'])
df2 = spark.createDataFrame(data2,schema=['id','name','salary'])
# df.union(df2).show()
# df.unionByName(df2,allowMissingColumns=True).show()


#groupBy() collect data like count,sum,avg,min,max...
data= [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500),(5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',4000),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender','salary'])
df.show()
# df.groupBy(col('salary')).count().show()                      #here salary are unique
# df.groupBy(col('name')).avg('salary').show()                  #here name are unique
# df.groupBy(col('gender'),col('name')).count().show()          #here salary+name are unique
# df.groupBy(col('gender'),col('name')).max('salary').show()    #here salary+name are unique
# df.groupBy(col('gender')).min('salary').show()                #here gender is unique

#gropBy agg() here calculate more than one(multiple) aggregate...
from pyspark.sql import functions as F
df.groupBy(df.name).agg(  
    F.count('*').alias('count_emp'),
    F.min('salary').alias('min_sal'),
    F.max('salary').alias('max_sal')
).show()
#name are unique

# help(df.groupBy('name').agg)


# COMMAND ----------

# DBTITLE 1,join(),pivot(),unpivote(),fill() ,fillna(),sample(),collect()
#join() is like sql join like  inner,outer,left,right,left anti,left semi,self...
data= [(1,'sonu','F'),(2,'dev','M'),(3,'tej','M'),(4,'kej','M')]
data2= [(1,'CE',5000),(7,'IT',1000),(3,'ME',2000),(4,'AI',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender'])
df2 = spark.createDataFrame(data2,schema=['id','name','salary'])
# df.show()
# df2.show()
# df.join(df2, df.id == df2.id, 'right').show()             #here right and rightouter join are same
# df.join(df2, df.id==df2.id, 'left').show()                #here left and leftouter join are same
# df.join(df2, df.id==df2.id, 'outer').show()               #all
# df.join(df2, df.id==df2.id, 'inner').show()               #common all
# df.join(df2, df.id==df2.id, 'leftsemi').show()            #coomon data (in only left)  (here leftsemi,semi join are same)
# df.join(df2, df.id==df2.id, 'leftanti').show()            #not common data (in only left) (here leftanti,anti join are same)
# df.join(df2, df.id==df2.id, 'right').select(df.id, df.name,df2.salary).show()

# self join..........
from pyspark.sql.functions import col
data3= [(1,'sonu',0),(2,'dev',1),(3,'tej',2),(4,'kej',3)]
df3 = spark.createDataFrame(data3,schema=['id','name','d_id'])
# df3.show()
# df3.alias('one').join(df3.alias('two'), col('one.id')==col('two.d_id'),'left').show()
# df3.alias('one').join(df3.alias('two'), col('one.id')==col('two.d_id'),'left').select(col('one.name').alias('one_name'),col('two.name').alias('two_name')).show()


# pivot(),....
data= [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500),
       (5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',4000),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender','salary'])
# df.show()
# df.groupBy('name','id').count().show() # both are unique
# df.groupBy('name').pivot('salary').count().show()
# df.groupBy('name').pivot('gender',['F','M']).count().show()
# df.groupBy('gender').pivot('salary',[2000]).count().show()

# unpivote() fun is not avilable in pyspark...
data2 = [('CE',8,4),('IT',4,3),('ME',1,2)]
df2 = spark.createDataFrame(data2,schema = ['sub','male','female'])
# df2.show()
from pyspark.sql.functions import expr
# here stack(numofcolummerge,valuecolumnname,columnname as convert in one row)
unpivoted_df = df2.selectExpr("sub", "stack(2, 'M', male, 'F', female) as (gender, count)")
# unpivoted_df.show()


# fill() & fillna() both are same working...
data= [(1,'sonu',None,None),(None,'dev','M',1000),(3,'tej','M',2000),(4,'kej',None,1500),
       (5,'son','F',5000),(2,None,'M',1000),(3,'tej',None,None),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema= ['id','name','gender','salary'])
# df.show()
# here string datatype column none value repalce only string value visa versa iteger etc..
# df.fillna(25).show()
# df.fillna('unknown').show()
# df.fillna('unknown',['gender','name']).show()
# or....
# df.na.fill('unknown',['name']).show()
# df.na.fill('unknown').show()
# df.na.fill('unknown',['name']).show()
# df.na.fill(0).show()


# sample() sampaling subset from large dataset
df = spark.range(0,101)
# df.show()
df2 = df.sample(fraction=0.1,seed=123)
df3 = df.sample(fraction=0.1, seed=123)
# df2.show()
# df3.show()


# collect() retrieves all elements in a Dataframe as an array of row type to the driver node..
data= [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500),
       (5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',4000),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender','salary'])
# df.printSchema()
df.show()
dataRows = df.collect()
# print(dataRows)
# print(dataRows[0])
# print(dataRows[1][0])


# COMMAND ----------

# DBTITLE 1,df.transform()

# DataFrame.transform() used to chain the custom transformations and fun returns new df after applying specific transformation..
from pyspark.sql.functions import upper
data =  [(1,'sonu',['f','mj'],2000),(2,'dev',['f','mj'],1000),(3,'tej',['f','mj'],2000),(4,'kej',['f','mj'],1500),
       (5,'son',['f','mj'],5000),(2,'dev',['f','mj'],1000),(3,'tej',['f','mj'],4000),(4,'kej',['f','mj'],1500)]
df = spark.createDataFrame(data, schema=['id','name','skills','salary'])
# df.show()

def convertUpper(df):
    return df.withColumn('name', upper(df.name))
def doubleSalary(df):
    return df.withColumn('doubleSal',df.salary*2)
# df.transform(convertUpper).show()
# df.transform(doubleSalary).show()
# df.transform(convertUpper).transform(doubleSalary).show()

# pyspark.sql.functions.transform ..here it's apply the transformation on a column of type array only
from pyspark.sql import functions as F
# df.select('id','name',F.transform('skills',lambda x: upper(x)).alias('skills')).show()
def ucase(x):
    return upper(x)
# df.select('id','name',F.transform('skills',ucase).alias('skills')).show()


# COMMAND ----------

# DBTITLE 1,createOrReplaceTempView(),createOrReplaceGlobalTempView()
# createOrReplaceTempView().. using this function and use SQL to select & manipulate data.
# Temp Views are session scoped and cannot be shared between the sessions
from pyspark.sql.functions import upper
data= [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500),
       (5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',4000),(4,'kej','M',1500)]
schema = ['id','name','gender','salary']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView('employee')
# spark.sql("SELECT * FROM employee").show()
# spark.sql("SELECT id, upper(name) FROM employee").show()

# createOrReplaceGlobalTempView() create table globally....
df.createOrReplaceGlobalTempView('gemployee')
# spark.sql("SELECT * FROM global_temp.gemployee").show()


df2 = spark.read.csv('dbfs:/FileStore/data/tips3.csv',header=True)
df2.createOrReplaceTempView('tip')
df2.createOrReplaceGlobalTempView('g_tip')
# spark.sql('SELECT * FROM tip').show()
# spark.sql('SELECT * FROM global_temp.g_tip').show()

spark.catalog.listTables()                                     # show  only local temp view table names
# global_temp_views = spark.catalog.listTables("global_temp")  # show all tables names
# print(global_temp_views)


# COMMAND ----------

# DBTITLE 1,Temp View with sql
# MAGIC %sql
# MAGIC -- SELECT * FROM tip
# MAGIC -- SELECT * FROM global_temp.g_tip
# MAGIC -- SELECT * FROM employee
# MAGIC -- SELECT * FROM global_temp.gemployee

# COMMAND ----------

# DBTITLE 1,udf()
# udf() (user define function)
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
data = [(1,'samir',10000,200),(2,'veer',20000,300),(3,'deep',30000,450)]
df = spark.createDataFrame(data,schema = ['id','name','salary','bonus'])
df.show()

def total(a,b):
    return a+b
total_payment = udf(lambda a,b : total(a,b), IntegerType())
# df.withColumn('payment',total_payment(df.salary,df.bonus)).show() 
# or
@udf(returnType=IntegerType())
def total(a,b):
    return a+b
# df.select('*',total(df.salary,df.bonus).alias('payment')).show()
 # or
# df.withColumn('payment', df.salary+df.bonus).show()

#UDF in sql query....
df.createOrReplaceTempView('udffun')
def total(s,b):
    return s+b
spark.udf.register(name='sumsql',f=total,returnType=IntegerType())
spark.sql("select id,name,sumsql(salary,bonus) as sum from udffun").show()


# COMMAND ----------

# DBTITLE 1,RDD
# RESILIENT DISTRIBUTED DATASET 
#RDD collection of object similar to list its immutable and in memory processing (unstructured data type)(Transformations & Action)
data = [(1,'dev',),(2,'raj')]
rdd = spark.sparkContext.parallelize(data)
# print(type(rdd))
# rdd.collect()

#rdd to DataFrame
# df = rdd.toDF(schema=['id','name']) #or
# df = spark.createDataFrame(rdd, schema=['id','name'])
# # print(type(df))
# df.show()


# map() its rdd transformation used to apply lambda on every element of rdd and return new rdd
data = [('ram','raj'),('radhe','shyam')]
rdd1 = spark.sparkContext.parallelize(data)
rdd2 = rdd1.map(lambda x: x + (x[0]+'-'+x[1],))
rdd2.collect()

# dataframe doesn't have map() tramsformation,you need to generate rdd first
df = spark.createDataFrame(data,['fn','ln'])
rdd3 = df.rdd.map(lambda x: x + (x[0]+'-'+x[1],))  #df to convert rdd ==> df.rdd
df2 = rdd3.toDF(schema=['fn','ln','an'])
# df.show()
# df2.show()

# or..
def fullName(x):
    return x + (x[0]+'-'+x[1],)
df = spark.createDataFrame(data,['fn','ln'])
rdd4 = df.rdd.map(lambda x : fullName(x))
df2 = rdd4.toDF(schema=['fn','ln','an'])
# df.show()
# df2.show()


# flatMap() is transformation operation that flattens the RDD after applying the function on every elements and return new PySpark RDD
data = [('ram-kaj'),('radhe-radhe')]
rdd1 = spark.sparkContext.parallelize(data)
rdd1.collect()
for item in rdd1.collect():
    print(item)

# rdd2 = rdd1.map(lambda x: x.split('-'))
# for item in rdd2.collect():
#     print(item)

# only flatMap() fun can flate element in RDD as a object
# rdd3 = rdd1.flatMap(lambda x: x.split('-'))
# for item in rdd3.collect():
#     print(item)


# COMMAND ----------

# DBTITLE 1,from_json(),to_json(),json_tuple(),get_json_object()
# from_json() it's used to convert json string in to MapType or StructType....
from pyspark.sql.functions import from_json,to_json,json_tuple,get_json_object
from pyspark.sql.types import MapType, StructType, StructField, StringType

data = [('ram','{"hair":"black","eye":"brown"}')] #'{"a":"b"}' this is json type
df =spark.createDataFrame(data,schema = ['id','props'])
# df.printSchema()
# df.show(truncate=False)

#1. MapType()
# here only changed schema in json data into MapType
MapTypeSchema = MapType(StringType(),StringType()) #key,value
df2 = df.withColumn('propMap',from_json(df.props,MapTypeSchema))
# df2.printSchema()
# df2.show(truncate=False)

df3 = df2.withColumn('hair',df2.propMap.hair).withColumn('eye',df2.propMap.eye)
# df3.printSchema()
# df3.show(truncate=False)

#2. StructType()
structSchema= StructType([
                        StructField('eye', StringType()),
                        StructField('hair', StringType())])
df2 = df.withColumn('propStruct', from_json(df.props,structSchema))
# df2.printSchema()
# df2.show()
df3 = df2.withColumn('hair', df2.propStruct.hair).withColumn('eyes', df2.propStruct.eye)
# df3.printSchema()
# df3.show()


#to_json() used to convert dataframe column MapType or Struct Type to json string....
#1. MapType to json string
data = [('ram',{"hair":"black","eye":"brown"})] #{"a":"b"} this is maptype
schema = ['id','props']
df =spark.createDataFrame(data,schema)
df2 = df.withColumn('propJsonString',to_json(df.props))
# df.printSchema()
# df.show(truncate=False)
# df2.printSchema()
# df2.show(truncate=False)

#2. Struct Type to json string
data = [('ram',('black','brown'))] #('black','brown') this is structtype
schema = StructType([StructField('name',StringType()),
                     StructField('props',StructType([StructField('hair',StringType()),StructField('eye',StringType())]))
                    ])
df =spark.createDataFrame(data,schema)
# df.printSchema()
# df.show(truncate=False)
df2 = df.withColumn('propJsonString',to_json(df.props))
# df2.printSchema()
# df2.show(truncate=False)


# json_tuple() this fun is used to query or extract element from json string column and create as new column....
data = [('ram','{"hair":"black","eye":"brown","skin":"shyam"}'),
        ('raj','{"hair":"white","eye":"black","skin":"brown"}')] #'{"a":"b"}' this is json type
schema = ['name','props']
df =spark.createDataFrame(data,schema)
# df.printSchema()
# df.show(truncate=False)
df2 = df.select(df.name, json_tuple(df.props,'hair','skin').alias('haircolour','eyecolour'))
# df.printSchema()
# df2.show()


#get_json_object() it's used to extract the json string based on path from the json column....
#here without any define schema we can access the any jason string
data = [('ram','{"body":{"hair":"black","eye":"brown","skin":"shyam"},"gender":"male"}'),
        ('raj','{"body":{"hair":"white","eye":"black","skin":"white"},"city":"Jaypur"}')] #'{"a":"b"}' this is json type
schema = ['name','props']
df =spark.createDataFrame(data,schema)
# df.printSchema()
# df.show(truncate=False)

# df.select('name',df.props.body.hair).show() # this get an error becuase here do not avilable struct schema
# df.select('name', get_json_object('props','$.body.hair').alias('hair')).show()
# df.select('name',get_json_object('props', '$.gender').alias('gender')).show()
df2 = df.select('name',get_json_object('props','$.body.skin').alias('skin'),get_json_object('props','$.city').alias('city'))
# df2.printSchema()
# df2.show()


# COMMAND ----------

# DBTITLE 1,current_date(),date_formate(),to_date(),current_timestamp(),to_timestamp(),hour(),minute(),second()
#Date functions -->here defult function is yyyy-MM-dd
#1. current_date() get the current system date return defult format date.(give a date in datetype)
#2. date_formate() to parse the date and convert from defult format to specific format.(give a date in string datatype)
#3. to_date() only convert date string into datetype

from pyspark.sql.functions import current_date,date_format,to_date,lit,current_timestamp,to_timestamp,hour,minute,second
df = spark.range(2)
# df.show()
df2 = df.withColumn('date',current_date())
# df2.show()
# df2.withColumn('stringFormate',date_format(lit('2002-02-07'),'MM.dd.yyyy')).show()
# df2.withColumn('stringFormate',date_format(current_date(),'dd/MM/yyyy')).show()
df3 = df2.withColumn('strinFormarte',date_format(df2.date,'dd/MM/yyyy'))
# df3.printSchema()
# df3.show()

# df2.withColumn('to_date',to_date(lit('2002-02-07'))).show()   # here put only date, dateformat is set accordingly avilable of dateformat in date
# df2.withColumn('to_date',to_date(current_date())).show()
df3 =df2.withColumn('to_date',to_date(df2.date))
# df3.printSchema()
# df3.show()


#Timestamp 
#1. current_timestamp() get defult yyyy-MM-dd HH:mm:ss.SS
#2. to_timestamp() convert timestamp string into TomestampType
#3. hour(),minute(),second() functions

df = spark.range(2)
df2 = df.withColumn('timestamp',current_timestamp())
df2.printSchema()
df2.show(truncate=False)
# df2.withColumn('to_timestamp',to_timestamp(df2.timestamp)).show(truncate=False)
# df2.withColumn('to_timestamp',to_timestamp(lit('2024-01-31 16:42:32.118405'))).show(truncate=False)  #it's defult no need to add format
df3 =df2.withColumn('to_timestamp',to_timestamp(lit('12.25.2022 08.10.03'),'MM.dd.yyyy HH.mm.ss')) #here not defult so add format
# df3.printSchema()
# df3.show(truncate=False)

df4 = df2.select('id',hour(df2.timestamp).alias('hours'),
                 minute(df2.timestamp).alias('minute'),
                 second(df2.timestamp).alias('second'))
# or...
df4 = df2.select('id',hour(current_timestamp()).alias('hours'),
                 minute(current_timestamp()).alias('minute'),
                 second(current_timestamp()).alias('second'))
# df4.printSchema()
# df4.show()


# COMMAND ----------

# DBTITLE 1,Aggregate functuins,Ranking functions
#Aggregate functuins
#1. approx_count_distinct() - return the count of distinct item in group of rows 
#2. avg() - return average of values in group of rows
#3. collect_list() - return all values from input column as list with duplicates
#4. collect_set() -  return all values from input column as list without duplicates
#5. countDistinct() - returns number of distinct elements in input column
#6. count() - returns number of elements in column

from pyspark.sql.functions import approx_count_distinct,avg,collect_list,collect_set,countDistinct,count
data= [(1,'sonu','F',2000),(2,'dev','M',1000),(3,'tej','M',2000),(4,'kej','M',1500),
       (5,'son','F',5000),(2,'dev','M',1000),(3,'tej','M',4000),(4,'kej','M',1500)]
df = spark.createDataFrame(data,schema=['id','name','gender','salary'])
# df.show()
# df.select(approx_count_distinct('salary').alias('distinct_sal')).show()
# df.select(avg('salary').alias('avgsal')).show()
# df.select(collect_list('salary').alias('collect_list')).show(truncate=False)
# df.select(collect_set('salary').alias('collect_set')).show(truncate=False)
# df.select(countDistinct('salary').alias('countDistinct')).show(truncate=False)
# df.select(count('salary').alias('count')).show()


#Ranking functions -->we need to partition the data using Window.partitionBy(), we need row num and rank function
#1. row_number() - window function is used to give the sequential start to 1 to the result of each window partition
#2. rank() - window function is used to provide a rank to the result within a partition.this fun leave gaps in rank when their are ties
#3. dense_rank() - window fun is used to get result with rows within partition without any gaps. sililar to rank() difference being rank fun leave gaps in rank when there are ties

from pyspark.sql.functions import row_number,rank,dense_rank
from pyspark.sql.window import Window
data= [(1,'Raj','CE',5000),(2,'Kunj','IT',1000),(3,'Manali','HR',2000),
       (4,'Dax','IT',1500),(5,'Saxi','HR',5000),(6,'Vraj','CE',9000),(7,'Aneri','HR',2000)]
df = spark.createDataFrame(data, schema= ['Id','Name','Department','Salary'])
df.show()
# df.sort('Department').show()

window = Window.partitionBy('Department').orderBy('Salary')
df.withColumn('Rownum', row_number().over(window)).withColumn('Rank',rank().over(window)).\
withColumn('Dense_rank', dense_rank().over(window)).show()


# COMMAND ----------

# DBTITLE 1,Real Time Scenarios
#Real Time Scenarios
#1. Remove double quotes from value of jason string using PySpark
from pyspark.sql.functions import *
# jsonString = """{"id":"1","Name":"Ram"Raj","City":"Surat"}"""
#or..
jsonString = '{"id":"1","Name":"Ram"Raj","City":"Surat"}'
data = [(1,jsonString)]
schema = ['id','json']
df = spark.createDataFrame(data,schema)
# df.show(truncate=False)

df = df.withColumn('col1',split(df.json,'"Name":')[0]).\
withColumn('col2',lit('"Name":')).\
withColumn('col3',lit('"RamRaj"')).\
withColumn('col4',split(df.json,'"Ram"Raj"')[1])
# df.show(truncate=False)

df = df.withColumn('col8',concat(df.col1,df.col2,df.col3,df.col4))
# df.select(df.json,df.col8).show(truncate=False)


#Real Time Scenarios
#2. Checking If a Column Exists in DataFrame
data = [(1,'Ram',40000),(2,'Kisan',90000)]
df = spark.createDataFrame(data,schema= ['id','name','salary'])
# df.printSchema()
# print(df.schema.fieldNames())
field = df.schema.fieldNames() 
num = field.count('salary')
if num > 0:
    print('salary column is present')
else:
    print('column is not found')


#Real Time Scenarios
#3. convert spark DataFrame into pandas dataframe
data = [(1,'Ram',40000),(2,'Kisan',90000)]
schema = ['id','name','salary']
df = spark.createDataFrame(data,schema)
# df.show()
df2 = df.toPandas()
# print(df2)


#Real Time Scenarios
#4. printSchema() to string or json
data = [(1,'Ram',40000),(2,'Kisan',90000)]
df = spark.createDataFrame(data,schema= ['id','name','salary'])

# a = df.schema.simpleString()
# print(a)
# print(type(a))	
# b = df.schema.json()
# print(b)
# print(type(b))
# c = df.schema.jsonValue()
# print(c)
# print(type(c))


#Real Time Scenarios
#5. Write DataFrame as single file with specific name in PySpark 
data = [(1,'Ram',40000),(2,'Kisan',90000),(3,'Ram',40000),(4,'Kisan',90000),
        (5,'Ram',40000),(6,'Kisan',90000),(7,'Ram',40000),(8,'Kisan',90000)]
schema = ['id','name','salary']
df = spark.createDataFrame(data,schema)
# df.printSchema()
# df.show()

## here write file every time then every time automatic change file name
# dbutils.fs.mkdirs('/FileStore/data/lastcsv')
# df.write.csv("/FileStore/data/lastcsv/part_csv",header=True, mode='overwrite')
# spark.read.csv("/FileStore/data/lastcsv/part_csv",header=True).show()
# df.coalesce(1).write.csv("/FileStore/data/lastcsv/single",header=True,mode='overwrite')
# spark.read.csv("/FileStore/data/lastcsv/single",header=True).show()

import os
folder_path = "dbfs:/FileStore/data/lastcsv/single"
files = dbutils.fs.ls(folder_path)
# Print out the list of files
# print([file.name for file in files])

# file_to_rename = 'part-00000-tid-2957102829446192735-84dc45f3-3cd1-424c-bf95-321bb7dba3c8-228-1-c000.csv'
# if any(file.name == file_to_rename for file in files):
#     old_path = os.path.join(folder_path, file_to_rename)
#     new_path = os.path.join(folder_path, 'one.csv')
#     # Use dbutils.fs.mv to rename the file in DBFS
#     dbutils.fs.mv(old_path,new_path)
#     print(f"File '{file_to_rename}' renamed to 'one.csv'")
# else:
#     print(f"File '{file_to_rename}' does not exist in the directory.")


# COMMAND ----------

# DBTITLE 1,help()
# parquetfiles, csv files etc are immutable(not changable) in pyspark.The direct modification of the source files (without writing) is not supported in PySpark's operational model. first convert in pyspark dataframe accordingly perform transformations and then write it.

# help(spark.read.parquet)
# help(StructType)
# help(df.withColumn)
# help(df.withColumnRenamed)
# help(array_contains)
# help(MapType)
# help(map_keys)
# help(Row)
# help(when)
# help(df.filter)
# help(df.dropDuplicates)
# help(df.sort)
# help(df.groupBy(df.salary).agg)
# help(df.join)
# help(df.fillna)
# help(df.createGlobalTempView)
# help(df.write.parquet)
# help(df.schema.fieldNames)
# help(df.toPandas)
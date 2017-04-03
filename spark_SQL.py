from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import Row

# sc is an existing SparkContext.
APP_NAME='sql test'
conf = SparkConf().setAppName(APP_NAME)
conf = conf.setMaster("local[*]")
sc  = SparkContext(conf=conf)
sqlContext = SQLContext(sc)
print('########################################started#############################')
# Load a text file and convert each line to a tuple.
lines = sc.textFile("/user/cloudera/spark/emp/employee.txt")
parts = lines.map(lambda l: l.split('\t'))
employee = parts.map(lambda p: Row(empno=int(p[0]), ename=p[1],job=p[2],mgr=p[3],hirdate=p[4],sal=p[5],comm=p[6],deptno=p[7]))


# The schema is encoded in a string.
print('Register of the schema')
schemaemp=sqlContext.createDataFrame(employee)
schemaemp.registerTempTable("emp")
print('Emp table has been created')
# SQL can be run over DataFrames that have been registered as a table.
print '####################################################################'
results = sqlContext.sql("SELECT * FROM emp where mgr>7839")
print(type(results))
print(results.show())

# The results of SQL queries are RDDs and support all the normal RDD operations.
#names = results.map(lambda p: "Name: " + p.name)
##for name in names.collect():
 # print(name)

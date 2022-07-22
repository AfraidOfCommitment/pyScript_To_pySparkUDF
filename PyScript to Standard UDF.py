# Databricks notebook source
#just a standard py dict in text form -> that is the content of our sample data files
sample_data = """{'dataset1': {'A':1,'B':1,'C':1},
              'dataset2': {'X':10,'Y':13},
              'dataset3': {'id':32, 'Name':'Peter'}
              }"""
print(eval(sample_data))

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Users/nikola.dyundev@databricks.com/complexUDF_data/

# COMMAND ----------

# MAGIC %fs
# MAGIC head dbfs:/Users/nikola.dyundev@databricks.com/complexUDF_data/data1.txt

# COMMAND ----------

#read all text files into a DataFrame(DF). We get a DF with one column and each file becomes a string entry in the dataframe -> thanks to wholetext=True
df = spark.read.load("dbfs:/Users/nikola.dyundev@databricks.com/complexUDF_data/", format="text",wholetext=True)
df.show()

# COMMAND ----------

df.limit(1).show()

# COMMAND ----------

df.first()

# COMMAND ----------

"""
Here is the place to put the actual python Script. In this case it is a simple function that evaluates the py dictionary and gets the datasets out of it. The function returns a dictionary that itself contains 3 dictionaries, which we regard as datasets in this example.
"""

def txtToDict(txt):
  #simply do an eval to parse teh py dict obj from string.
  dict_obj = eval(txt)
  return dict_obj


result = txtToDict("{'dataset1': {'A':1,'B':2,'C':3},'dataset2': {'X':10,'Y':10},'dataset3': {'id':111, 'Name':'Peter'}}")

print(result)

# COMMAND ----------

"""
This is a wrapper around the funciton that we created above, which packs it into a UDF.

Here we crate a complex nested type to be returned as a result of the UDF. We define three structypes (one for each Dataset) and then put all three together in a single big 'mothership' struct, which we return.
"""
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, StringType, MapType


dataset1_struct = StructType([
    StructField('A', IntegerType(), nullable=True),
    StructField('B', IntegerType(), nullable=True),
    StructField('C', IntegerType(), nullable=True)
    ])

dataset2_struct = StructType([
    StructField('X', IntegerType(), nullable=True),
    StructField('Y', IntegerType(), nullable=True)
    ])

dataset3_struct = StructType([
    StructField('id', IntegerType(), nullable=False),
    StructField('Name', StringType(), nullable=False)
    ])

return_struct = StructType([StructField('dataset1',dataset1_struct),
                            StructField('dataset2',dataset2_struct),
                            StructField('dataset3',dataset3_struct)])

@udf(returnType=return_struct)
def udf_toStruct(raw_text):
  #apply this function to every entry of the series, that have been given as an input
  return txtToDict(raw_text)

# COMMAND ----------

#use the UDF to transform the raw text and put the result from the UDF in anohter column
df=df.withColumn("UDF_Output",udf_toStruct(df["value"]))
df.show()

# COMMAND ----------

#bring out the first nested dataset/struct from the UDF output column
df = df.withColumn("dataset1",df["UDF_Output"]["dataset1"])
df.show()

# COMMAND ----------

#bring out the rest of the elements 
df = df.withColumn("dataset2",df["UDF_Output"]["dataset2"]).withColumn("dataset3",df["UDF_Output"]["dataset3"])
df.show()

# COMMAND ----------

#now let's cleanup and have the 3 dasets as 3 different columns
df = df["dataset1","dataset2","dataset3"]
df.show(truncate=False)

# COMMAND ----------

#isolate dataset1 into a different DataFrame
dataset1_df=df.select("dataset1.*")
dataset1_df.show()

# COMMAND ----------

#isolate dataset2 into a different DataFrame
dataset2_df=df.select("dataset2.*")
dataset2_df.show()

# COMMAND ----------

#isolate dataset3 into a different DataFrame
dataset3_df=df.select("dataset3.*")
dataset3_df.show()

# COMMAND ----------

#write a dataframe using the delta format
dataset1_df.write.mode("append").format("delta").save("/Users/nikola.dyundev@databricks.com/complexUDF_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Users/nikola.dyundev@databricks.com/complexUDF_delta`

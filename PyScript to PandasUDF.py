# Databricks notebook source
#pandas UDF link: https://databricks.com/de/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html
#https://databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html

# COMMAND ----------

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
Here is the place to put the actual python Script. In this case it is a simple function that evaluates the py dictionary and gets the datasets out of it. The function returns the three datasets as 3 JSON strings.
"""

import pandas as pd

def txtToJSON(txt):
  #simply do an eval to parse teh py dict obj from string.
  dict_obj = eval(txt)
  
  #extract the data from the file for dataset1
  pdf_dataset1 = pd.DataFrame(
      {
          "A": dict_obj['dataset1']['A'],
          "B": dict_obj['dataset1']['B'],
          "C": dict_obj['dataset1']['C']
      }, index=[0])

  #extract the data from the file for dataset1
  pdf_dataset2 = pd.DataFrame(
    {
        "X": dict_obj['dataset2']['X'],
        "Y": dict_obj['dataset2']['Y']
    }, index=[0])

  #extract the data from the file for dataset1
  pdf_dataset3 = pd.DataFrame(
    {
        "id": dict_obj['dataset3']['id'],
        "Name": dict_obj['dataset3']['Name']
    }, index=[0])
  
  pdf_dataset1=pdf_dataset1.reset_index().to_json(orient='records')
  pdf_dataset2=pdf_dataset2.reset_index().to_json(orient='records')
  pdf_dataset3=pdf_dataset3.reset_index().to_json(orient='records')

  return [pdf_dataset1,pdf_dataset2,pdf_dataset3]


result = txtToJSON("{'dataset1': {'A':1,'B':2,'C':3},'dataset2': {'X':10,'Y':10},'dataset3': {'id':111, 'Name':'Peter'}}")

print(result[1])

# COMMAND ----------

"""
This is a wrapper around the funciton that we created above, which packs it into a pandas UDF.
It is important to understand that a Pandas UDF always acts on series of data entries, not a single one (as is the case with regular UDFs)

It is worth setting the number of entries the pandas UDF is getting as input. This is done using -> spark.conf.set("spark.sql.execution.arrow.maxRecordsPerBatch", X) -> X is the number of entries
The bigger the input files the fewer entries should we process together using the same UDF
"""
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, struct

@pandas_udf("array<string>")
def pandas_udf_toJSON(raw_text: pd.Series) -> pd.Series:
  #apply this function to every entry of the series, that have been given as an input
  return raw_text.apply(txtToJSON)

# COMMAND ----------

#use the UDF to transform the raw text to an array of 3 JSON strings. We put the result of teh UDF in anohter column
df=df.withColumn("json_array",pandas_udf_toJSON(df["value"]))
df.show()

# COMMAND ----------

#bring out the first element of the array in a separate column
df = df.withColumn("json_dataset1",df["json_array"][0])
df.show()

# COMMAND ----------

#bring out the rest of the elements 
df = df.withColumn("json_dataset2",df["json_array"][1]).withColumn("json_dataset3",df["json_array"][2])
df.show()

# COMMAND ----------

#now let's cleanup and have the 3 dasets as 3 different columns
df = df["json_dataset1","json_dataset2","json_dataset3"]
df.show(truncate=False)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import *

schema = ArrayType(MapType(StringType(),StringType()))

dataset3_df = df.withColumn("json_dataset3", F.explode(F.from_json("json_dataset3", schema)))\
  .select("json_dataset3.id","json_dataset3.Name")
dataset3_df.show()

# COMMAND ----------

schema = ArrayType(MapType(StringType(),StringType()))

dataset2_df = df.withColumn("json_dataset2", F.explode(F.from_json("json_dataset2", schema)))\
  .select("json_dataset2.X","json_dataset2.Y")
dataset2_df.show()

# COMMAND ----------

schema = ArrayType(MapType(StringType(),StringType()))

dataset1_df = df.withColumn("json_dataset1", F.explode(F.from_json("json_dataset1", schema)))\
  .select("json_dataset1.A","json_dataset1.B","json_dataset1.C")
dataset1_df.show()

# COMMAND ----------

schema = ArrayType(MapType(StringType(),StringType()))

df.withColumn("json_dataset3", F.explode(F.from_json("json_dataset3", schema)))\
  .select("json_dataset3.id","json_dataset3.Name").show()

# COMMAND ----------

#write a dataframe using the delta format
dataset1_df.write.mode("append").format("delta").save("/Users/nikola.dyundev@databricks.com/complexUDF_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Users/nikola.dyundev@databricks.com/complexUDF_delta`

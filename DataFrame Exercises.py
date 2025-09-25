# Databricks notebook source
raw_fire_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/databricks-datasets//learning-spark-v2/sf-fire/sf-fire-calls.csv")

# COMMAND ----------

display(raw_fire_df)

# COMMAND ----------

renamed_fire_df = raw_fire_df \
    .withColumnRenamed("Call Number", "CallNumber") \
    .withColumnRenamed("Unit ID", "UnitId") \
    .withColumnRenamed("Incident Number", "IncidentNumber") \
    .withColumnRenamed("Call Date", "CallDate") \
    .withColumnRenamed("Watch Date", "WatchDate") \
    .withColumnRenamed("Call Final Disposition", "CallFinalDisposition") \
    .withColumnRenamed("Available DtTm", "AvailableDtTm") \
    .withColumnRenamed("Call Type", "CallType") \
    .withColumnRenamed("Call Type Group", "CallTypeGroup") \
    .withColumnRenamed("Zipcode of Incident", "Zipcode") \
    .withColumnRenamed("Station Area", "StationArea") \
    .withColumnRenamed("Original Priority", "OriginalPriority") \
    .withColumnRenamed("Final Priority", "FinalPriority") \
    .withColumnRenamed("Fire Prevention District", "FirePreventionDistrict") \
    .withColumnRenamed("Supervisor District", "SupervisorDistrict") \
    .withColumnRenamed("Unit sequence in call dispatch", "UnitSequenceInCallDispatch") \
    .withColumnRenamed("ALS Unit", "AlSUnit")

# COMMAND ----------

display(renamed_fire_df)

# COMMAND ----------

from pyspark.sql.functions import *

fire_df = renamed_fire_df.withColumn("CallDate", to_date("CallDate", "yyyy-MM-dd")) \
    .withColumn("WatchDate", to_date("WatchDate", "yyyy-MM-dd")) \
        .withColumn("AvailableDtTm", to_timestamp("AvailableDtTm", "yyyy-MM-dd hh:mm:ss a")) \
            .withColumn("Delay", round("Delay", 2))

# COMMAND ----------

# MAGIC %md
# MAGIC Q1. Quantos tipos diferentes de chamadas foram feitas ao Corpo de Bombeiros?

# COMMAND ----------

q1_df = fire_df.where("CallType is not null") \
    .select("CallType") \
        .distinct()

print(q1_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC Q2. Quais eram os diferentes tipos de chamadas feitas ao Corpo de Bombeiros?

# COMMAND ----------

q2_df = fire_df.where("CallType is not null") \
    .select(expr("CallType as distinct_call_types")) \
        .distinct()

q2_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q3. Quais foram todas as respostas com tempos de atraso maiores que 5 minutos?

# COMMAND ----------

q3_df = fire_df.select("CallNumber", "Delay") \
        .where("Delay > 5")

q3_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q.4 Quais eram os tipos de chamada mais comuns?

# COMMAND ----------

q4_df = fire_df.where("CallType is not null") \
    .select("CallType") \
        .groupBy("CallType") \
            .count() \
                .orderBy(desc("count"))

q4_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q5. Quais CEPs foram responsáveis ​​pelas chamadas mais comuns?

# COMMAND ----------

q5_df = fire_df.select("CallType", "Zipcode") \
    .where("CallType is not null") \
        .groupBy("CallType", "Zipcode") \
            .count() \
                .orderBy(desc("count"))
display(q5_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Q6. Quais bairros de São Francisco estão nos códigos postais 94102 e 94103?

# COMMAND ----------

q6_df = fire_df.select("Neighborhood", "Zipcode") \
    .where("Zipcode == 94102 or Zipcode == 94103") \
        
display(q6_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Q7. Qual foi a soma de todos os alarmes de chamada, média, mínimo e máximo dos tempos de resposta da chamada?

# COMMAND ----------

q7_df = fire_df.select("NumAlarms", "Delay") \
    .agg(
      sum("NumAlarms"), 
      min("Delay"), 
      avg("Delay"), 
      max("Delay"))
          
q7_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q8. Quantos anos distintos de dados existem no conjunto de dados?

# COMMAND ----------

q8_df = fire_df.select(year("CallDate")) \
    .distinct() \
        .orderBy(year("CallDate"), aescending=False)
q8_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q9. Qual semana do ano de 2018 teve mais chamados de incêndio?

# COMMAND ----------

q9_df = fire_df.select("CallDate") \
.where(year(col("CallDate")) == 2018)  \
        .groupBy(weekofyear("CallDate")) \
            .count() \
                .orderBy(desc("count"))
q9_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Q10. Quais bairros de São Francisco tiveram o pior tempo de resposta em 2018?

# COMMAND ----------

q10_df = fire_df.select("Neighborhood", "Delay") \
    .where(year(("CallDate")) == 2018) \
        .orderBy("Delay", ascending=False)
q10_df.display()
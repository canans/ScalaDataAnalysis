// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC This notebook will show you how to create and query a table or DataFrame that you uploaded to DBFS. [DBFS](https://docs.databricks.com/user-guide/dbfs-databricks-file-system.html) is a Databricks File System that allows you to store data for querying inside of Databricks. This notebook assumes that you have a file already inside of DBFS that you would like to read from.
// MAGIC 
// MAGIC This notebook is written in **Python** so the default cell type is Python. However, you can use different languages by using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported.

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC /* Query the created temp table in a SQL cell */
// MAGIC 
// MAGIC select * from `PastWeaponSalesOrders_csv`

// COMMAND ----------

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC 
// MAGIC %scala
// MAGIC lazy val session: SparkSession = {
// MAGIC     SparkSession
// MAGIC       .builder()
// MAGIC       .master("local")
// MAGIC       .appName("Scala-Project")
// MAGIC       .getOrCreate()
// MAGIC   }

// COMMAND ----------

// MAGIC %scala
// MAGIC val salesOrderSchema: StructType = StructType(Array(
// MAGIC     StructField("sCustomerId", IntegerType,false),
// MAGIC     StructField("sCustomerName", StringType,false),
// MAGIC     StructField("sItemId", IntegerType,true),
// MAGIC     StructField("sItemName",  StringType,true),
// MAGIC     StructField("sItemUnitPrice",DoubleType,true),
// MAGIC     StructField("sOrderSize", DoubleType,true),
// MAGIC     StructField("sAmountPaid",  DoubleType,true)
// MAGIC   ))

// COMMAND ----------

// MAGIC 
// MAGIC %scala
// MAGIC def buildSalesOrders(dataSet: String): DataFrame = {
// MAGIC    session.read
// MAGIC       .format("com.databricks.spark.csv")
// MAGIC       .option("header", true).schema(salesOrderSchema).option("nullValue", "")
// MAGIC       .option("treatEmptyValuesAsNulls", "true")
// MAGIC       .load(dataSet).cache()
// MAGIC 
// MAGIC   }

// COMMAND ----------

 val salesOrdersDf = buildSalesOrders("/FileStore/tables/PastWeaponSalesOrders-3.csv")

// COMMAND ----------


    salesOrdersDf.show

// COMMAND ----------


    salesOrdersDf.printSchema()

// COMMAND ----------

import org.apache.spark.mllib.recommendation.{ALS, Rating}
import session.implicits._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// MAGIC %scala
// MAGIC val ratingsDf: DataFrame = salesOrdersDf.map( salesOrder =>
// MAGIC     Rating( salesOrder.getInt(0),
// MAGIC       salesOrder.getInt(2),
// MAGIC       salesOrder.getDouble(6)
// MAGIC     ) ).toDF("user", "item", "rating")
// MAGIC 
// MAGIC   

// COMMAND ----------


  ratingsDf.show


// COMMAND ----------

// MAGIC %scala
// MAGIC val ratings: RDD[Rating] = ratingsDf.rdd.map( row => Rating( row.getInt(0), row.getInt(1), row.getDouble(2) ) )
// MAGIC  

// COMMAND ----------

// MAGIC  %scala
// MAGIC println( ratings.take(10).mkString("--- ") )

// COMMAND ----------

  import org.apache.spark.mllib.recommendation.MatrixFactorizationModel

// COMMAND ----------

// MAGIC %scala
// MAGIC val ratingsModel: MatrixFactorizationModel = ALS.train(ratings,6,10,15.0)

// COMMAND ----------

// MAGIC 
// MAGIC %scala
// MAGIC  val salesLeadSchema: StructType = StructType(Array(
// MAGIC     StructField("sCustomerId", IntegerType,false),
// MAGIC     StructField("sCustomerName", StringType,false),
// MAGIC     StructField("sItemId", IntegerType,true),
// MAGIC     StructField("sItemName",  StringType,true)
// MAGIC   ))

// COMMAND ----------

// MAGIC  %scala
// MAGIC  def buildSalesLeads (dataSet: String): DataFrame = {
// MAGIC     session.read
// MAGIC       .format("com.databricks.spark.csv")
// MAGIC       .option("header", true).schema(salesLeadSchema).option("nullValue", "")
// MAGIC       .option("treatEmptyValuesAsNulls", "true")
// MAGIC       .load(dataSet).cache()
// MAGIC   }

// COMMAND ----------

val weaponSalesLeadDf = buildSalesLeads("/FileStore/tables/WeaponSalesLeads-4.csv")

// COMMAND ----------

// MAGIC %scala
// MAGIC  weaponSalesLeadDf.show

// COMMAND ----------

  weaponSalesLeadDf.show

// COMMAND ----------

// MAGIC %scala
// MAGIC val customerWeaponsSystemPairDf: DataFrame = weaponSalesLeadDf.map(salesLead => ( salesLead.getInt(0), salesLead.getInt(2) )).toDF("user","item")

// COMMAND ----------


  customerWeaponsSystemPairDf.show

// COMMAND ----------

// MAGIC %scala
// MAGIC  val customerWeaponsSystemPairRDD: RDD[(Int, Int)] = customerWeaponsSystemPairDf.rdd.map(row => (row.getInt(0), row.getInt(1)) )
// MAGIC   println( customerWeaponsSystemPairRDD.take(10).mkString("------ "))

// COMMAND ----------

val weaponRecs: RDD[Rating] = ratingsModel.predict(customerWeaponsSystemPairRDD).distinct()
  println("Future ratings are: " + weaponRecs.foreach(rating => { println( "Customer Nation:" + rating.user + " Weapons System:  " + rating.product + " Rating: " + rating.rating ) } ) )





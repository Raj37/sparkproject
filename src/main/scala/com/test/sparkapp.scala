package com.test

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType}

object sparkapp {
  def main(args: Array[String]){
  // We'll run our project in local mode
  val conf = new SparkConf().
    setAppName("word-count").
    setMaster("local[*]")

  // Here we initialize the context with the configuration above
  val sc = new SparkContext(conf)

  // sqlcontext created
  val sqlContext = new SQLContext(sc)

    val customSchema = StructType(Array(
      StructField("server", StringType, true),
      StructField("country", StringType, true),
      StructField("ddmi_servertype", StringType, true),
      StructField("ddmi_region", StringType, true)
      ))
  // load csv file using databricks csv jar
 /* val df = sqlContext.read
    .format("com.databricks.spark.csv")
    .option("header", "true") // Use first line of all files as header
    .option("inferSchema", "true") // Automatically infer data types
    .load("src//main//resources//ddmi.csv")*/

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true") // Use first line of all files as header
      .schema(customSchema)
      .load("src//main//resources//ddmi.csv")

   // show entire csv file data
    df.show()

  /*  // show Server column records
    df.select("Server").show()

    // show Server Type column records
    df.select("Server Type").show()

    // register as ddmi_tbl
    df.registerTempTable("ddmi_tbl")

    // create dataframe for Server and Server Type columns

    val res = sqlContext.sql("select Server, 'Server Type' from ddmi_tbl")

    // schema details
    res.printSchema()

    // show res records
    res.show()*/

}
}

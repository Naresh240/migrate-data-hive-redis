package org.example

import org.apache.spark.sql.{SaveMode, SparkSession}

object redis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("redis-df")
      .master("local[*]")
      .config("spark.redis.host", "54.172.32.246")
      .config("spark.redis.port", "6379")
      .enableHiveSupport()
      .getOrCreate()
    val employeeData = spark.sql("select * from employee")

    employeeData.write
      .format("org.apache.spark.sql.redis")
      .option("table", "employee")
      .mode(SaveMode.Overwrite)
      .save()

    val loadedDf = spark.read
      .format("org.apache.spark.sql.redis")
      .option("table", "employee")
      .load()
    loadedDf.printSchema()
    loadedDf.show()
  }
}

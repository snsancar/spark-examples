package org.shellinterview

import org.apache.spark.sql.SparkSession

import java.util.Properties

object PostgresDocker extends App {

  val spark = SparkSession
    .builder()
    .appName("Test-Postgres")
    .master("local")
    .config("spark.driver.bindAddress", "localhost")
    .getOrCreate()

  import spark.implicits._

  val database = "postgres"
  val jdbcUrl = s"jdbc:postgresql://0.0.0.0:5432/$database"
  val user = "postgres"
  val password = "pg_password"
  val properties = new Properties()
  properties.setProperty("user", user)
  properties.setProperty("password", password)
  properties.put("driver", "org.postgresql.Driver")

  val df = Seq((3, "shankar")).toDF("id", "name")
  //  df.show(false)

  //  df.write.mode(SaveMode.Overwrite).jdbc(jdbcUrl, "test", properties)


}

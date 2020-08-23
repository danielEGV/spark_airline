package co.com.sparkairline

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
    SparkSession.builder().master("local[4]").appName("spark session").getOrCreate()
  }

}
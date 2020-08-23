package co.com.sparkairline.challenge

import co.com.sparkairline.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, Dataset}

trait AirlineDataWrapper { this: SparkSessionWrapper =>
  import spark.implicits._

  private val testFlightsFiles = Seq(
    "./airline_data/flights/2015.csv",
    "./airline_data/flights/2016.csv"
  )

  lazy val loadFlights: DataFrame =
    spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./airline_data/flights/*.csv")
      //.load(testFlightsFiles: _*)
      .cache()

  lazy val loadAirports: DataFrame =
    spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./airline_data/airports/airports.csv")
      .cache()

  lazy val loadAirlines: Dataset[Airline] =
    spark
      .read
      .format("csv")
      .option("sep", ",")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("./airline_data/airlines/airlines.csv")
      .as[Airline]
      .cache()

}

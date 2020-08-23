package co.com.sparkairline.challenge

import java.time.LocalDate
import java.time.format.{DateTimeFormatter, TextStyle}

import co.com.sparkairline.SparkSessionWrapper
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{collect_list, size, udf}

object AirlinesService {

  private val numDoubleRegex = "^[0-9]+(\\.[0-9]+)?$"
  private val numIntRegex = "^([0-9]+)$"

/**
  * Un vuelo se clasifica de la siguiente manera:
  * ARR_DELAY < 5 min --- On time
  * 5 > ARR_DELAY < 45min -- small Delay
  * ARR_DELAY > 45min large delay
  * */
  private def toTimeClass: Option[String] => Option[String] = { number =>
    number
      .filter(_.matches(numDoubleRegex))
      .map { _.toDouble match {
          case d if d < 5 => "ON_TIME"
          case d if d >= 5 && d < 45 => "SMALL_DELAY"
          case d if d >= 45 => "LARGE_DELAY"
        }
      }
  }

  private def toYear(date: String): Int = {
    val formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss")
    LocalDate.parse(date, formatter).getYear
  }

  private def toDayOfTheWeek: String => String = { date =>
    val formatter = DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss")
    LocalDate.parse(date, formatter).getDayOfWeek.getDisplayName(TextStyle.FULL, formatter.getLocale)
  }


/**
  * Encuentre los destinos a partir de un origen, y de acuerdo a DEP_TIME clasifique el vuelo de la siguiente manera:
  * 00:00 y 8:00 - Morning
  * 8:01 y 16:00 - Afternoon
  * 16:01 y 23:59 - Night
  **/
  private def toPeriodTime: Double => String = {
    case t if (0 <= t && t <= 800) || t == 2400 => "Morning"
    case t if 800 < t && t <= 1600 => "Afternoon"
    case t if 1600 < t && t < 2400 => "Night"
  }

  private def yearToInt(year: Option[String]): Option[Int] = {
    year
      .filter(_.matches(numIntRegex))
      .flatMap {
        _.toInt match {
          case i if 2009 <= i && i <= 2018 => Some(i)
          case _ => None
        }
      }
  }

/**
  * Encuentre los vuelos más cancelados y cual es la causa mas frecuente
  * Un vuelo es cancelado si CANCELLED = 1
  * CANCELLATION_CODE A - Airline/Carrier; B - Weather; C - National Air System; D - Security
  **/
  private def toCancelledDetail: String => String = {
    case "A" => "Airline/Carrier"
    case "B" => "Weather"
    case "C" => "National Air System"
    case "D" => "Security"
    case _ => "Other"
  }


  private def groupCancelledDetail: Array[String] => Array[(String, Int)] = { details =>
    details
      .groupBy(identity)
      .map { case (k, v) => (k, v.length)}
      .toArray
      .sortBy(- _._2)
  }


}

class AirlinesService extends SparkSessionWrapper with AirlineDataWrapper {
  import spark.implicits._
  import AirlinesService._
  import co.com.sparkairline.util.SparkOps._


  def delayedAirlines(ds: Dataset[AirlineDelay], year: Option[String]): Seq[AirlineStats] = {
    ds
      .filterOpt(yearToInt(year)) { case (t, y) => toYear(t.FL_DATE) == y }
      .map(airlineDelay => (airlineDelay.OP_CARRIER, toTimeClass(airlineDelay.ARR_DELAY)))
      .filter(_._2.isDefined)
      .groupBy('_1)
      .pivot('_2)
      .count()
      .withColumn("totalFlights", 'LARGE_DELAY + 'ON_TIME + 'SMALL_DELAY)
      .joinWith(loadAirlines, '_1 === 'IATA_CODE, "inner")
      .select(
        $"_2.AIRLINE" as "name",
        $"_1.LARGE_DELAY" as "largeDelayFlights",
        $"_1.ON_TIME" as "onTimeFlights",
        $"_1.SMALL_DELAY" as "smallDelayFlights",
        $"_1.totalFlights"
      )
      .sort($"largeDelayFlights" desc, $"smallDelayFlights" desc, $"ontimeFlights" desc)
      .as[AirlineStats]
      .collect()
      .toSeq
  }

  def destinations(ds: DataFrame, origin: String): Seq[FlightsStats] = {
    val periodTimeUdf = udf(toPeriodTime)

    ds
      .where('ORIGIN === origin && 'DEP_TIME.isNotNull)
      .withColumn("period", periodTimeUdf('DEP_TIME))
      .select('DEST, 'period)
      .where('period isNotNull)
      .groupBy('DEST)
      .pivot('period)
      .count()
      .select(
        'DEST as "destination",
        'Afternoon as "afternoonFlights",
        'Morning as "morningFlights",
        'Night as "nightFlights"
      )
      .where('afternoonFlights.isNotNull && 'morningFlights.isNotNull && 'nightFlights.isNotNull)
      .as[FlightsStats]
      .collect()
      .toSeq
  }

  def flightInfo(ds: DataFrame): Seq[CancelledFlight] = {
    val cancelledDetailUdf = udf(toCancelledDetail)
    val causesUdf = udf { rows: Seq[String] => groupCancelledDetail(rows.toArray) }

    ds
      .select('OP_CARRIER_FL_NUM, 'ORIGIN, 'DEST, 'CANCELLED, 'CANCELLATION_CODE)
      .where('CANCELLED === 1)
      .withColumn("CANCELLATION_DETAIL", cancelledDetailUdf('CANCELLATION_CODE))
      .groupBy('OP_CARRIER_FL_NUM, 'ORIGIN, 'DEST)
      .agg(collect_list('CANCELLATION_DETAIL) as 'CANCELLATION_DETAILS)
      .withColumn("cancelled", size('CANCELLATION_DETAILS))
      .sort('cancelled desc)
      .limit(20)
      .withColumn("causes", causesUdf('CANCELLATION_DETAILS))
      .select('OP_CARRIER_FL_NUM as "number", 'ORIGIN, 'DEST as "destination", 'cancelled, 'causes)
      .as[CancelledFlight]
      .collect()
      .toSeq
  }

  def daysWithDelays(ds: DataFrame): Seq[(String, Int)] = {
    val dateOfTheWeekUdf = udf(toDayOfTheWeek)

    ds
      .where('ARR_DELAY > 45)
      .withColumn("day", dateOfTheWeekUdf('FL_DATE))
      .groupBy('day)
      .count()
      .sort('count desc)
      .collect()
      .map(r => (r.getAs[String](0), r.getAs[Int](1)))
      .toSeq

  }
}

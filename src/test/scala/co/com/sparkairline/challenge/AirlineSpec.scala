package co.com.sparkairline.challenge

import co.com.sparkairline.SparkSessionWrapper
import org.scalatest.FunSpec

class AirlineSpec extends FunSpec with SparkSessionWrapper with AirlineDataWrapper {
  import spark.implicits._

  private val airlines: AirlinesService = new AirlinesService()

  describe("airline stats") {
    it("should find airline delay") {

      val r = airlines.delayedAirlines(loadFlights.as[AirlineDelay], None)

      assert(r.forall(_.totalFlights > 0))
      assert(r.forall(_.largeDelayFlights > 0))
      assert(r.forall(_.onTimeFlights > 0))
      assert(r.forall(_.smallDelayFlights > 0))

      r.foreach { t =>
        println(t)
      }
    }

    it("should find airline delay with specific day") {

      val r = airlines.delayedAirlines(loadFlights.as[AirlineDelay], Some("2015"))

      r.foreach { t =>
        println(t)
      }

      assert(r.forall(_.totalFlights > 0))
      assert(r.forall(_.largeDelayFlights > 0))
      assert(r.forall(_.onTimeFlights > 0))
      assert(r.forall(_.smallDelayFlights > 0))
    }

    it("should find destination") {
      val origin = "ATL"

      val r = airlines.destinations(loadFlights, origin)

      assert(r.forall(_.destination != origin))
      assert(r.nonEmpty)

      r.foreach { t =>
        println(t)
      }
    }

    it("should find cancelled flights") {
      val r = airlines.flightInfo(loadFlights)

      assertResult(20)(r.length)

      r.foreach { t =>
        println(t)
      }
    }

    it("should get the days with the most delays") {
      val r = airlines.daysWithDelays(loadFlights)

      assertResult(7)(r.length)

      r.foreach { t =>
        println(t)
      }
    }
  }
}

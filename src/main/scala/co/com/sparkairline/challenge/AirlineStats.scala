package co.com.sparkairline.challenge

case class AirlineStats(
                         name: String,
                        totalFlights: Long,
                        largeDelayFlights: Long,
                        smallDelayFlights: Long,
                        onTimeFlights: Long
                       )

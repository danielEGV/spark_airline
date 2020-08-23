package co.com.sparkairline.challenge


case class AirlineDelay(
                         FL_DATE: String,
                         OP_CARRIER: String,
                         ORIGIN: String,
                         DEST: String,
                         DEP_DELAY: Option[String],
                         ARR_DELAY: Option[String]
                       )

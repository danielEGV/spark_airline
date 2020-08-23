package co.com.sparkairline.challenge

case class CancelledFlight(number: Int, origin: String, destination: String, cancelled: Long, causes: List[(String,Int)])

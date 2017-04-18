package com.manonthegithub.bz

import java.nio.file.Paths
import java.time.{LocalDateTime, LocalTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString

import scala.collection.{immutable, mutable}
import scala.concurrent.duration.Duration

object CsvConverterApp extends App {

  implicit val system = ActorSystem("csv-converter")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher


  val FilePath = Paths.get("/Users/Kirill/Desktop/logins0.csv")
  val CsvSeparator: Char = ','
  val StringSeparator: ByteString = ByteString("\n")
  val DateFormatString = "yyyy-MM-dd HH:mm:ss"

  FileIO.fromPath(FilePath)
    .via(Framing.delimiter(StringSeparator, 1000, false))
    .map(_.utf8String.replace("\"", "").split(CsvSeparator))
    .filter(_.size == 3)
    .map(a => (a(0), a(1), LocalDateTime.parse(a(2), DateTimeFormatter.ofPattern(DateFormatString))))
    .runForeach(println)
    .onComplete {
      case _ => system.terminate()
    }


}


class UserAuthAggregator(period: Duration,
                         zoneOffset: ZoneOffset = ZoneOffset.UTC,
                         countdownPoint: LocalDateTime = LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC)) {

  val tracker = mutable.Map.empty[String, mutable.Seq[(String, LocalDateTime)]]
  var start: LocalDateTime = _


  def update(ip: String, username: String, date: LocalDateTime): Option[mutable.Map[String, mutable.Seq[(String, LocalDateTime)]]] = {

    if (start == null) start = computeStart(date)

    if (start.plusSeconds(period.toSeconds).isAfter(date)) {
      tracker.update(ip, tracker.getOrElse(ip, mutable.Seq.empty[(String, LocalDateTime)]) :+ (username, date))
      None
    } else {
      start = computeStart(date)
      val multInPeriod = tracker.clone()
      tracker.clear()
      Some(multInPeriod)
    }

  }

  private def computeStart(firstIncoming: LocalDateTime): LocalDateTime = {
    val startSecond = countdownPoint.toEpochSecond(zoneOffset)
    val periodSeconds = period.toSeconds
    val firstIncomingSeconds = firstIncoming.toEpochSecond(zoneOffset)
    if (firstIncoming.isAfter(countdownPoint)) {
      val startDiffSeconds = ((firstIncomingSeconds - startSecond) / periodSeconds) * periodSeconds
      LocalDateTime.ofEpochSecond(startSecond + startDiffSeconds, 0, zoneOffset)
    } else {
      val startDiffSeconds = ((startSecond - firstIncomingSeconds) / periodSeconds) * periodSeconds
      LocalDateTime.ofEpochSecond(firstIncomingSeconds + startDiffSeconds, 0, zoneOffset)
    }

  }

}
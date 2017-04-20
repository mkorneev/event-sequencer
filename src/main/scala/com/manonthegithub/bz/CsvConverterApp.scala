package com.manonthegithub.bz

import java.nio.file.Paths
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream.{ActorMaterializer, SourceShape}
import akka.stream.scaladsl.{Broadcast, FileIO, Flow, Framing, GraphDSL, Source}
import akka.util.ByteString

import scala.collection.mutable
import scala.concurrent.duration._

object CsvConverterApp extends App {

  implicit val system = ActorSystem("csv-converter")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val FilePath = Paths.get("C:\\Users\\yankov.kirill\\Desktop\\logins0.csv")
  val CsvSeparator: Char = ','
  val StringSeparator: ByteString = ByteString("\n")
  val DateFormatString = "yyyy-MM-dd HH:mm:ss"

  val countdownStart = LocalDateTime.parse("2015-11-30T23:10:40")

  val tuplesSource = FileIO.fromPath(FilePath)
    .via(Framing.delimiter(StringSeparator, 1000, false))
    .map(_.utf8String.replace("\"", "").split(CsvSeparator))
    .filter(_.size == 3)
    .map(a => (a(0), a(1), LocalDateTime.parse(a(2), DateTimeFormatter.ofPattern(DateFormatString))))

  val periodsFlow = Flow[LocalDateTime].prepend(Source.single(countdownStart))

  val temp = periodsFlow
    .zip(tuplesSource)
    .map(tpl => (tpl._1, UserAuthAggregator.startOfPeriod(tpl._2._3, 10 minutes), tpl._2))


  //  import GraphDSL.Implicits._
  //  GraphDSL.create(temp){ implicit b => periods =>
  //
  //    val br = b.add(Broadcast[](2))
  //
  //
  //
  //
  //
  //    SourceShape()
  //  }


}

object UserAuthAggregator {

  val DefaultZone = ZoneOffset.UTC

  def startOfPeriod(date: LocalDateTime,
                    period: Duration,
                    zoneOffset: ZoneOffset = UserAuthAggregator.DefaultZone,
                    countdownPoint: LocalDateTime = LocalDateTime.ofEpochSecond(0, 0, UserAuthAggregator.DefaultZone)
                   ): LocalDateTime = {
    val startSecond = countdownPoint.toEpochSecond(zoneOffset)
    val periodSeconds = period.toSeconds
    val firstIncomingSeconds = date.toEpochSecond(zoneOffset)
    if (date.isAfter(countdownPoint)) {
      val startDiffSeconds = ((firstIncomingSeconds - startSecond) / periodSeconds) * periodSeconds
      LocalDateTime.ofEpochSecond(startSecond + startDiffSeconds, 0, zoneOffset)
    } else {
      val startDiffSeconds = ((startSecond - firstIncomingSeconds) / periodSeconds + 1) * periodSeconds
      LocalDateTime.ofEpochSecond(startSecond - startDiffSeconds, 0, zoneOffset)
    }
  }

}

class UserAuthAggregator(period: Duration,
                         zoneOffset: ZoneOffset = UserAuthAggregator.DefaultZone,
                         countdownPoint: LocalDateTime = LocalDateTime.ofEpochSecond(0, 0, UserAuthAggregator.DefaultZone)) {

  val tracker = mutable.Map.empty[String, mutable.Seq[(String, LocalDateTime)]]
  var start: LocalDateTime = _


  def update(ip: String, username: String, date: LocalDateTime): Option[mutable.Map[String, mutable.Seq[(String, LocalDateTime)]]] = {

    if (start == null) start = startOfPeriod(date)

    if (start.plusSeconds(period.toSeconds).isAfter(date)) {
      tracker.update(ip, tracker.getOrElse(ip, mutable.Seq.empty[(String, LocalDateTime)]) :+ (username, date))
      None
    } else {
      start = startOfPeriod(date)
      val multInPeriod = tracker.clone()
      tracker.clear()
      Some(multInPeriod)
    }

  }

  private def startOfPeriod(date: LocalDateTime,
                            zoneOffset: ZoneOffset = UserAuthAggregator.DefaultZone,
                            countdownPoint: LocalDateTime = LocalDateTime.ofEpochSecond(0, 0, UserAuthAggregator.DefaultZone)
                           ): LocalDateTime = {
    val startSecond = countdownPoint.toEpochSecond(zoneOffset)
    val periodSeconds = period.toSeconds
    val firstIncomingSeconds = date.toEpochSecond(zoneOffset)
    if (date.isAfter(countdownPoint)) {
      val startDiffSeconds = ((firstIncomingSeconds - startSecond) / periodSeconds) * periodSeconds
      LocalDateTime.ofEpochSecond(startSecond + startDiffSeconds, 0, zoneOffset)
    } else {
      val startDiffSeconds = ((startSecond - firstIncomingSeconds) / periodSeconds) * periodSeconds
      LocalDateTime.ofEpochSecond(firstIncomingSeconds + startDiffSeconds, 0, zoneOffset)
    }
  }

}
package com.manonthegithub.bz

import java.nio.file.Paths
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString
import com.mkorneev.bz.{EventSequencerFlow, EventsSeq}

import scala.concurrent.ExecutionContextExecutor
import scala.util.Try

object CsvAggregatorApp extends App {

  implicit val system: ActorSystem = ActorSystem("csv-converter")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val InputFilePath = Paths.get(args(0))
  val OutputFilePath = Paths.get(args(1))

  val CsvSeparator = ','
  val Quote = "\""
  val StringSeparator = "\n"
  val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  val Period = Try {
    Duration.ofSeconds(args(2).toInt)
  }.getOrElse({
    println("Period is not supplied or can not be parsed, using default value.")
    Duration.ofMinutes(10)
  })

  if (InputFilePath.toFile.canRead && OutputFilePath.toFile.createNewFile) {
    println(s"Input file: $InputFilePath, output file: $OutputFilePath, period: $Period")
    println("Started computation, please wait.")
    FileIO.fromPath(InputFilePath)
      .via(Framing.delimiter(ByteString(StringSeparator), 1000, allowTruncation = false))
      .map(_.utf8String
        .replace(Quote, "")
        .split(CsvSeparator))
      .filter(_.length == 3)
      .map(a => UserAuthEvent(a(0), a(1), LocalDateTime.parse(a(2), dateTimeFormatter)))
      .via(Flow.fromGraph(new EventSequencerFlow(Period)))
      .map(toCsvString)
      .log("MultiAuth")
      .map(ByteString(_))
      .runWith(FileIO.toPath(OutputFilePath))
      .onComplete {
        _ =>
          system.terminate()
          println(s"Operation successfully performed, you can view results in file: $OutputFilePath")
      }
  } else {
    system.terminate()
    println("One of supplied file paths is wrong, can not perform read or write. Please restart program with other parameters.")
  }

  def toCsvString(tuple: (String, EventsSeq[String])): String = {
    s"$Quote${tuple._1}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.startTime.format(dateTimeFormatter)}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.endTime.format(dateTimeFormatter)}$Quote$CsvSeparator" +
      s"$Quote${multiUserString(tuple._2)}$Quote" +
      StringSeparator
  }

  def multiUserString(events: EventsSeq[String]): String = {
    events.values.map(e => s"${e._2}:${e._1.format(dateTimeFormatter)}").mkString(",")
  }

}

case class UserAuthEvent(username: String, ip: String, date: LocalDateTime)


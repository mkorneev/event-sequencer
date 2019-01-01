package com.mkorneev.bz

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvParsing, CsvQuotingStyle}
import akka.stream.checkpoint.DropwizardBackend._
import akka.stream.checkpoint.scaladsl.Checkpoint
import akka.stream.scaladsl.{FileIO, Flow}
import akka.util.ByteString
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.typesafe.scalalogging.Logger
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.ExecutionContextExecutor
import scala.util.{Failure, Success}

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    """Usage: EventSequencerApp <input-file> <output-file> [period]
      |
      |Options:
      |""".stripMargin)

  val inputFile: ScallopOption[File] = trailArg[File]()
  val outputFile: ScallopOption[File] = trailArg[File]()
  val period: ScallopOption[Int] = trailArg[Int](required = false,
    descr = "time window in seconds, 1 hour by default", default = Some(3600))

  validateFileExists(inputFile)
  validateFileDoesNotExist(outputFile)
  verify()
}

object EventSequencerApp {
  val logger = Logger("EventSequencerApp")

  private val dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  implicit val system: ActorSystem = ActorSystem("EventSequencerApp")
  implicit val mat: ActorMaterializer = ActorMaterializer()
  implicit val ec: ExecutionContextExecutor = system.dispatcher
  implicit val metricRegistry: MetricRegistry = new MetricRegistry()

  def main(args: Array[String]) {
    val conf = new Conf(args)

    logger.info("Input file: {}, output file: {}, period: {} seconds",
      conf.inputFile(), conf.outputFile(), conf.period())
    logger.info("Started computation, please wait")

    val reporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build

    FileIO.fromPath(conf.inputFile().toPath)
      .via(buildEventFlow(conf.period()))
      .runWith(FileIO.toPath(conf.outputFile().toPath))
      .onComplete {
        result => {
          system.terminate()

          result match {
            case Success(s) => s.status match {
              case Success(_) =>
                reporter.report()

                logger.info("Operation successfully performed, written {} bytes. You can find results in {}",
                  s.count, conf.outputFile())
              case Failure(f) =>
                logger.error("IO failure", f)
            }
            case Failure(f) =>
              logger.error("Processing failure", f)
          }
        }
      }
  }

  def buildEventFlow(period: Int): Flow[ByteString, ByteString, NotUsed] = {
    val workerCount = 8

    Flow[ByteString]
      .via(CsvParsing.lineScanner())
      .via(Checkpoint("read"))
      .groupBy(workerCount, l => Math.abs(l(1).hashCode()) % workerCount)
      .map(_.map(_.utf8String))
      .map(s => UserAuthEvent(s(0), s(1), LocalDateTime.parse(s(2), dateTimeFormatter)))
      .via(Flow.fromGraph(new EventSequencerFlow(Duration.ofSeconds(period))))
      .via(Checkpoint("groupped"))

      .map(Function.tupled(toList))
      .via(CsvFormatting.format(quotingStyle = CsvQuotingStyle.Always))
      .via(Checkpoint("ready"))
      .async
      .mergeSubstreams
  }

  def toList(ip: String, events: EventsSeq[String]): List[String] = {
    List(ip, events.startTime.format(dateTimeFormatter), events.endTime.format(dateTimeFormatter),
      multiUserString(events))
  }

  def multiUserString(events: EventsSeq[String]): String = {
    events.values.map {
      case (time, value) => s"$value:${time.format(dateTimeFormatter)}"
    }.mkString(",")
  }
}

case class UserAuthEvent(username: String, ip: String, date: LocalDateTime)

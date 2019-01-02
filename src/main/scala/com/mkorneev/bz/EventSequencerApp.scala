package com.mkorneev.bz

import java.io.File
import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDateTime}
import java.util.Comparator
import java.util.concurrent.TimeUnit

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.alpakka.csv.scaladsl.{CsvFormatting, CsvQuotingStyle}
import akka.stream.checkpoint.DropwizardBackend._
import akka.stream.checkpoint.scaladsl.Checkpoint
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.util.ByteString
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.github.tototoshi.csv.{CSVParser, defaultCSVFormat}
import com.typesafe.scalalogging.Logger
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.concurrent.{ExecutionContextExecutor, Future}
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

    logger.info("Sorting the input file")

    val sortedFile: File = sortExternally(conf.inputFile())

    logger.info("Starting the computation, please wait")

    val reporter = ConsoleReporter
      .forRegistry(metricRegistry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build

    FileIO.fromPath(sortedFile.toPath)
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

  def sortExternally(inputFile: File): File = {
    val tempFile = File.createTempFile("external_sort", "output")
    tempFile.deleteOnExit()

    // sort by the third field (separated by comma)
    val thirdFieldComparator = new Comparator[String]() {
      override def compare(s1: String, s2: String): Int = {
        s1.split(',')(2).compareTo(s2.split(',')(2))
      }
    }

    FixedExternalSort.sort(inputFile, tempFile, thirdFieldComparator)

    tempFile
  }

  def buildEventFlow(period: Int): Flow[ByteString, ByteString, NotUsed] = {
    val parser = new CSVParser(defaultCSVFormat)
    val workerCount = 6

    Flow[ByteString]
      .via(Framing.delimiter(delimiter = ByteString("\n"), maximumFrameLength = 1000, allowTruncation = false))
      .via(Checkpoint("read"))
      .grouped(1000)
      // CSV parsing takes the most time, let's do it in parallel
      // Cannot use Alpakka [[CsvParsing.lineScanner]] here because it doesn't work line by line
      .mapAsync(workerCount)(bs => Future(bs.map(b => {
          val List(user, ip, date) = parser.parseLine(b.utf8String).get
          UserAuthEvent(user, ip, LocalDateTime.parse(date, dateTimeFormatter))
        }).toList  // need toList here for better performance
      ))
      .mapConcat(list => list)
      .via(Flow.fromGraph(new EventSequencerFlow(Duration.ofSeconds(period))))
      .map(Function.tupled(toList))
      .via(CsvFormatting.format(quotingStyle = CsvQuotingStyle.Always))
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

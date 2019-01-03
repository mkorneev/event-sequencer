package com.mkorneev.event_sequencer

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
import ch.qos.logback.classic
import ch.qos.logback.classic.Level
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import com.github.tototoshi.csv.{CSVParser, defaultCSVFormat}
import com.typesafe.scalalogging.Logger
import org.rogach.scallop.{ScallopConf, ScallopOption}
import org.slf4j
import org.slf4j.LoggerFactory

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.util.{Failure, Success}


class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  banner(
    """Usage: java -jar path-to-jar <input-file> <output-file> [period]
      |
      |Options:
      |""".stripMargin)

  val debug: ScallopOption[Boolean] = opt[Boolean](descr = "will show flow throughput and latency statistics")

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

  private val reporter = ConsoleReporter
    .forRegistry(metricRegistry)
    .convertRatesTo(TimeUnit.SECONDS)
    .convertDurationsTo(TimeUnit.MILLISECONDS)
    .build

  def main(args: Array[String]) {
    val conf = new Conf(args)

    if (conf.debug.getOrElse(false)) {
      LoggerFactory.getLogger(slf4j.Logger.ROOT_LOGGER_NAME).asInstanceOf[classic.Logger].setLevel(Level.DEBUG)
    }

    logger.info("Input file: {}, output file: {}, period: {} seconds",
      conf.inputFile(), conf.outputFile(), conf.period())

    logger.info("Sorting the input file")

    val sortedFile: File = sortExternally(conf.inputFile())

    logger.info("Starting the computation, please wait")

    FileIO.fromPath(sortedFile.toPath)
      .via(buildEventFlow(conf.period()))
      .runWith(FileIO.toPath(conf.outputFile().toPath))
      .onComplete {
        result => {
          system.terminate()

          result match {
            case Success(s) => s.status match {
              case Success(_) =>
                logger.whenDebugEnabled({
                  reporter.report()
                })

                logger.info("Operation successfully performed. Processed {} records, written {} sequences. " +
                  "You can find the results in {}",
                  metricRegistry.getMeters.get("read_throughput").getCount,
                  metricRegistry.getMeters.get("write_throughput").getCount,
                  conf.outputFile())
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

    // sort by the last field (separated by comma)
    // won't work for multi-line CSV entries
    val lastFieldComparator = new Comparator[String]() {
      override def compare(s1: String, s2: String): Int = {
        val f1 = s1.split(',').last
        val f2 = s2.split(',').last
        f1.compareTo(f2)
      }
    }

    FixedExternalSort.sort(inputFile, tempFile, lastFieldComparator)

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
      .mapAsync(workerCount)(byteStrings =>
        Future(byteStrings.map(b => {
          val List(user, ip, date) = parser.parseLine(b.utf8String).get
          UserAuthEvent(user, ip, LocalDateTime.parse(date, dateTimeFormatter))
        }).toList  // need toList here for async parsing
      ))
      .mapConcat(list => list)

      .via(Flow.fromGraph(new EventSequencerFlow(Duration.ofSeconds(period))))
      .map(Function.tupled(eventsToList))
      .via(CsvFormatting.format(quotingStyle = CsvQuotingStyle.Always))
      .via(Checkpoint("write"))
  }

  def eventsToList(ip: String, events: EventsSeq[String]): List[String] = {
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

package com.manonthegithub.bz

import java.nio.file.Paths
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Flow, Framing}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import com.manonthegithub.bz.CsvConverterApp.UserAuthEvent

import scala.collection.immutable
import scala.concurrent.duration._
import scala.util.Try

object CsvConverterApp extends App {

  implicit val system = ActorSystem("csv-converter")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val InputFilePath = Paths.get(args(0))
  val OutputFilePath = Paths.get(args(1))

  val CsvSeparator = ','
  val Quote = "\""
  val StringSeparator = "\n"
  val DateFormatString = "yyyy-MM-dd HH:mm:ss"
  val Period = Try {
    args(2).toInt seconds
  }.toOption.getOrElse({
    println("Period is not supplied or can not be parsed, using default value.")
    10 minutes
  })

  if (InputFilePath.toFile.canRead && OutputFilePath.toFile.createNewFile) {
    println(s"Input file: $InputFilePath, output file: $OutputFilePath, period: $Period")
    println("Started computation, please wait.")
    FileIO.fromPath(InputFilePath)
      .via(Framing.delimiter(ByteString(StringSeparator), 1000, false))
      .map(_.utf8String
        .replace(Quote, "")
        .split(CsvSeparator))
      .filter(_.length == 3)
      //here events come in sorted by timestamp order, if not the logic would be violated
      .map(a => UserAuthEvent(a(0), a(1), LocalDateTime.parse(a(2), DateTimeFormatter.ofPattern(DateFormatString))))
      .via(Flow.fromGraph(new EventPeriodFlow(Period)))
      .mapConcat(s => s
        .groupBy(_.ip)
        //different filters can be applied
        .filter(ip => eventsByIpFilter(ip._2))
      ).map(toCsvString)
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

  def eventsByIpFilter(ipEvents: Seq[UserAuthEvent]): Boolean = ipEvents.size > 1

  def toCsvString(tuple: (String, Seq[UserAuthEvent])): String = {
    s"$Quote${tuple._1}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.head.date}$Quote$CsvSeparator" +
      s"$Quote${tuple._2.last.date}$Quote$CsvSeparator" +
      s"$Quote${multiUserString(tuple._2)}$Quote" +
      StringSeparator
  }

  def multiUserString(events: Seq[UserAuthEvent]): String = {
    //events are ordered by timestamp, otherwise they should be sorted before processing
    val sb = StringBuilder.newBuilder
    for (i <- events.indices) {
      val e = events(i)
      sb.append(s"${e.username}:${e.date}")
      if (i != events.size - 1) {
        sb.append(",")
      }
    }
    sb.toString
  }

  case class UserAuthEvent(username: String, ip: String, date: LocalDateTime)

}

/**
 * Flow aggregates events by time periods and sends downstream batch of events that correspond to single period.
 * <p>
 * !!!Attention!!!
 * Here made an assumption that incoming events flow is `ordered by timestamp`, if this is violated this stage won't work properly.
 *
 * @param period         period of aggregation
 * @param countdownStart start point for period countdown (default value is time of first incoming event)
 */
class EventPeriodFlow(period: FiniteDuration, countdownStart: Option[LocalDateTime] = None)
  extends GraphStage[FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]]] with DateUtils {

  private val in = Inlet[UserAuthEvent]("EventPeriodFlow.in")
  private val out = Outlet[immutable.Seq[UserAuthEvent]]("EventPeriodFlow.out")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    private var prevPeriod: Option[LocalDateTime] = None
    private var eventsInPeriod = immutable.Seq.empty[UserAuthEvent]
    private var firstIncomingEventTime: LocalDateTime = _

    override def onPush(): Unit = {
      val event = grab(in)
      if (firstIncomingEventTime == null) firstIncomingEventTime = event.date
      val eventPeriod = startOfPeriod(event.date, period, countdownStart.getOrElse(firstIncomingEventTime))
      val isNewPeriod = prevPeriod
        .map(!_.isEqual(eventPeriod))
        .getOrElse(false)
      if (isNewPeriod) {
        prevPeriod = Some(eventPeriod)
        push(out, eventsInPeriod)
        eventsInPeriod = immutable.Seq(event)
      } else {
        if (prevPeriod.isEmpty) prevPeriod = Some(eventPeriod)
        eventsInPeriod :+= event
        pull(in)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    override def onUpstreamFinish(): Unit = {
      if (isAvailable(out)) push(out, eventsInPeriod)
      eventsInPeriod = null
      completeStage()
    }
    
    setHandler(in, this)
    setHandler(out, this)
  }

  override def shape: FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]] = FlowShape(in, out)
}


trait DateUtils {

  val DefaultZone = ZoneOffset.UTC

  def startOfPeriod(date: LocalDateTime,
                    period: Duration,
                    countdownPoint: LocalDateTime,
                    zoneOffset: ZoneOffset = DefaultZone
                   ): LocalDateTime = {

    val startSecond = countdownPoint.toEpochSecond(zoneOffset)
    val periodSeconds = period.toSeconds
    val dateSecond = date.toEpochSecond(zoneOffset)

    val startPeriodSecond = if (date.isAfter(countdownPoint)) {
      val startDiffSeconds = ((dateSecond - startSecond) / periodSeconds) * periodSeconds
      startSecond + startDiffSeconds
    } else {
      val diff = startSecond - dateSecond
      val diffPeriods = if (diff % periodSeconds == 0) {
        diff / periodSeconds
      } else {
        diff / periodSeconds + 1
      }
      val startDiffSeconds = diffPeriods * periodSeconds
      startSecond - startDiffSeconds
    }
    LocalDateTime.ofEpochSecond(startPeriodSecond, 0, zoneOffset)
  }

}
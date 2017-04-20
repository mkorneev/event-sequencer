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

object CsvConverterApp extends App {


  implicit val system = ActorSystem("csv-converter")
  implicit val mat = ActorMaterializer()
  implicit val ec = system.dispatcher

  val FilePath = Paths.get("/Users/Kirill/Desktop/logins0.csv")
  val CsvSeparator = ','
  val StringSeparator = ByteString("\n")
  val DateFormatString = "yyyy-MM-dd HH:mm:ss"


  val TuplesSource = FileIO.fromPath(FilePath)
    .via(Framing.delimiter(StringSeparator, 1000, false))
    .map(_.utf8String
      .replace("\"", "")
      .split(CsvSeparator))
    .filter(_.length == 3)
    .map(a => UserAuthEvent(a(0), a(1), LocalDateTime.parse(a(2), DateTimeFormatter.ofPattern(DateFormatString))))
    .log("Event")
    .via(Flow.fromGraph(new EventPeriodFlow(10 minutes)))
    .mapConcat(s => s
      .groupBy(_.ip)
      .filter(_._2.size > 1)
    ).map(e => s"${e._1} ${e._2}")
    .runForeach(println).onComplete {
    _ => system.terminate()
  }

  case class UserAuthEvent(username: String, ip: String, date: LocalDateTime)

}


class EventPeriodFlow(period: FiniteDuration) extends GraphStage[FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]]] with DateUtils {

  val in = Inlet[UserAuthEvent]("EventPeriodFlow.in")
  val out = Outlet[immutable.Seq[UserAuthEvent]]("EventPeriodFlow.out")

  def createLogic(inheritedAttributes: Attributes): GraphStageLogic = new GraphStageLogic(shape) with InHandler with OutHandler {

    var prevPeriod: Option[LocalDateTime] = None
    var eventsInPeriod = immutable.Seq.empty[UserAuthEvent]

    override def onPush(): Unit = {
      val event = grab(in)
      val eventPeriod = startOfPeriod(event.date, period)
      val isNewPeriod = prevPeriod
        .map(!_.isEqual(eventPeriod))
        .getOrElse(false)
      if (isNewPeriod) {
        prevPeriod = Some(eventPeriod)
        push(out, eventsInPeriod)
        eventsInPeriod = immutable.Seq.empty[UserAuthEvent]
      } else {
        if (prevPeriod.isEmpty) prevPeriod = Some(eventPeriod)
        eventsInPeriod :+= event
        pull(in)
      }
    }

    override def onPull(): Unit = {
      pull(in)
    }

    setHandler(in, this)
    setHandler(out, this)
  }

  override def shape: FlowShape[UserAuthEvent, immutable.Seq[UserAuthEvent]] = FlowShape(in, out)
}


trait DateUtils {

  val DefaultZone = ZoneOffset.UTC
  val DefaultCountdownDate = LocalDateTime.ofEpochSecond(0, 0, DefaultZone)


  def startOfPeriod(date: LocalDateTime,
                    period: Duration,
                    zoneOffset: ZoneOffset = DefaultZone,
                    countdownPoint: LocalDateTime = DefaultCountdownDate
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
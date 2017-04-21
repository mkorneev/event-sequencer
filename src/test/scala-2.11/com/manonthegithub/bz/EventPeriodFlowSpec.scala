package com.manonthegithub.bz

import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import com.manonthegithub.bz.CsvConverterApp.UserAuthEvent
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.collection.immutable

class EventPeriodFlowSpec extends TestKit(ActorSystem(s"${UUID.randomUUID}")) with WordSpecLike with Matchers {

  implicit val mat = ActorMaterializer()

  "EventPeriodFlow" should {

    "aggregate events" in {

      val Events: immutable.Seq[UserAuthEvent] = (0 to 120).map(i => {
        UserAuthEvent("name", "ip", LocalDateTime.ofEpochSecond(i, 0, ZoneOffset.UTC))
      })

      val flow = Flow.fromGraph(new EventPeriodFlow(1 minute, Some(LocalDateTime.ofEpochSecond(0, 0, ZoneOffset.UTC))))

      val probe = Source(Events)
        .via(flow)
        .runWith(TestSink.probe)
        .request(4)

      val result = probe.expectNextN(3)

      for (i <- result.indices) {
        if (i != result.length - 1) {
          result(i) should have size 60
        } else {
          result(i) should have size 1
        }
      }

      probe.expectComplete()

    }


  }


}

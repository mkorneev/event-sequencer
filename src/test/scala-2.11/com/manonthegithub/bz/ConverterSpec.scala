package com.manonthegithub.bz

import java.time.LocalDateTime

import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ConverterSpec extends Matchers with WordSpecLike {

  "DateUtils" should {

    "count right start of period" in new DateUtils {

      val period = 2 minutes

      val dates = Seq(
        (0, 0),
        (1, 0),
        (599, 480),
        (600, 600),
        (1199, 1080),
        (1200, 1200)
      )

      dates.foreach(d => {
        val date = LocalDateTime.ofEpochSecond(d._1, 0, DefaultZone)
        startOfPeriod(date, period, countdownPoint = LocalDateTime.ofEpochSecond(600, 0, DefaultZone))
          .toEpochSecond(DefaultZone) should be(d._2)
      })

    }

  }

}

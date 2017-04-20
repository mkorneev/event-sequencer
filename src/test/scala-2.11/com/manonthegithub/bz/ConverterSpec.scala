/*
 * Copyright 2017 Infotecs. All rights reserved.
 */
package com.manonthegithub.bz

import java.time.LocalDateTime

import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.duration._

class ConverterSpec extends Matchers with WordSpecLike {

  "UserAuthAggregator" should {

    "count start of period" in {

      val period = 2 minutes

      val dates = Seq(
        LocalDateTime.ofEpochSecond(0, 0, UserAuthAggregator.DefaultZone),
        LocalDateTime.ofEpochSecond(1, 0, UserAuthAggregator.DefaultZone),
        LocalDateTime.ofEpochSecond(599, 0, UserAuthAggregator.DefaultZone),
        LocalDateTime.ofEpochSecond(600, 0, UserAuthAggregator.DefaultZone),
        LocalDateTime.ofEpochSecond(1199, 0, UserAuthAggregator.DefaultZone),
        LocalDateTime.ofEpochSecond(1200, 0, UserAuthAggregator.DefaultZone)
      )


      dates.foreach(d =>
        println((UserAuthAggregator.startOfPeriod(d, period, countdownPoint = LocalDateTime.ofEpochSecond(1200, 0, UserAuthAggregator.DefaultZone))))
      )

    }

  }

}

package com.mkorneev.event_sequencer

import java.time.temporal.ChronoUnit
import java.time.{Duration, LocalDateTime}

import org.scalatest.{FunSuite, Matchers}


class EventSequencerTest extends FunSuite with Matchers {

  type IP = String
  type User = String

  private val window = Duration.ofMinutes(1)
  private val time = LocalDateTime.now()

  test("testEmpty") {
    val ts = new EventSequencer[IP, User](window)

    ts.removeCompletedSequences(time) should have size 0
  }

  test("testSingle") {
    val ts = new EventSequencer[IP, User](window)
    ts.put(time, "IP", "User")

    ts.removeCompletedSequences(time.plus(10, ChronoUnit.MINUTES)) should have size 1
  }

  test("testSortedAssert") {
    val ts = new EventSequencer[IP, User](window)
    ts.put(time, "IP", "User")

    assertThrows[AssertionError] {
      ts.put(time.minus(1, ChronoUnit.SECONDS), "IP", "User")
    }
  }

  test("testWindow") {
    val ts = new EventSequencer[IP, User](window)
    ts.put(time, "IP", "User")
    ts.put(time, "IP", "User")
    ts.put(time, "IP", "User")
    ts.put(time.plus(10, ChronoUnit.MINUTES), "IP", "User")
    ts.put(time.plus(10, ChronoUnit.MINUTES), "IP", "User")

    val result = ts.removeCompletedSequences(time.plus(10, ChronoUnit.MINUTES))
    result should have size 1
    result.head._2.values should have size 3
  }

  test("testOpen") {
    val ts = new EventSequencer[IP, User](window)
    ts.put(time, "IP", "User")

    ts.removeCompletedSequences(time) should be(empty)
  }

}

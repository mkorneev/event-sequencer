package com.mkorneev.bz

import java.time.{Duration, LocalDateTime}

import scala.collection.mutable


case class EventsSeq[V](startTime: LocalDateTime, endTime: LocalDateTime, values: Seq[(LocalDateTime, V)])


class EventSequencer[K, V](window: Duration) {

  var openSequences = new mutable.HashMap[K, EventsSeq[V]]()
  var closedSequences = Seq.empty[(K, EventsSeq[V])]

  /**
    * Will throw as exception if time values are not sorted per key.
    */
  def add(time: LocalDateTime, key: K, value: V): Unit = {
    val seq = openSequences.get(key)

    if (!openSequences.contains(key)) {
      openSequences.update(key, EventsSeq(time, time, Seq((time, value))))
    } else {
      val seq = openSequences(key)

      assert(!time.isBefore(seq.endTime), "Time values should not decrease for each given key")

      if (seq.endTime.isBefore(time.minus(window))) {
        closedSequences :+= (key, seq)
        openSequences.update(key, EventsSeq(time, time, Seq((time, value))))
      } else {
        openSequences.update(key, EventsSeq(seq.startTime, time, seq.values :+ (time, value)))
      }
    }
  }

  def takeClosedSequences(now: LocalDateTime): Seq[(K, EventsSeq[V])] = {
    val (closed, open) = openSequences
      .partition(pair => pair._2.endTime.isBefore(now.minus(window)))

    val result = closedSequences ++ closed.toSeq

    openSequences = open
    closedSequences = Seq.empty

    result
  }

}

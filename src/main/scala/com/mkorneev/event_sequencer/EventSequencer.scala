package com.mkorneev.event_sequencer

import java.time.{Duration, LocalDateTime}

import scala.collection.mutable


case class EventsSeq[V](startTime: LocalDateTime, endTime: LocalDateTime, values: Seq[(LocalDateTime, V)])

/**
  * A custom data structure that accepts generic timed key-value pairs (events)
  * and builds completed sequences of events for each key where every event is no further away
  * from the previous one than the duration of the `timeWindow`.
  *
  * @param timeWindow Duration after which a sequence of events is considered completed.
  */
class EventSequencer[K, V](timeWindow: Duration) {

  var openSequences = new mutable.HashMap[K, EventsSeq[V]]()
  var completedSequences = Seq.empty[(K, EventsSeq[V])]

  /**
    * Adds a timed key-value pair (event) to the list of open sequences.
    *
    * Will throw as exception if time values are not sorted per key.
    */
  def put(time: LocalDateTime, key: K, value: V): Unit = {
    if (!openSequences.contains(key)) {
      openSequences.update(key, EventsSeq(time, time, Seq((time, value))))
    } else {
      val seq = openSequences(key)

      assert(!time.isBefore(seq.endTime), "Time values should not decrease for each given key")

      if (seq.endTime.isBefore(time.minus(timeWindow))) {
        completedSequences :+= (key, seq)
        openSequences.update(key, EventsSeq(time, time, Seq((time, value))))
      } else {
        openSequences.update(key, EventsSeq(seq.startTime, time, seq.values :+ (time, value)))
      }
    }
  }

  /**
    * Returns a list of completed event sequences with regards to the current time.
    *
    * @param now Current time. Any event sequence that has ended before `now` - `timeWindow` is considered completed.
    */
  def removeCompletedSequences(now: LocalDateTime): Seq[(K, EventsSeq[V])] = {
    val (completedByNow, stillOpen) = openSequences
      .partition { case (_, seq) => seq.endTime.isBefore(now.minus(timeWindow))}

    val result = completedSequences ++ completedByNow.toSeq

    openSequences = stillOpen
    completedSequences = Seq.empty

    result
  }

}
